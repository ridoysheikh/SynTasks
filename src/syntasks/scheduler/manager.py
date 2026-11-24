"""
Schedules of an app (``app.schedules``).

* **Static** schedules are declared in code (``@app.task(schedule=...)``) or
  in config (``[schedules.<id>]``). Every worker syncs them into Redis on
  start: new ones are added, changed ones updated (runtime state such as
  ``paused`` or ``runs`` is kept), removed ones deleted. ``[app] revision``
  stops an old deploy from undoing a newer one.
* **Dynamic** schedules are created at runtime with full CRUD, and can end
  by ``ttl`` / ``expires_at`` / ``end_at`` / ``max_runs``.

Dispatching: one worker holds the dispatcher lease (per shard). It claims
due schedules atomically, computes the next fire times, and commits the
task messages and the new due time in one script, so every fire is
enqueued exactly once even when dispatchers crash or race. From there a
fire is a normal task: one free worker runs it.
"""

from __future__ import annotations

import builtins
import inspect
import json
import logging
import uuid
import zlib
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Annotated, Any, get_args, get_origin, get_type_hints

from syntasks.codec import ConversionError, convert
from syntasks.dependencies.dependency import Dependency
from syntasks.exceptions import SynTasksError
from syntasks.limits import dumps_bounded
from syntasks.message import SynTasksMessage
from syntasks.scheduler import scripts
from syntasks.scheduler.recurrence import (
    At,
    Cron,
    Every,
    Recurrence,
    ScheduleDecl,
    parse_time_ms,
)
from syntasks.scheduler.schedule import Schedule, ScheduleSpec
from syntasks.units import parse_duration_ms

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks
    from syntasks.cluster.janitor import Janitor
    from syntasks.cluster.lease import Lease
    from syntasks.result import SynTasksResult
    from syntasks.task import AsyncSynTasksTask

__all__ = [
    "ScheduleConflictError",
    "ScheduleError",
    "ScheduleManager",
    "ScheduleNotFoundError",
    "StaticScheduleError",
]

logger = logging.getLogger("syntasks.scheduler")

_FIRE_NAMESPACE = uuid.UUID("5f1c3d7e-8a42-4b8e-9d3c-2a6f0e9b1c44")
_POLICY_NAMES = {
    "start_at",
    "end_at",
    "jitter",
    "misfire",
    "misfire_grace",
    "max_catchup",
    "overlap",
    "max_instances",
    "timeout",
    "retries",
    "queue",
    "priority",
    "labels",
    "max_runs",
    "pause_after_failures",
    "paused",
}


def _is_dependency(parameter: inspect.Parameter, hint: Any) -> bool:
    if isinstance(parameter.default, Dependency):
        return True
    if get_origin(hint) is Annotated:
        return any(isinstance(meta, Dependency) for meta in get_args(hint)[1:])
    return False


def call_signature(func: Any) -> inspect.Signature:
    """
    The parameters a caller passes: the signature without dependencies.

    :param func: task function.
    :return: signature without injected parameters.
    """
    signature = inspect.signature(func)
    try:
        hints = get_type_hints(func, include_extras=True)
    except (NameError, TypeError):
        hints = {}
    return signature.replace(
        parameters=[
            p
            for p in signature.parameters.values()
            if not _is_dependency(p, hints.get(p.name))
        ],
    )


class ScheduleError(SynTasksError, ValueError):
    """A schedule can't be created or changed."""

    __template__ = "{detail}"
    detail: str


class ScheduleNotFoundError(ScheduleError, KeyError):
    """No schedule with this id."""


class ScheduleConflictError(ScheduleError):
    """The schedule changed since it was read (optimistic locking)."""


class StaticScheduleError(ScheduleError):
    """Declared (code) schedules can't be updated or deleted through the API."""


# Missed fire times looked at once; more are skipped without counting.
_MAX_MISSED = 100_000


def _fire_task_id(schedule_id: str, fire_ms: int) -> str:
    return uuid.uuid5(_FIRE_NAMESPACE, f"{schedule_id}:{fire_ms}").hex


class ScheduleManager:
    """Static declarations, CRUD and dispatching of schedules."""

    def __init__(self, app: SynTasks) -> None:
        self.app = app
        self._declared: dict[str, list[ScheduleDecl]] = {}

    # -- keys --------------------------------------------------------------

    def _key(self, *parts: str) -> str:
        return self.app.keys("sched", *parts)

    def def_key(self, schedule_id: str) -> str:
        """Hash of a schedule."""
        return self._key("def", schedule_id)

    def shard(self, schedule_id: str) -> int:
        """Dispatcher shard of a schedule."""
        shards = max(self.app.config.scheduler.shards, 1)
        return zlib.crc32(schedule_id.encode()) % shards

    def due_key(self, shard: int) -> str:
        """Due set of a shard."""
        return self._key("due", str(shard))

    def _index_keys(self, schedule_id: str, task: str) -> builtins.list[str]:
        return [
            self.def_key(schedule_id),
            self.due_key(self.shard(schedule_id)),
            self._key("expiry"),
            self._key("ids"),
            self._key("dynamic"),
            self._key("by-task", task),
        ]

    # -- building specs ------------------------------------------------------

    def _spec(
        self,
        schedule_id: str,
        task: str,
        recurrences: builtins.list[Recurrence],
        args: builtins.list[Any],
        kwargs: dict[str, Any],
        options: dict[str, Any],
    ) -> tuple[ScheduleSpec, dict[str, Any]]:
        unknown = set(options) - _POLICY_NAMES
        if unknown:
            raise ScheduleError(
                detail=f"Unknown schedule option(s): {', '.join(sorted(unknown))}.",
            )

        def time_or_none(name: str) -> int | None:
            value = options.get(name)
            return None if value is None else parse_time_ms(value)

        def ms_or_none(name: str) -> int | None:
            value = options.get(name)
            return None if value is None else parse_duration_ms(value)

        timeout = options.get("timeout")
        try:
            spec = ScheduleSpec(
                id=schedule_id,
                task=task,
                recurrences=recurrences,
                args=list(args),
                kwargs=dict(kwargs),
                start_at_ms=time_or_none("start_at"),
                end_at_ms=time_or_none("end_at"),
                jitter_ms=ms_or_none("jitter") or 0,
                misfire=options.get("misfire") or "once",
                misfire_grace_ms=ms_or_none("misfire_grace"),
                max_catchup=options.get("max_catchup"),
                overlap=options.get("overlap") or "skip",
                max_instances=options.get("max_instances") or 1,
                timeout=None if timeout is None else parse_duration_ms(timeout) / 1000,
                retries=options.get("retries"),
                queue=options.get("queue"),
                priority=options.get("priority"),
                labels=dict(options.get("labels") or {}),
                max_runs=options.get("max_runs"),
                pause_after_failures=options.get("pause_after_failures"),
            )
        except ValueError as exc:
            raise ScheduleError(detail=f"Schedule {schedule_id!r}: {exc}") from exc
        extra = {"paused": bool(options.get("paused", False))}
        return spec, extra

    def _check_payload(self, spec: ScheduleSpec) -> None:
        try:
            dumps_bounded(
                {"args": spec.args, "kwargs": spec.kwargs, "labels": spec.labels},
                max_bytes=self.app.config.scheduler.max_payload,
                what=f"Arguments of schedule {spec.id!r}",
            )
        except ValueError as exc:
            raise ScheduleError(detail=str(exc)) from exc

    def check_arguments(self, spec: ScheduleSpec) -> None:
        """
        Check a schedule's arguments against its task's signature.

        Skipped when the task isn't registered in this process.

        :param spec: schedule spec.
        :raises ScheduleError: if the arguments don't fit.
        """
        task = self.app.find_task(spec.task)
        if task is None:
            return
        func = task.original_func
        try:
            signature = call_signature(func)
            hints = get_type_hints(func)
            bound = signature.bind(*spec.args, **spec.kwargs)
        except TypeError as exc:
            raise ScheduleError(
                detail=f"Schedule {spec.id!r}: arguments don't match "
                f"{spec.task}: {exc}",
            ) from exc
        for name, value in bound.arguments.items():
            if name in hints:
                try:
                    convert(value, hints[name])
                except (ConversionError, ValueError, TypeError) as exc:
                    raise ScheduleError(
                        detail=f"Schedule {spec.id!r}: argument {name!r}: {exc}",
                    ) from exc

    def _check_interval(self, spec: ScheduleSpec) -> None:
        minimum = int(self.app.config.scheduler.min_interval * 1000)
        for recurrence in spec.recurrences:
            gap = recurrence.min_interval_ms()
            if gap is not None and gap < minimum:
                raise ScheduleError(
                    detail=f"Schedule {spec.id!r} fires every {gap}ms; the minimum is "
                    f"{minimum}ms (scheduler.min_interval).",
                )

    # -- static declarations ----------------------------------------------------

    def declare(self, task_name: str, schedule: Any) -> None:
        """
        Attach schedules to a task (used by ``@app.schedule(...)``).

        :param task_name: task name.
        :param schedule: declaration(s) from :func:`every`, :func:`cron`,
            :func:`at`, a :class:`Recurrence`, or a list of them.
        :raises ScheduleError: on invalid declarations.
        """
        items = schedule if isinstance(schedule, (list, tuple)) else [schedule]
        decls = self._declared.setdefault(task_name, [])
        for item in items:
            if isinstance(item, Recurrence):
                item = ScheduleDecl(recurrence=item)  # noqa: PLW2901
            if not isinstance(item, ScheduleDecl):
                raise ScheduleError(
                    detail=f"Task {task_name}: schedule must come from every(), cron() "
                    f"or at(), got {item!r}.",
                )
            if item.after_ms is not None:
                raise ScheduleError(
                    detail=f"Task {task_name}: after() is only for dynamic schedules.",
                )
            decls.append(item)

    def static_specs(self) -> dict[str, tuple[ScheduleSpec, str, dict[str, Any]]]:
        """
        All schedules declared in code.

        A schedule's id is the one given, else the task name, or
        ``<task>:<n>`` when the task has several timings.

        :raises ScheduleError: on invalid or duplicate declarations.
        :return: ``id → (spec, source, extra)``.
        """
        result: dict[str, tuple[ScheduleSpec, str, dict[str, Any]]] = {}
        for task_name, decls in self._declared.items():
            for index, decl in enumerate(decls):
                default_id = task_name if len(decls) == 1 else f"{task_name}:{index}"
                schedule_id = decl.id or default_id
                if schedule_id in result:
                    raise ScheduleError(
                        detail=f"Duplicate schedule id {schedule_id!r}.",
                    )
                assert decl.recurrence is not None  # noqa: S101
                spec, extra = self._spec(
                    schedule_id,
                    task_name,
                    [decl.recurrence],
                    decl.args,
                    decl.kwargs,
                    decl.options,
                )
                result[schedule_id] = (spec, "code", extra)
        for spec, _, _ in result.values():
            self._check_payload(spec)
            self.check_arguments(spec)
        return result

    def validate_declared(self) -> None:
        """Validate the declared schedules (used by ``syntasks config check``)."""
        self.static_specs()

    async def sync_static(self) -> str:
        """
        Make Redis match the declared schedules (run on worker start).

        :return: ``same``, ``synced``, ``stale`` or ``busy``.
        """
        specs = self.static_specs()
        config = self.app.config
        fingerprints = {sid: spec.fingerprint() for sid, (spec, _, _) in specs.items()}
        set_fp = json.dumps(sorted(fingerprints.items()))
        revision = "" if config.app.revision is None else str(config.app.revision)
        node = self.app.cluster.node_id
        meta = self._key("static", "meta")
        lock = self._key("static", "lock")
        status, stored = await scripts.SYNC_BEGIN(
            self.app.redis,
            [meta, lock],
            [revision, set_fp, node],
        )
        status = status.decode()
        if status == "stale":
            logger.warning(
                "Schedules were synced by a newer revision (%s); this node "
                "(revision %s) leaves them as they are.",
                stored.decode(),
                revision or "none",
            )
            return status
        if status != "go":
            return status
        try:
            stored_fps = {
                k.decode(): v.decode()
                for k, v in (await self.app.redis.hgetall(self._key("static"))).items()
            }
            now = self.app.cluster.clock.now_ms()
            for schedule_id, (spec, source, extra) in specs.items():
                fp = fingerprints[schedule_id]
                if stored_fps.get(schedule_id) == fp and await self.app.redis.exists(
                    self.def_key(schedule_id),
                ):
                    continue
                existing = await self._load(schedule_id)
                keep = (
                    existing is not None
                    and existing.state in ("active", "paused")
                    and existing.spec.timing_fingerprint() == spec.timing_fingerprint()
                )
                nominal = spec.next_after(now)
                await self._write(
                    spec,
                    source=source,
                    nominal=nominal,
                    if_exists="replace",
                    state="paused" if extra.get("paused") else "active",
                    keep_runtime=keep,
                    expires_at=None,
                    allow_static=True,
                )
                await self.app.redis.hset(self._key("static"), {schedule_id: fp})
            removed = set(stored_fps) - set(specs)
            for schedule_id in removed:
                if config.scheduler.prune_removed:
                    await self._delete(schedule_id, allow_static=True)
                    logger.info(
                        "Removed schedule %s (no longer declared).",
                        schedule_id,
                    )
                else:
                    await self.app.redis.hset(
                        self.def_key(schedule_id),
                        {"state": "orphaned"},
                    )
                    await self.app.redis.zrem(
                        self.due_key(self.shard(schedule_id)),
                        schedule_id,
                    )
                    await self.app.redis.hdel(self._key("static"), schedule_id)
            await scripts.SYNC_END(
                self.app.redis,
                [meta, lock],
                [revision, set_fp, node],
            )
        except BaseException:
            await self.app.redis.delete(lock)
            raise
        await self.app.cluster.control.publish("schedule.changed")
        logger.info("Synced %d declared schedule(s).", len(specs))
        return "synced"

    # -- storage helpers -----------------------------------------------------------

    async def _write(
        self,
        spec: ScheduleSpec,
        *,
        source: str,
        nominal: int | None,
        if_exists: str,
        state: str,
        keep_runtime: bool = False,
        expires_at: int | None,
        expected_version: int | None = None,
        allow_static: bool = False,
    ) -> int:
        due = None if nominal is None else spec.due_for(nominal)
        reply = await scripts.UPSERT(
            self.app.redis,
            self._index_keys(spec.id, spec.task),
            [
                spec.id,
                spec.to_json(),
                source,
                spec.task,
                "" if due is None else due,
                "" if nominal is None else nominal,
                "" if expires_at is None else expires_at,
                if_exists,
                self.app.config.scheduler.max_dynamic if source == "api" else "",
                state,
                "" if expected_version is None else expected_version,
                "1" if keep_runtime else "",
                self._key("by-task", ""),
                "" if spec.max_runs is None else spec.max_runs,
            ],
        )
        status = reply[0].decode()
        if status == "exists":
            raise ScheduleError(detail=f"Schedule {spec.id!r} already exists.")
        if status == "static" and not allow_static:
            raise StaticScheduleError(
                detail=f"Schedule {spec.id!r} is declared in code or config; "
                "change it there.",
            )
        if status == "quota":
            raise ScheduleError(
                detail=f"Too many dynamic schedules (scheduler.max_dynamic = "
                f"{self.app.config.scheduler.max_dynamic}).",
            )
        if status == "conflict":
            raise ScheduleConflictError(
                detail=f"Schedule {spec.id!r} changed (version {reply[1].decode()}); "
                "read it again and retry.",
            )
        if status == "missing":
            raise ScheduleNotFoundError(detail=f"No schedule {spec.id!r}.")
        if status == "ignored":
            return -1
        return int(reply[1])

    async def _load(self, schedule_id: str) -> Schedule | None:
        data = await self.app.redis.hgetall(self.def_key(schedule_id))
        if not data or b"spec" not in data:
            return None
        return Schedule.from_hash(data)

    async def _delete(self, schedule_id: str, *, allow_static: bool = False) -> int:
        return int(
            await scripts.DELETE(
                self.app.redis,
                [
                    self.def_key(schedule_id),
                    self.due_key(self.shard(schedule_id)),
                    self._key("expiry"),
                    self._key("ids"),
                    self._key("dynamic"),
                    self._key("runs", schedule_id),
                    self._key("static"),
                ],
                [schedule_id, self._key("by-task", ""), "1" if allow_static else ""],
            ),
        )

    # -- CRUD --------------------------------------------------------------------

    async def create(
        self,
        task: Any,
        *,
        args: Any = (),
        kwargs: dict[str, Any] | None = None,
        every: Any = None,
        cron: str | None = None,
        tz: str | None = None,
        at: Any = None,
        after: Any = None,
        id: str | None = None,
        ttl: Any = None,
        expires_at: Any = None,
        if_exists: str = "error",
        **options: Any,
    ) -> Schedule:
        """
        Create a dynamic schedule.

        :param task: task object or task name.
        :param args: task arguments (JSON).
        :param kwargs: task keyword arguments (JSON).
        :param every: interval (``"1s"``, ``"250ms"``, seconds, timedelta).
        :param cron: cron expression (5 or 6 fields).
        :param tz: time zone for ``cron``.
        :param at: one-shot time.
        :param after: one-shot delay from now.
        :param id: schedule id (default: random).
        :param ttl: delete the schedule this long after creation.
        :param expires_at: delete the schedule at this time.
        :param if_exists: ``error``, ``replace`` or ``ignore``.
        :param options: policies: ``start_at``, ``end_at``, ``jitter``,
            ``misfire``, ``misfire_grace``, ``max_catchup``, ``overlap``,
            ``max_instances``, ``timeout``, ``retries``, ``queue``,
            ``priority``, ``labels``, ``max_runs``, ``pause_after_failures``,
            ``paused``.
        :raises ScheduleError: if the schedule is invalid.
        :return: the stored schedule.
        """
        task_name = task if isinstance(task, str) else task.task_name
        now = self.app.cluster.clock.now_ms()
        recurrences: builtins.list[Recurrence] = []
        try:
            if every is not None:
                recurrences.append(Every(parse_duration_ms(every), anchor_ms=now))
            if cron is not None:
                recurrences.append(Cron(cron, tz))
            if at is not None:
                recurrences.append(At(parse_time_ms(at)))
            if after is not None:
                recurrences.append(At(now + parse_duration_ms(after)))
            expiry: int | None = None
            if ttl is not None:
                expiry = now + parse_duration_ms(ttl)
            if expires_at is not None:
                parsed = parse_time_ms(expires_at)
                expiry = parsed if expiry is None else min(expiry, parsed)
        except ValueError as exc:
            raise ScheduleError(detail=str(exc)) from exc
        if not recurrences:
            raise ScheduleError(detail="Give every, cron, at or after.")
        if if_exists not in ("error", "replace", "ignore"):
            raise ScheduleError(detail="if_exists must be error, replace or ignore.")
        schedule_id = id or uuid.uuid4().hex
        spec, extra = self._spec(
            schedule_id,
            task_name,
            recurrences,
            list(args),
            dict(kwargs or {}),
            options,
        )
        self._check_payload(spec)
        self._check_interval(spec)
        self.check_arguments(spec)
        await self._write(
            spec,
            source="api",
            nominal=spec.next_after(now),
            if_exists=if_exists,
            state="paused" if extra.get("paused") else "active",
            expires_at=expiry,
        )
        await self.app.cluster.control.publish("schedule.changed")
        return await self.get(schedule_id)

    async def get(self, schedule_id: str) -> Schedule:
        """
        Read a schedule.

        :param schedule_id: id.
        :raises ScheduleNotFoundError: if it doesn't exist.
        :return: schedule.
        """
        schedule = await self._load(schedule_id)
        if schedule is None:
            raise ScheduleNotFoundError(detail=f"No schedule {schedule_id!r}.")
        return schedule

    async def list(
        self,
        *,
        task: str | None = None,
        source: str | None = None,
        state: str | None = None,
    ) -> AsyncIterator[Schedule]:
        """
        Iterate over schedules, oldest first.

        :param task: only schedules of this task.
        :param source: ``code``, ``config`` or ``api``.
        :param state: ``active``, ``paused``, ``done``, ``invalid``, ...
        :yield: schedules.
        """
        if task is not None:
            ids = sorted(
                raw.decode()
                for raw in await self.app.redis.smembers(self._key("by-task", task))
            )
        else:
            ids = [
                raw.decode()
                for raw in await self.app.redis.execute(
                    "ZRANGE",
                    self._key("ids"),
                    0,
                    -1,
                )
            ]
        for start in range(0, len(ids), 100):
            chunk = ids[start : start + 100]
            pipe = self.app.redis.pipeline()
            for schedule_id in chunk:
                pipe.add("HGETALL", self.def_key(schedule_id))
            for reply in await pipe.execute():
                if not reply:
                    continue
                schedule = Schedule.from_hash(
                    dict(zip(reply[::2], reply[1::2], strict=True)),
                )
                if source is not None and schedule.source != source:
                    continue
                if state is not None and schedule.state != state:
                    continue
                yield schedule

    async def update(  # noqa: C901
        self,
        schedule_id: str,
        *,
        version: int | None = None,
        **changes: Any,
    ) -> Schedule:
        """
        Change a dynamic schedule.

        Accepts the same arguments as :meth:`create` (except ``id`` and
        ``if_exists``). Changing the timing recomputes the next fire.

        :param schedule_id: id.
        :param version: expected version (optimistic lock); None to skip.
        :param changes: new values.
        :raises StaticScheduleError: for schedules declared in code.
        :raises ScheduleConflictError: if ``version`` is stale.
        :return: updated schedule.
        """
        current = await self.get(schedule_id)
        if current.source != "api":
            raise StaticScheduleError(
                detail=f"Schedule {schedule_id!r} is declared in code or config; "
                "change it there.",
            )
        spec = current.spec
        now = self.app.cluster.clock.now_ms()
        data = spec.to_dict()
        timing = {"every", "cron", "tz", "at", "after"}
        if timing & set(changes):
            recurrences: builtins.list[Recurrence] = []
            if changes.get("every") is not None:
                recurrences.append(
                    Every(parse_duration_ms(changes["every"]), anchor_ms=now),
                )
            cron_expr = changes.get("cron")
            if cron_expr is not None:
                recurrences.append(Cron(cron_expr, changes.get("tz")))
            if changes.get("at") is not None:
                recurrences.append(At(parse_time_ms(changes["at"])))
            if changes.get("after") is not None:
                recurrences.append(At(now + parse_duration_ms(changes["after"])))
            if not recurrences:
                raise ScheduleError(detail="Give every, cron, at or after.")
            data["recurrences"] = [r.to_dict() for r in recurrences]
        for name in ("args", "kwargs"):
            if name in changes:
                data[name] = (
                    list(changes[name]) if name == "args" else dict(changes[name])
                )
        task_name = changes.get("task", spec.task)
        task_name = task_name if isinstance(task_name, str) else task_name.task_name
        options = {
            "start_at": changes.get(
                "start_at",
                spec.start_at_ms and spec.start_at_ms / 1000,
            ),
            "end_at": changes.get("end_at", spec.end_at_ms and spec.end_at_ms / 1000),
            "jitter": changes.get("jitter", spec.jitter_ms / 1000),
            "misfire": changes.get("misfire", spec.misfire),
            "misfire_grace": changes.get(
                "misfire_grace",
                None if spec.misfire_grace_ms is None else spec.misfire_grace_ms / 1000,
            ),
            "max_catchup": changes.get("max_catchup", spec.max_catchup),
            "overlap": changes.get("overlap", spec.overlap),
            "max_instances": changes.get("max_instances", spec.max_instances),
            "timeout": changes.get("timeout", spec.timeout),
            "retries": changes.get("retries", spec.retries),
            "queue": changes.get("queue", spec.queue),
            "priority": changes.get("priority", spec.priority),
            "labels": changes.get("labels", spec.labels),
            "max_runs": changes.get("max_runs", spec.max_runs),
            "pause_after_failures": changes.get(
                "pause_after_failures",
                spec.pause_after_failures,
            ),
        }
        options = {k: v for k, v in options.items() if v is not None}
        from syntasks.scheduler.recurrence import recurrence_from_dict  # noqa: PLC0415

        new_spec, _ = self._spec(
            schedule_id,
            task_name,
            [recurrence_from_dict(r) for r in data["recurrences"]],
            data["args"],
            data["kwargs"],
            options,
        )
        self._check_payload(new_spec)
        self._check_interval(new_spec)
        self.check_arguments(new_spec)
        expiry = current.expires_ms
        if "ttl" in changes:
            expiry = (
                None
                if changes["ttl"] is None
                else now + parse_duration_ms(changes["ttl"])
            )
        if "expires_at" in changes:
            expiry = (
                None
                if changes["expires_at"] is None
                else parse_time_ms(changes["expires_at"])
            )
        timing_changed = new_spec.timing_fingerprint() != spec.timing_fingerprint()
        await self._write(
            new_spec,
            source="api",
            nominal=new_spec.next_after(now),
            if_exists="replace",
            state=current.state,
            keep_runtime=not timing_changed,
            expires_at=expiry,
            expected_version=version,
        )
        await self.app.cluster.control.publish("schedule.changed")
        return await self.get(schedule_id)

    async def delete(self, schedule_id: str) -> None:
        """
        Delete a dynamic schedule.

        :param schedule_id: id.
        :raises ScheduleNotFoundError: if it doesn't exist.
        :raises StaticScheduleError: for schedules declared in code.
        """
        status = await self._delete(schedule_id)
        if status == 0:
            raise ScheduleNotFoundError(detail=f"No schedule {schedule_id!r}.")
        if status == -1:
            raise StaticScheduleError(
                detail=f"Schedule {schedule_id!r} is declared in code or config; "
                "remove it there (or pause it).",
            )

    async def _set_state(self, schedule_id: str, action: str) -> Schedule:
        nominal: int | None = None
        if action == "resume":
            schedule = await self.get(schedule_id)
            nominal = schedule.spec.next_after(self.app.cluster.clock.now_ms())
        due = None
        if nominal is not None:
            due = (await self.get(schedule_id)).spec.due_for(nominal)
        reply = await scripts.SET_STATE(
            self.app.redis,
            [self.def_key(schedule_id), self.due_key(self.shard(schedule_id))],
            [
                schedule_id,
                action,
                "" if due is None else due,
                "" if nominal is None else nominal,
            ],
        )
        if reply[0] == b"missing":
            raise ScheduleNotFoundError(detail=f"No schedule {schedule_id!r}.")
        await self.app.cluster.control.publish("schedule.changed")
        return await self.get(schedule_id)

    async def pause(self, schedule_id: str) -> Schedule:
        """
        Stop a schedule from firing (kept across deploys).

        :param schedule_id: id.
        :return: schedule.
        """
        return await self._set_state(schedule_id, "pause")

    async def resume(self, schedule_id: str) -> Schedule:
        """
        Let a paused schedule fire again, from its next future time.

        :param schedule_id: id.
        :return: schedule.
        """
        return await self._set_state(schedule_id, "resume")

    async def trigger(self, schedule_id: str) -> AsyncSynTasksTask[Any]:
        """
        Run a schedule's task now, without changing its timing.

        :param schedule_id: id.
        :return: handle of the started task.
        """
        schedule = await self.get(schedule_id)
        now = self.app.cluster.clock.now_ms()
        message = self._message(schedule.spec, now, schedule.runs + 1, manual=True)
        await self.app.kick(self.app.formatter.dumps(message))
        return self.app.tasks.handle(message.task_id)

    async def runs(
        self,
        schedule_id: str,
        limit: int = 20,
    ) -> builtins.list[dict[str, Any]]:
        """
        Recent runs of a schedule, newest first.

        :param schedule_id: id.
        :param limit: max entries.
        :return: run records: ``fire``, ``task_id``, ``status``,
            ``duration``, ``error``.
        """
        raw = await self.app.redis.lrange(self._key("runs", schedule_id), 0, limit - 1)
        return [json.loads(item) for item in raw]

    async def cancel_running(self, schedule_id: str) -> int:
        """
        Cancel the runs of a schedule that are executing now.

        :param schedule_id: id.
        :return: number of cancel requests sent.
        """
        sent = 0
        schedule = await self.get(schedule_id)
        for item in await self.app.tasks.running():
            if item.task_name == schedule.task:
                await self.app.tasks.cancel(item.task_id)
                sent += 1
        return sent

    # -- dispatching -----------------------------------------------------------------

    def _message(
        self,
        spec: ScheduleSpec,
        fire_ms: int,
        run_number: int,
        *,
        manual: bool = False,
        sent_ms: int | None = None,
    ) -> SynTasksMessage:
        labels: dict[str, Any] = {
            **spec.labels,
            "_schedule": spec.id,
            "_fire": fire_ms,
            "_run": run_number,
        }
        if sent_ms is not None:
            labels["_sent"] = sent_ms
        if spec.overlap != "allow" and not manual:
            labels["_overlap"] = spec.overlap
            labels["_max_instances"] = spec.max_instances
        if spec.queue:
            labels["queue"] = spec.queue
        if spec.priority is not None:
            labels["priority"] = spec.priority
        if spec.timeout is not None:
            labels["_timeout"] = spec.timeout
        if spec.retries is not None:
            labels["_schedule_retries"] = spec.retries
        task_id = uuid.uuid4().hex if manual else _fire_task_id(spec.id, fire_ms)
        return SynTasksMessage(
            task_id=task_id,
            task_name=spec.task,
            labels=labels,
            labels_types=None,
            args=list(spec.args),
            kwargs=dict(spec.kwargs),
        )

    def _plan(
        self,
        spec: ScheduleSpec,
        fire: int,
        now: int,
    ) -> tuple[builtins.list[int], int, int | None]:
        """
        Fires to send now, fires skipped, and the next nominal time.

        A fire is *missed* when a newer fire time has passed too (the cluster
        was down or behind), or when it is dispatched later than
        ``scheduler.late_after``. Otherwise it is on time and simply runs.
        For missed fires the schedule's ``misfire`` policy decides which of
        the passed fire times run, after dropping those older than its
        ``misfire_grace``.
        """
        config = self.app.config.scheduler
        missed = [fire]
        nxt = spec.next_after(fire)
        while nxt is not None and nxt <= now and len(missed) < _MAX_MISSED:
            missed.append(nxt)
            nxt = spec.next_after(nxt)
        if len(missed) == 1 and now - fire <= int(config.late_after * 1000):
            return [fire], 0, nxt
        while nxt is not None and nxt <= now:  # beyond the cap: count only
            nxt = spec.next_after(nxt)
        grace = spec.misfire_grace_ms
        usable = [f for f in missed if grace is None or now - f <= grace]
        if spec.misfire == "skip" or not usable:
            fires: builtins.list[int] = []
        elif spec.misfire == "once":
            fires = usable[:1]
        else:
            fires = usable[: spec.max_catchup or config.max_catchup]
        return fires, len(missed) - len(fires), nxt

    async def dispatch(
        self,
        lease: Lease | None = None,
        shard: int = 0,
    ) -> int | None:
        """
        Fire every due schedule of a shard (run by the dispatcher).

        :param lease: the dispatcher lease (fencing).
        :param shard: shard number.
        :return: next due time of the shard (Unix ms), or None if empty.
        """
        config = self.app.config.scheduler
        redis = self.app.redis
        token = uuid.uuid4().hex
        while True:
            if lease is not None and not lease.held:
                return None
            reply = await scripts.CLAIM(
                redis,
                [self.due_key(shard)],
                [config.batch, int(config.claim_lease * 1000), self.def_key(""), token],
            )
            for index in range(0, len(reply), 4):
                schedule_id = reply[index].decode()
                fire = int(float(reply[index + 1]))
                runs = int(reply[index + 3] or 0)
                try:
                    spec = ScheduleSpec.from_dict(json.loads(reply[index + 2]))
                    await self._commit(spec, fire, runs, token)
                except Exception as exc:
                    logger.exception(
                        "Schedule %s can't fire; marking it invalid.",
                        schedule_id,
                    )
                    await scripts.INVALIDATE(
                        redis,
                        [self.def_key(schedule_id), self.due_key(shard)],
                        [schedule_id, str(exc)[:1000]],
                    )
            if len(reply) < config.batch * 4:
                break
        head = await redis.zrange_withscores(self.due_key(shard), 0, 0)
        return int(head[0][1]) if head else None

    async def _commit(
        self,
        spec: ScheduleSpec,
        fire: int,
        runs: int,
        token: str,
    ) -> None:
        engine = self.app.engine
        now = self.app.cluster.clock.now_ms()
        fires, skipped, nominal = self._plan(spec, fire, now)
        if spec.max_runs is not None:
            fires = fires[: max(spec.max_runs - runs, 0)]
            if runs + len(fires) >= spec.max_runs:
                nominal = None
        messages = [
            self._message(spec, f, runs + i + 1, sent_ms=now)
            for i, f in enumerate(fires)
        ]
        probe_labels = (
            messages[0].labels
            if messages
            else {"queue": spec.queue}
            if spec.queue
            else {}
        )
        queue, stream, options = engine.route(spec.task, probe_labels)
        triples: builtins.list[Any] = []
        for message in messages:
            data = self.app.formatter.dumps(message).message
            if options.max_payload is not None and len(data) > options.max_payload:
                raise ScheduleError(
                    detail=f"Message of schedule {spec.id!r} is {len(data)} bytes; the "
                    f"task's max_payload is {options.max_payload}.",
                )
            triples += [data, message.labels["_fire"], message.task_id]
        due = None if nominal is None else spec.due_for(nominal)
        reply = await scripts.COMMIT(
            self.app.redis,
            [
                self.def_key(spec.id),
                self.due_key(self.shard(spec.id)),
                self._key("runs", spec.id),
                stream,
                engine.streams_key(queue),
                engine.index_key,
                engine.task_wake_key(spec.task),
                self.app.keys("lim", "sem", f"sched:{spec.id}"),
            ],
            [
                token,
                "" if due is None else due,
                "" if nominal is None else nominal,
                engine.group,
                spec.task,
                queue,
                spec.overlap,
                spec.max_instances,
                self.app.config.scheduler.history,
                "1" if engine.dedicated(options) else "",
                fires[-1] if fires else fire,
                skipped,
                spec.id,
                *triples,
            ],
        )
        if reply[0] == b"lost":
            logger.warning(
                "Lost the claim on schedule %s; another dispatcher has it.",
                spec.id,
            )
            return
        fired, missed = int(reply[1]), int(reply[2])
        if missed:
            logger.info(
                "Schedule %s: %d fire(s) skipped (missed or overlapping).",
                spec.id,
                missed,
            )
        logger.debug("Schedule %s fired %d task(s).", spec.id, fired)

    # -- runs and cleanup -------------------------------------------------------------

    async def record_run(
        self,
        message: SynTasksMessage,
        result: SynTasksResult[Any],
    ) -> None:
        """
        Store the end of a scheduled run (called by workers).

        :param message: the run's message.
        :param result: its result.
        """
        schedule_id = str(message.labels.get("_schedule"))
        schedule = await self._load(schedule_id)
        if schedule is None:
            return
        status = "error" if result.is_err else "ok"
        error = "" if result.error is None else repr(result.error)[:1000]
        fire = message.labels.get("_fire")
        sent = message.labels.get("_sent")
        late_by = (
            max(int(str(sent)) - int(str(fire)), 0) / 1000
            if fire not in (None, "") and sent not in (None, "")
            else 0.0
        )
        entry = json.dumps(
            {
                "fire": fire,
                "late_by": late_by,
                "task_id": message.task_id,
                "status": status,
                "duration": result.execution_time,
                "error": error or None,
                "attempt": message.labels.get("_attempt", 0),
            },
        )
        paused = await scripts.RECORD(
            self.app.redis,
            [
                self.def_key(schedule_id),
                self.due_key(self.shard(schedule_id)),
                self._key("runs", schedule_id),
            ],
            [
                status,
                error,
                entry,
                self.app.config.scheduler.history,
                (
                    ""
                    if schedule.spec.pause_after_failures is None
                    else schedule.spec.pause_after_failures
                ),
                schedule_id,
            ],
        )
        if paused == 2:
            logger.warning(
                "Schedule %s paused after %d failures in a row.",
                schedule_id,
                schedule.spec.pause_after_failures,
            )

    async def expire(self) -> int:
        """
        Delete dynamic schedules whose TTL ran out, and finished ones.

        :return: number deleted.
        """
        redis = self.app.redis
        now = self.app.cluster.clock.now_ms()
        expired = await redis.zrangebyscore(self._key("expiry"), "-inf", now)
        deleted = 0
        for raw in expired:
            deleted += await self._delete(raw.decode(), allow_static=True)
        for raw in await redis.smembers(self._key("dynamic")):
            schedule_id = raw.decode()
            state = await redis.hget(self.def_key(schedule_id), "state")
            if state in (b"done", b"expired"):
                deleted += await self._delete(schedule_id)
        return deleted

    def add_janitor_hooks(self, janitor: Janitor) -> None:
        """
        Register cleanup of expired schedules.

        :param janitor: the janitor.
        """
        janitor.add_cleanup(self._expire_hook)

    async def _expire_hook(self) -> None:
        count = await self.expire()
        if count:
            logger.info("Deleted %d expired or finished schedule(s).", count)
