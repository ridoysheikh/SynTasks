"""
What happens when a task finishes: callbacks, fan-in and batches.

* ``on_success`` / ``on_failure`` (or ``.then()`` / ``.catch()`` per send) send
  a callback task with a :class:`TaskOutcome` after the final attempt.
* ``group.then(task)`` sends ``task`` once with a :class:`GroupOutcome` after
  the last member of a group finishes, on any machine.
* :class:`Batch` is a named, persistent group (a campaign): pause, resume,
  cancel, progress, and ``then`` with a :class:`BatchOutcome`.

Callbacks are sent exactly once: their task id comes from the parent's and
the send is deduplicated by a one-time key.

Keys (under ``syntasks:{<app>}:``):

==============================  ===============================================
``res:grp:<id>:bar``            HASH: total, ok, failed, then, fired (fan-in)
``res:grp:<id>:seen``           SET of finished member task ids
``batch:<id>``                  HASH: state, owner, total, sent, done, failed,
                                cancelled, sealed, then, fired, created
``batch:<id>:seen``             SET of finished member task ids
``batch:<id>:parked``           LIST of members taken while paused
==============================  ===============================================
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import time
import uuid
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from syntasks.cluster.scripts import Script

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks
    from syntasks.message import SynTasksMessage
    from syntasks.options import TaskOptions
    from syntasks.queues.engine import Delivery
    from syntasks.result import SynTasksResult
    from syntasks.task import AsyncSynTasksTask

__all__ = [
    "Batch",
    "BatchOutcome",
    "BatchProgress",
    "Completion",
    "GroupOutcome",
    "TaskOutcome",
]

logger = logging.getLogger("syntasks.completion")

_NAMESPACE = uuid.UUID("0b8f4c6e-3f6a-4f2e-9a43-6a1d2c7e5b91")
_KEEP_MS = 7 * 86_400_000  # batches and group barriers live this long after a change
_MAX_IDS = 1000  # task ids sent along in a GroupOutcome


@dataclass(frozen=True)
class TaskOutcome:
    """How a task ended (the argument of ``on_success`` / ``on_failure``)."""

    task_id: str
    task_name: str
    ok: bool
    return_value: Any = None
    error: str | None = None
    error_type: str | None = None
    labels: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GroupOutcome:
    """How a group ended (the argument of a ``group.then`` callback)."""

    group_id: str
    total: int
    ok: int
    failed: int
    task_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class BatchOutcome:
    """How a batch ended (the argument of a ``batch.then`` callback)."""

    batch_id: str
    total: int
    done: int
    failed: int
    cancelled: int


@dataclass(frozen=True)
class BatchProgress:
    """Counters of a batch."""

    state: str
    total: int
    done: int
    failed: int
    cancelled: int
    sealed: bool

    @property
    def pending(self) -> int:
        """Members not finished yet (waiting or running)."""
        return max(self.total - self.done - self.failed - self.cancelled, 0)

    @property
    def finished(self) -> bool:
        """Whether every member is done (and no more will be added)."""
        return self.state == "cancelled" or (self.sealed and self.pending == 0)


_FIRE = """
local function fire(hash, finished, total)
  local then_task = redis.call('HGET', hash, 'then')
  if then_task and then_task ~= '' and redis.call('HGET', hash, 'fired') ~= '1'
     and total and finished >= tonumber(total) then
    redis.call('HSET', hash, 'fired', '1')
    return then_task
  end
  return false
end
"""

# KEYS: 1 barrier hash, 2 seen set; ARGV: 2 task id, 3 '1' ok | '0' failed, 4 keep ms
# Returns {} or {then task, total, ok, failed}.
_GROUP_DONE = Script(
    "group_done",
    _FIRE
    + """
if redis.call('SADD', KEYS[2], ARGV[2]) == 1 then
  redis.call('HINCRBY', KEYS[1], ARGV[3] == '1' and 'ok' or 'failed', 1)
end
redis.call('PEXPIRE', KEYS[1], ARGV[4])
redis.call('PEXPIRE', KEYS[2], ARGV[4])
local ok = tonumber(redis.call('HGET', KEYS[1], 'ok') or '0')
local failed = tonumber(redis.call('HGET', KEYS[1], 'failed') or '0')
local total = redis.call('HGET', KEYS[1], 'total')
local then_task = fire(KEYS[1], ok + failed, total)
if then_task then return {then_task, total, tostring(ok), tostring(failed)} end
return {}
""",
)

# KEYS: 1 barrier hash, 2 seen set; ARGV: 2 total, 3 then task, 4 keep ms
_GROUP_SEAL = Script(
    "group_seal",
    _FIRE
    + """
redis.call('HSET', KEYS[1], 'total', ARGV[2], 'then', ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
local ok = tonumber(redis.call('HGET', KEYS[1], 'ok') or '0')
local failed = tonumber(redis.call('HGET', KEYS[1], 'failed') or '0')
local then_task = fire(KEYS[1], ok + failed, ARGV[2])
if then_task then return {then_task, ARGV[2], tostring(ok), tostring(failed)} end
return {}
""",
)

_BATCH_FIRE = (
    _FIRE
    + """
local function batch_fire(hash)
  local data = redis.call('HMGET', hash, 'total', 'done', 'failed', 'cancelled',
    'sealed', 'state')
  local total = tonumber(data[1] or '0')
  local done, failed, cancelled = tonumber(data[2] or '0'), tonumber(data[3] or '0'),
    tonumber(data[4] or '0')
  local finished = done + failed + cancelled
  if data[5] ~= '1' and data[6] ~= 'cancelled' then return {} end
  if data[6] == 'cancelled' then finished = total end
  local then_task = fire(hash, finished, tostring(total))
  if then_task then
    return {then_task, tostring(total), tostring(done), tostring(failed),
      tostring(cancelled)}
  end
  return {}
end
"""
)

# KEYS: 1 batch hash, 2 seen set; ARGV: 2 task id, 3 done|failed|cancelled, 4 keep ms
_BATCH_DONE = Script(
    "batch_done",
    _BATCH_FIRE
    + """
if redis.call('EXISTS', KEYS[1]) == 0 then return {} end
if redis.call('SADD', KEYS[2], ARGV[2]) == 1 then
  redis.call('HINCRBY', KEYS[1], ARGV[3], 1)
end
redis.call('PEXPIRE', KEYS[1], ARGV[4])
redis.call('PEXPIRE', KEYS[2], ARGV[4])
return batch_fire(KEYS[1])
""",
)

# KEYS: 1 batch hash; ARGV: 2 then task|''
_BATCH_SEAL = Script(
    "batch_seal",
    _BATCH_FIRE
    + """
if ARGV[2] ~= '' then redis.call('HSET', KEYS[1], 'then', ARGV[2]) end
redis.call('HSET', KEYS[1], 'sealed', '1')
return batch_fire(KEYS[1])
""",
)

# A member was taken: run it, park it (paused), or drop it (cancelled).
# KEYS: 1 batch hash, 2 parked list; ARGV: 2 parked entry
_BATCH_GATE = Script(
    "batch_gate",
    """
local state = redis.call('HGET', KEYS[1], 'state')
if state == 'paused' then
  redis.call('RPUSH', KEYS[2], ARGV[2])
  return 'parked'
end
if state == 'cancelled' then return 'cancelled' end
return 'run'
""",
)

# Pause, resume or cancel a batch.
# KEYS: 1 batch hash, 2 parked list, 3 seen set
# ARGV: 2 pause|resume|cancel, 3 queue key prefix ("…:q:"), 4 keep ms
# Parked entries are "<task id>\\n<queue>\\n<delayed member>".
_BATCH_STATE = Script(
    "batch_state",
    _BATCH_FIRE
    + """
if redis.call('EXISTS', KEYS[1]) == 0 then return {'missing'} end
local state = redis.call('HGET', KEYS[1], 'state')
if state == 'cancelled' then return {'cancelled'} end
if ARGV[2] == 'pause' then
  redis.call('HSET', KEYS[1], 'state', 'paused')
  return {'paused'}
end
local now = now_ms()
local parked = redis.call('LRANGE', KEYS[2], 0, -1)
redis.call('DEL', KEYS[2])
for _, entry in ipairs(parked) do
  local first = string.find(entry, '\\n', 1, true)
  local second = string.find(entry, '\\n', first + 1, true)
  local task_id = string.sub(entry, 1, first - 1)
  local queue = string.sub(entry, first + 1, second - 1)
  local member = string.sub(entry, second + 1)
  if ARGV[2] == 'resume' then
    redis.call('ZADD', ARGV[3] .. queue .. ':delayed', now, member)
  elseif redis.call('SADD', KEYS[3], task_id) == 1 then
    redis.call('HINCRBY', KEYS[1], 'cancelled', 1)
  end
end
redis.call('HSET', KEYS[1], 'state', ARGV[2] == 'resume' and 'active' or 'cancelled')
redis.call('PEXPIRE', KEYS[1], ARGV[4])
local fired = batch_fire(KEYS[1])
if #fired > 0 then
  return {'fired', fired[1], fired[2], fired[3], fired[4], fired[5]}
end
return {ARGV[2] == 'resume' and 'active' or 'cancelled', tostring(#parked)}
""",
)


def _decode(reply: list[Any]) -> list[str]:
    return [item.decode() if isinstance(item, bytes) else str(item) for item in reply]


class Completion:
    """Callbacks, group fan-in and batch bookkeeping of an app."""

    def __init__(self, app: SynTasks) -> None:
        self.app = app

    # -- callbacks ---------------------------------------------------------------

    async def send_callback(self, task_name: str, payload: Any, once: str) -> None:
        """
        Send a callback task exactly once.

        :param task_name: callback task.
        :param payload: its argument (a dataclass).
        :param once: stable text the callback's task id is derived from.
        """
        task_id = uuid.uuid5(_NAMESPACE, once).hex
        data = dataclasses.asdict(payload)
        kicker = (
            self.app.kicker(task_name).with_task_id(task_id).with_labels(_once=task_id)
        )
        try:
            await kicker.kiq(data)
        except (TypeError, ValueError):
            if "return_value" not in data:
                raise
            logger.warning(
                "Return value of %s isn't JSON; the callback gets None.", once
            )
            await kicker.kiq({**data, "return_value": None})

    async def after_run(
        self,
        message: SynTasksMessage,
        result: SynTasksResult[Any],
        options: TaskOptions,
    ) -> None:
        """
        After a task's final attempt: callbacks, group fan-in, batch counters.

        :param message: the run's message.
        :param result: its result.
        :param options: task options.
        """
        labels = message.labels
        ok = not result.is_err
        targets = []
        if ok:
            targets = [options.on_success, labels.get("_then")]
        else:
            targets = [options.on_failure, labels.get("_catch")]
        names = list(dict.fromkeys(str(t) for t in targets if t))
        if names:
            outcome = _outcome(message, result)
            for name in names:
                await self.send_callback(name, outcome, f"{message.task_id}:{name}")
        group = labels.get("_group")
        if group:
            await self._group_done(str(group), message.task_id, ok)
        batch = labels.get("_batch")
        if batch:
            await self.batch_done(
                str(batch), message.task_id, "done" if ok else "failed"
            )

    # -- groups ----------------------------------------------------------------------

    def _group_keys(self, group_id: str) -> list[str]:
        return [
            self.app.keys("res", "grp", group_id, "bar"),
            self.app.keys("res", "grp", group_id, "seen"),
        ]

    async def _group_done(self, group_id: str, task_id: str, ok: bool) -> None:
        reply = await _GROUP_DONE(
            self.app.redis,
            self._group_keys(group_id),
            [task_id, "1" if ok else "0", _KEEP_MS],
        )
        await self._fire_group(group_id, reply)

    async def seal_group(self, group_id: str, total: int, then_task: str) -> None:
        """
        Send ``then_task`` once every one of ``total`` members has finished.

        :param group_id: group id.
        :param total: members.
        :param then_task: callback task name.
        """
        reply = await _GROUP_SEAL(
            self.app.redis,
            self._group_keys(group_id),
            [total, then_task, _KEEP_MS],
        )
        await self._fire_group(group_id, reply)

    async def _fire_group(self, group_id: str, reply: list[Any]) -> None:
        if not reply:
            return
        then_task, total, ok, failed = _decode(reply)
        ids: list[str] = []
        if int(total) <= _MAX_IDS:
            ids = sorted(
                raw.decode()
                for raw in await self.app.redis.smembers(self._group_keys(group_id)[1])
            )
        outcome = GroupOutcome(group_id, int(total), int(ok), int(failed), ids)
        await self.send_callback(then_task, outcome, f"group:{group_id}:then")

    # -- batches ----------------------------------------------------------------------

    def batch_keys(self, batch_id: str) -> list[str]:
        """``[hash, seen set, parked list]`` of a batch."""
        return [
            self.app.keys("batch", batch_id),
            self.app.keys("batch", batch_id, "seen"),
            self.app.keys("batch", batch_id, "parked"),
        ]

    async def batch_done(self, batch_id: str, task_id: str, outcome: str) -> None:
        """
        Count a finished batch member.

        :param batch_id: batch id.
        :param task_id: member task id.
        :param outcome: ``done``, ``failed`` or ``cancelled``.
        """
        hash_key, seen, _ = self.batch_keys(batch_id)
        reply = await _BATCH_DONE(
            self.app.redis, [hash_key, seen], [task_id, outcome, _KEEP_MS]
        )
        await self.fire_batch(batch_id, reply)

    async def fire_batch(self, batch_id: str, reply: list[Any]) -> None:
        """Send a batch's ``then`` callback if the script says it finished."""
        if not reply:
            return
        then_task, total, done, failed, cancelled = _decode(reply)
        outcome = BatchOutcome(
            batch_id, int(total), int(done), int(failed), int(cancelled)
        )
        await self.send_callback(then_task, outcome, f"batch:{batch_id}:then")

    async def batch_gate(self, delivery: Delivery, message: SynTasksMessage) -> bool:
        """
        Whether a batch member may run now.

        Paused: the member is parked until :meth:`Batch.resume`. Cancelled:
        it is counted as cancelled and dropped.

        :param delivery: the delivery.
        :param message: its message.
        :return: True to run it.
        """
        batch_id = str(message.labels["_batch"])
        hash_key, _, parked = self.batch_keys(batch_id)
        engine = self.app.engine
        member = engine.delayed_member(
            delivery.stream, delivery.task_name, delivery.data
        )
        entry = (
            message.task_id.encode() + b"\n" + delivery.queue.encode() + b"\n" + member
        )
        state = await _BATCH_GATE(self.app.redis, [hash_key, parked], [entry])
        if state == b"run":
            return True
        if state == b"cancelled":
            await self.batch_done(batch_id, message.task_id, "cancelled")
        await engine.ack(delivery)
        await engine.finish(delivery)
        return False


def _outcome(message: SynTasksMessage, result: SynTasksResult[Any]) -> TaskOutcome:
    error = result.error
    return TaskOutcome(
        task_id=message.task_id,
        task_name=message.task_name,
        ok=not result.is_err,
        return_value=None if result.is_err else result.return_value,
        error=None if error is None else str(error) or repr(error),
        error_type=None if error is None else type(error).__name__,
        labels={k: v for k, v in message.labels.items() if not str(k).startswith("_")},
    )


class Batch:
    """
    A named, persistent group of tasks: a campaign.

    Example::

        campaign = await app.batch("spring-sale", owner="team:marketing")
        await campaign.map(send_email, ({"to": a} for a in addresses))
        await campaign.pause(); await campaign.resume()
        print(await campaign.progress())
    """

    def __init__(self, app: SynTasks, batch_id: str) -> None:
        self.app = app
        self.id = batch_id
        self._keys = app.completion.batch_keys(batch_id)

    async def _create(self, owner: str | None) -> None:
        await self.app.redis.execute(
            "HSETNX",
            self._keys[0],
            "state",
            "active",
        )
        mapping = {"created": str(int(time.time() * 1000))}
        if owner is not None:
            mapping["owner"] = owner
        for name, value in mapping.items():
            await self.app.redis.execute("HSETNX", self._keys[0], name, value)
        await self.app.redis.execute("PEXPIRE", self._keys[0], _KEEP_MS)

    async def kiq(self, task: Any, *args: Any, **kwargs: Any) -> AsyncSynTasksTask[Any]:
        """
        Send one task as a member of this batch.

        :param task: decorated task or task name.
        :param args: arguments.
        :param kwargs: keyword arguments.
        :return: handle.
        """
        await self.app.redis.execute("HINCRBY", self._keys[0], "total", 1)
        kicker = self.app.kicker(task) if isinstance(task, str) else task.kicker()
        return await kicker.with_labels(_batch=self.id).kiq(*args, **kwargs)

    async def map(  # noqa: C901
        self,
        task: Any,
        items: Iterable[Any],
        *,
        chunk: int = 1000,
        seal: bool = True,
        resume: bool = False,
    ) -> int:
        """
        Send one task per item.

        A dict item is keyword arguments, a tuple or list positional
        arguments, anything else the only argument.

        :param task: decorated task or task name.
        :param items: the items.
        :param chunk: items sent together; progress is saved after each chunk.
        :param seal: no more members after this (lets ``then`` fire).
        :param resume: skip the items an earlier, interrupted ``map`` sent.
        :return: members sent by this call.
        """
        skip = 0
        if resume:
            skip = int(await self.app.redis.execute("HGET", self._keys[0], "sent") or 0)
        sent = 0
        batch: list[Any] = []

        async def flush() -> None:
            nonlocal sent
            if not batch:
                return
            await self.app.redis.execute("HINCRBY", self._keys[0], "total", len(batch))
            kicker_for = (
                (lambda: self.app.kicker(task))
                if isinstance(task, str)
                else task.kicker
            )
            sends = []
            for item in batch:
                kicker = kicker_for().with_labels(_batch=self.id)
                if isinstance(item, dict):
                    sends.append(kicker.kiq(**item))
                elif isinstance(item, (tuple, list)):
                    sends.append(kicker.kiq(*item))
                else:
                    sends.append(kicker.kiq(item))
            await asyncio.gather(*sends)
            await self.app.redis.execute("HINCRBY", self._keys[0], "sent", len(batch))
            sent += len(batch)
            batch.clear()

        for index, item in enumerate(items):
            if index < skip:
                continue
            batch.append(item)
            if len(batch) >= chunk:
                await flush()
        await flush()
        if seal:
            await self.seal()
        return sent

    async def seal(self) -> None:
        """No more members: lets ``then`` fire once all of them finished."""
        reply = await _BATCH_SEAL(self.app.redis, [self._keys[0]], [""])
        await self.app.completion.fire_batch(self.id, reply)

    async def then(self, task: Any) -> None:
        """
        Send ``task`` once with a :class:`BatchOutcome` when the batch ends.

        Seals the batch.

        :param task: decorated task or task name.
        """
        name = task if isinstance(task, str) else task.task_name
        reply = await _BATCH_SEAL(self.app.redis, [self._keys[0]], [name])
        await self.app.completion.fire_batch(self.id, reply)

    async def _state(self, action: str) -> None:
        hash_key, seen, parked = self._keys
        reply = await _BATCH_STATE(
            self.app.redis,
            [hash_key, parked, seen],
            [action, self.app.keys("q") + ":", _KEEP_MS],
        )
        status = reply[0].decode()
        if status == "missing":
            raise KeyError(f"No batch {self.id!r}.")
        if status == "fired":
            await self.app.completion.fire_batch(self.id, reply[1:])
        if action == "resume":
            with contextlib.suppress(Exception):
                await self.app.cluster.control.publish("delayed", queue="*")

    async def pause(self) -> None:
        """Members not started yet wait until :meth:`resume`."""
        await self._state("pause")

    async def resume(self) -> None:
        """Let a paused batch run again."""
        await self._state("resume")

    async def cancel(self) -> None:
        """Members not started yet are dropped (counted as cancelled)."""
        await self._state("cancel")

    async def progress(self) -> BatchProgress:
        """
        The batch's counters.

        :return: progress.
        """
        raw = await self.app.redis.hgetall(self._keys[0])
        data = {k.decode(): v.decode() for k, v in raw.items()}
        return BatchProgress(
            state=data.get("state", "active"),
            total=int(data.get("total", 0)),
            done=int(data.get("done", 0)),
            failed=int(data.get("failed", 0)),
            cancelled=int(data.get("cancelled", 0)),
            sealed=data.get("sealed") == "1",
        )

    async def watch(self, interval: float = 0.5) -> AsyncIterator[BatchProgress]:
        """
        Progress whenever it changes, until the batch is finished.

        :param interval: seconds between checks.
        :yield: progress.
        """
        last = None
        while True:
            current = await self.progress()
            if current != last:
                yield current
                last = current
            if current.finished:
                return
            await asyncio.sleep(interval)
