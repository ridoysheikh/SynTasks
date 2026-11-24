"""Result backend of the SynTasks app."""

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from syntasks.abc.serializer import SynTasksSerializer
from syntasks.depends.progress_tracker import TaskProgress
from syntasks.exceptions import ResultTooLargeError
from syntasks.redis.result_backend import RedisResultBackend
from syntasks.result import SynTasksResult

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks

__all__ = ["AppResultBackend", "make_serializer"]


def make_serializer(name: str) -> SynTasksSerializer:
    """
    Serializer by config name.

    :param name: ``json``, ``pickle``, ``orjson``, ``msgpack`` or ``cbor``.
    :return: serializer.
    """
    from syntasks import serializers  # noqa: PLC0415

    factories: dict[str, Any] = {
        "json": serializers.JSONSerializer,
        "pickle": serializers.PickleSerializer,
        "orjson": serializers.ORJSONSerializer,
        "msgpack": serializers.MSGPackSerializer,
        "cbor": serializers.CBORSerializer,
    }
    return factories[name]()


class AppResultBackend(RedisResultBackend[Any]):
    """
    Results in Redis under ``syntasks:{<app>}:res:<task_id>``.

    Each result gets its task's ``result_ttl``. When a result is stored, a
    one-entry stream ``res:done:<task_id>`` is written, so waiters block on
    it instead of polling (see :meth:`wait`).
    """

    done_ttl_ms = 60_000

    def __init__(self, app: "SynTasks") -> None:
        config = app.config
        super().__init__(
            client=app.redis,
            prefix=app.keys("res"),
            serializer=make_serializer(config.results.serializer),
            keep_results=config.results.keep_results,
        )
        self.app = app

    def group_key(self, group_id: str) -> str:
        """Stream of a group's finished tasks."""
        return f"{self.prefix}:group:{group_id}"

    def progress_stream(self, task_id: str) -> str:
        """Stream of a task's progress updates."""
        return f"{self.prefix}:{task_id}:progress:stream"

    async def set_progress(self, task_id: str, progress: Any) -> None:
        """
        Save progress and push it to watchers.

        :param task_id: task id.
        :param progress: current progress.
        """
        await super().set_progress(task_id, progress)
        stream = self.progress_stream(task_id)
        pipe = self.redis.pipeline()
        pipe.add(
            "XADD",
            stream,
            "MAXLEN",
            "~",
            100,
            "*",
            "data",
            self.serializer.dumpb(progress.to_dict()),
        )
        pipe.add("PEXPIRE", stream, 3_600_000)
        await pipe.execute()

    async def watch_progress(
        self,
        task_id: str,
        *,
        timeout: float | None = None,
    ) -> "AsyncIterator[TaskProgress[Any]]":
        """
        Progress updates as they happen, until the result is stored.

        :param task_id: task id.
        :param timeout: stop after this many seconds.
        :yield: progress updates.
        """
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        cursors = {self.progress_stream(task_id): "0", self.done_key(task_id): "0"}
        while True:
            block = 1000
            if deadline is not None:
                left = deadline - loop.time()
                if left <= 0:
                    return
                block = max(min(block, int(left * 1000)), 1)
            batches = await self.redis.xread(cursors, count=100, block=block)
            finished = False
            for raw_key, entries in batches:
                key = raw_key.decode()
                if not entries:
                    continue
                cursors[key] = entries[-1][0].decode()
                if key == self.done_key(task_id):
                    finished = True
                    continue
                for _, fields in entries:
                    yield TaskProgress.from_dict(self.serializer.loadb(fields[b"data"]))
            if finished:
                return

    def done_key(self, task_id: str) -> str:
        """Stream written once when the task's result is stored."""
        return f"{self.prefix}:done:{task_id}"

    async def store(
        self,
        task_id: str,
        result: SynTasksResult[Any],
        *,
        ttl: float | None,
        max_size: int | None = None,
    ) -> None:
        """
        Save a result and notify waiters.

        :param task_id: task id.
        :param result: result.
        :param ttl: seconds to keep it (None = forever).
        :param max_size: max encoded size; bigger results are replaced by
            a :class:`ResultTooLargeError` result.
        """
        data = self.serializer.dumpb(result.to_dict())
        if max_size is not None and len(data) > max_size:
            replacement: SynTasksResult[Any] = SynTasksResult(
                is_err=True,
                log=None,
                return_value=None,
                execution_time=result.execution_time,
                labels=result.labels,
                error=ResultTooLargeError(
                    task_id=task_id,
                    size=len(data),
                    limit=max_size,
                ),
            )
            data = self.serializer.dumpb(replacement.to_dict())
        pipe = self.redis.pipeline()
        if ttl:
            pipe.add(
                "SET",
                self.result_key(task_id),
                data,
                "PX",
                max(int(ttl * 1000), 1),
            )
        else:
            pipe.add("SET", self.result_key(task_id), data)
        done = self.done_key(task_id)
        pipe.add("XADD", done, "*", "done", "1")
        pipe.add("PEXPIRE", done, self.done_ttl_ms)
        group = result.labels.get("_group") if isinstance(result.labels, dict) else None
        if group:
            group_key = self.group_key(str(group))
            pipe.add(
                "XADD",
                group_key,
                "*",
                "task_id",
                task_id,
                "status",
                "error" if result.is_err else "ok",
            )
            group_ttl = int((ttl or 86400) * 1000)
            pipe.add("PEXPIRE", group_key, max(group_ttl, self.done_ttl_ms))
        await pipe.execute()

    async def set_result(self, task_id: str, result: SynTasksResult[Any]) -> None:
        """
        Save a result with the default TTL.

        :param task_id: task id.
        :param result: result.
        """
        await self.store(task_id, result, ttl=self.app.config.defaults.result_ttl)

    async def wait(self, task_id: str, timeout: float | None) -> bool:
        """
        Block until a task's result is stored.

        :param task_id: task id.
        :param timeout: seconds (None waits forever).
        :return: True if the result is ready.
        """
        if await self.is_result_ready(task_id):
            return True
        block = 0 if timeout is None else max(int(timeout * 1000), 1)
        reply = await self.redis.xread(
            {self.done_key(task_id): "0"},
            count=1,
            block=block,
        )
        if reply:
            return True
        return await self.is_result_ready(task_id)
