"""Redis result backend."""

from typing import Any, TypeVar

from syntasks.abc.result_backend import AsyncResultBackend
from syntasks.abc.serializer import SynTasksSerializer
from syntasks.depends.progress_tracker import TaskProgress
from syntasks.exceptions import ResultIsMissingError
from syntasks.redis.broker import DEFAULT_URL, make_client
from syntasks.redis.client import Redis
from syntasks.result import SynTasksResult
from syntasks.serializers import JSONSerializer

_ReturnType = TypeVar("_ReturnType")


class RedisResultBackend(AsyncResultBackend[_ReturnType]):
    """
    Stores task results and progress in Redis.

    Keys: ``{prefix}:{task_id}`` for results and
    ``{prefix}:{task_id}:progress`` for progress.
    """

    def __init__(
        self,
        url: str = DEFAULT_URL,
        *,
        keep_results: bool = True,
        result_ex_time: int | None = None,
        result_px_time: int | None = None,
        prefix: str = "syntasks:result",
        serializer: SynTasksSerializer | None = None,
        client: Redis | None = None,
        max_connection_pool_size: int | None = None,
        **connection_kwargs: Any,
    ) -> None:
        """
        Create the result backend.

        :param url: Redis URL.
        :param keep_results: keep results after they're read; if False,
            reading a result deletes it.
        :param result_ex_time: expire results after this many seconds.
        :param result_px_time: expire results after this many milliseconds.
        :param prefix: key prefix.
        :param serializer: how results are encoded; JSON by default.
        :param client: share an existing client.
        :param max_connection_pool_size: connections in the pool.
        :param connection_kwargs: extra connection options.
        :raises ValueError: if both or invalid expiry times are given.
        """
        if result_ex_time is not None and result_px_time is not None:
            raise ValueError("Choose either result_ex_time or result_px_time.")
        for name, value in (
            ("result_ex_time", result_ex_time),
            ("result_px_time", result_px_time),
        ):
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be a positive number.")
        self.keep_results = keep_results
        self.result_ex_time = result_ex_time
        self.result_px_time = result_px_time
        self.prefix = prefix
        self.serializer = serializer or JSONSerializer()
        self.redis, self._owns_client = make_client(
            url,
            client,
            max_connection_pool_size,
            connection_kwargs,
        )

    def result_key(self, task_id: str) -> str:
        """Key that holds the result of a task."""
        return f"{self.prefix}:{task_id}"

    def progress_key(self, task_id: str) -> str:
        """Key that holds the progress of a task."""
        return f"{self.prefix}:{task_id}:progress"

    async def shutdown(self) -> None:
        """Close the backend's connections."""
        if self._owns_client:
            await self.redis.close()

    async def _store(self, key: str, value: bytes) -> None:
        await self.redis.set(
            key,
            value,
            ex=self.result_ex_time,
            px=self.result_px_time,
        )

    async def set_result(
        self,
        task_id: str,
        result: SynTasksResult[_ReturnType],
    ) -> None:
        """
        Save a result.

        :param task_id: task id.
        :param result: result to save.
        """
        await self._store(
            self.result_key(task_id),
            self.serializer.dumpb(result.to_dict()),
        )

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Check whether a result exists.

        :param task_id: task id.
        :return: True if the result is stored.
        """
        return await self.redis.exists(self.result_key(task_id)) == 1

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> SynTasksResult[_ReturnType]:
        """
        Read a result.

        :param task_id: task id.
        :param with_logs: keep the (deprecated) ``log`` field.
        :raises ResultIsMissingError: if there is no result.
        :return: the result.
        """
        key = self.result_key(task_id)
        if self.keep_results:
            raw = await self.redis.get(key)
        else:
            raw = await self.redis.getdel(key)
        if raw is None:
            raise ResultIsMissingError(task_id=task_id)
        result: SynTasksResult[Any] = SynTasksResult.from_dict(
            self.serializer.loadb(raw),
        )
        if not with_logs:
            result.log = None
        return result

    async def set_progress(self, task_id: str, progress: TaskProgress[Any]) -> None:
        """
        Save task progress.

        :param task_id: task id.
        :param progress: current progress.
        """
        await self._store(
            self.progress_key(task_id),
            self.serializer.dumpb(progress.to_dict()),
        )

    async def get_progress(self, task_id: str) -> TaskProgress[Any] | None:
        """
        Read task progress.

        :param task_id: task id.
        :return: progress, or None if there is none.
        """
        raw = await self.redis.get(self.progress_key(task_id))
        if raw is None:
            return None
        return TaskProgress.from_dict(self.serializer.loadb(raw))
