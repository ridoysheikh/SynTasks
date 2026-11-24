import asyncio
from logging import getLogger
from time import time
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from syntasks.codec import convert
from syntasks.exceptions import (
    ResultGetError,
    ResultIsReadyError,
    SynTasksResultTimeoutError,
)

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.abc.result_backend import AsyncResultBackend
    from syntasks.depends.progress_tracker import TaskProgress
    from syntasks.result import SynTasksResult

logger = getLogger("syntasks.task")

_ReturnType = TypeVar("_ReturnType")


class AsyncSynTasksTask(Generic[_ReturnType]):
    """AsyncTask for AsyncResultBackend."""

    def __init__(
        self,
        task_id: str,
        result_backend: "AsyncResultBackend[_ReturnType]",
        return_type: type[_ReturnType] | None = None,
    ) -> None:
        self.task_id = task_id
        self.result_backend = result_backend
        self.return_type = return_type

    async def is_ready(self) -> bool:
        """
        Checks if task is completed.

        :raises ResultIsReadyError: if we can't get info about task readiness.

        :return: True if task is completed.
        """
        try:
            return await self.result_backend.is_result_ready(self.task_id)
        except Exception as exc:
            raise ResultIsReadyError from exc

    async def get_result(
        self,
        with_logs: bool = False,
    ) -> "SynTasksResult[_ReturnType]":
        """
        Get result of a task from result backend.

        :param with_logs: whether you want to fetch logs from worker.

        :raises ResultGetError: if we can't get result from ResultBackend.

        :return: task's return value.
        """
        try:
            res = await self.result_backend.get_result(
                self.task_id,
                with_logs=with_logs,
            )
            if self.return_type is not None:
                try:
                    res.return_value = convert(
                        res.return_value,
                        self.return_type,
                    )
                except ValueError:
                    logger.warning("Cannot parse return type into %s", self.return_type)
            return res
        except Exception as exc:
            raise ResultGetError from exc

    async def wait_result(
        self,
        check_interval: float = 0.2,
        timeout: float = -1.0,
        with_logs: bool = False,
    ) -> "SynTasksResult[_ReturnType]":
        """
        Waits until result is ready.

        This method just checks whether the task is
        ready. And if it is it returns the result.

        It may throw SynTasksResultTimeoutError if
        task didn't become ready in provided
        period of time.

        :param check_interval: How often checks are performed.
        :param timeout: timeout for the result.
        :param with_logs: whether you want to fetch logs from worker.
        :raises SynTasksResultTimeoutError: if task didn't
            become ready in provided period of time.
        :return: task's return value.
        """
        start_time = time()
        while not await self.is_ready():
            if 0 < timeout < time() - start_time:
                raise SynTasksResultTimeoutError(timeout=timeout)
            await asyncio.sleep(check_interval)
        return await self.get_result(with_logs=with_logs)

    async def get_progress(self) -> "TaskProgress[Any] | None":
        """
        Get task progress.

        :return: task's progress.
        """
        return await self.result_backend.get_progress(self.task_id)
