"""
Task groups: wait for many tasks together.

Send many tasks, then wait for all, the first N, or each as it finishes,
with one blocking read instead of polling every result.

Example::

    group = app.group()
    for n in range(100):
        await group.kiq(square, n)
    results = await group.wait(timeout=30)
    async for handle, result in group.as_completed():
        ...
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from syntasks.exceptions import SynTasksResultTimeoutError
from syntasks.result import SynTasksResult
from syntasks.task import AsyncSynTasksTask

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks

__all__ = ["TaskGroup"]


class TaskGroup:
    """A set of tasks whose results are awaited together."""

    def __init__(self, app: "SynTasks", group_id: str | None = None) -> None:
        self.app = app
        self.id = group_id or uuid.uuid4().hex
        self.handles: list[AsyncSynTasksTask[Any]] = []

    def __len__(self) -> int:
        return len(self.handles)

    async def kiq(self, task: Any, *args: Any, **kwargs: Any) -> AsyncSynTasksTask[Any]:
        """
        Send a task as a member of this group.

        :param task: decorated task or task name.
        :param args: arguments.
        :param kwargs: keyword arguments.
        :return: handle of the sent task.
        """
        kicker = self.app.kicker(task) if isinstance(task, str) else task.kicker()
        handle = await kicker.with_labels(_group=self.id).kiq(*args, **kwargs)
        self.handles.append(handle)
        return handle

    async def as_completed(  # noqa: C901
        self,
        timeout: float | None = None,
    ) -> AsyncIterator[tuple[AsyncSynTasksTask[Any], SynTasksResult[Any]]]:
        """
        Results as tasks finish (finish order).

        :param timeout: total seconds to wait.
        :raises SynTasksResultTimeoutError: if not everything finished in time.
        :yield: ``(handle, result)``.
        """
        backend = self.app.result_backend
        by_id = {handle.task_id: handle for handle in self.handles}
        remaining = set(by_id)
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        # Already finished (e.g. before we started waiting).
        for task_id in sorted(remaining):
            if await backend.is_result_ready(task_id):
                remaining.discard(task_id)
                yield by_id[task_id], await by_id[task_id].get_result()
        cursor = "0"
        key = backend.group_key(self.id)
        while remaining:
            block = 1000
            if deadline is not None:
                left = deadline - loop.time()
                if left <= 0:
                    raise SynTasksResultTimeoutError(timeout=timeout)
                block = max(min(block, int(left * 1000)), 1)
            batches = await backend.redis.xread({key: cursor}, count=500, block=block)
            for _, entries in batches:
                for entry_id, fields in entries:
                    cursor = entry_id.decode()
                    task_id = fields.get(b"task_id", b"").decode()
                    if task_id in remaining:
                        remaining.discard(task_id)
                        yield by_id[task_id], await by_id[task_id].get_result()
            if not batches:
                # Members sent without the group label: check them directly.
                for task_id in sorted(remaining):
                    if await backend.is_result_ready(task_id):
                        remaining.discard(task_id)
                        yield by_id[task_id], await by_id[task_id].get_result()

    async def wait(self, timeout: float | None = None) -> list[SynTasksResult[Any]]:
        """
        Wait for every task.

        :param timeout: seconds.
        :return: results in the order the tasks were sent.
        """
        results: dict[str, SynTasksResult[Any]] = {}
        async for handle, result in self.as_completed(timeout):
            results[handle.task_id] = result
        return [results[handle.task_id] for handle in self.handles]

    async def first(
        self,
        n: int = 1,
        timeout: float | None = None,
    ) -> list[SynTasksResult[Any]]:
        """
        Wait for the first ``n`` tasks to finish.

        :param n: how many.
        :param timeout: seconds.
        :return: results in finish order.
        """
        found: list[SynTasksResult[Any]] = []
        async for _, result in self.as_completed(timeout):
            found.append(result)
            if len(found) >= n:
                break
        return found
