"""Running tasks across the cluster (``app.tasks``)."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from syntasks.task import AsyncSynTasksTask

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks

__all__ = ["RunningTaskInfo", "TaskControl"]


@dataclass(frozen=True, kw_only=True)
class RunningTaskInfo:
    """A task running somewhere in the cluster."""

    task_id: str
    task_name: str
    node_id: str
    host: str


class TaskControl:
    """See and cancel running tasks."""

    def __init__(self, app: "SynTasks") -> None:
        self.app = app

    async def running(self) -> list[RunningTaskInfo]:
        """Tasks running now, as reported by node heartbeats."""
        result = []
        for node in await self.app.nodes():
            if not node.alive:
                continue
            for task_id, task_name in node.running_tasks.items():
                result.append(
                    RunningTaskInfo(
                        task_id=task_id,
                        task_name=task_name,
                        node_id=node.node_id,
                        host=node.host,
                    ),
                )
        return sorted(result, key=lambda info: info.task_id)

    async def cancel(self, task_id: str) -> None:
        """
        Ask the node running a task to cancel it.

        The run ends with :class:`~syntasks.exceptions.TaskCancelledError`.

        :param task_id: task id.
        """
        await self.app.cluster.control.publish("task.cancel", task_id=task_id)

    def handle(self, task_id: str, return_type: Any = None) -> AsyncSynTasksTask[Any]:
        """
        Handle of a task by id, to read or wait for its result.

        :param task_id: task id.
        :param return_type: type to convert the return value to.
        :return: task handle.
        """
        return AsyncSynTasksTask(task_id, self.app.result_backend, return_type)
