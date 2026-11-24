import asyncio
import json
import urllib.error
import urllib.request
from datetime import UTC, datetime
from logging import getLogger
from typing import Any
from urllib.parse import urljoin

from syntasks.abc.middleware import SynTasksMiddleware
from syntasks.message import SynTasksMessage
from syntasks.result import SynTasksResult

__all__ = ("SynTasksAdminMiddleware",)

_logger = getLogger("syntasks.syntasks_admin_middleware")


class SynTasksAdminMiddleware(SynTasksMiddleware):
    """A SynTasks middleware that reports task lifecycle events to an admin API.

    This middleware sends HTTP POST requests to a configured endpoint when tasks
    are queued, started, or completed. It can be used for task monitoring, auditing,
    or visualization in external systems.

    Attributes:
        url (str): Base URL of the admin API.
        api_token (str): Token used for authenticating with the API.
        timeout (int): Timeout (in seconds) for API requests.
        syntasks_broker_name (str | None): Optional name of the broker instance
            to include in the payload.
        _pending (set[asyncio.Task]): Set of currently running background request tasks.

    Requests are sent with the standard library's ``urllib`` in a worker
    thread, so they never block the event loop.
    """

    def __init__(
        self,
        url: str,
        api_token: str,
        timeout: int = 5,
        syntasks_broker_name: str | None = None,
    ) -> None:
        super().__init__()
        self.url = url
        self.timeout = timeout
        self.api_token = api_token
        self.__ta_broker_name = syntasks_broker_name
        self._pending: set[asyncio.Task[Any]] = set()

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).replace(tzinfo=None).isoformat()

    def _post(self, endpoint: str, payload: dict[str, Any]) -> None:
        """Send one POST request (blocking; runs in a worker thread)."""
        request = urllib.request.Request(  # noqa: S310
            urljoin(self.url, endpoint),
            data=json.dumps(payload).encode(),
            headers={
                "access-token": self.api_token,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(  # noqa: S310
                request,
                timeout=self.timeout,
            ) as resp:
                resp.read()
        except urllib.error.HTTPError as exc:
            _logger.warning(
                "POST %s failed: %s %s",
                endpoint,
                exc.code,
                exc.read().decode(errors="replace"),
            )

    async def shutdown(self) -> None:
        """Shutdown method to wait for all pending requests.

        :returns nothing.
        """
        if self._pending:
            await asyncio.gather(*self._pending, return_exceptions=True)

    async def _spawn_request(
        self,
        endpoint: str,
        payload: dict[str, Any],
    ) -> None:
        """Fire and forget helper.

        start an async POST to the admin API, keep the resulting Task in _pending
        so it can be awaited/cleaned during graceful shutdown.
        """

        async def _send() -> None:
            try:
                await asyncio.to_thread(self._post, endpoint, payload)
            except Exception as exc:
                _logger.warning(
                    "Failed to report to taskiq-admin %s: %r",
                    endpoint,
                    exc,
                )

        task = asyncio.create_task(_send())
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def post_send(self, message: SynTasksMessage) -> None:
        """
        This hook is executed right after the task is sent.

        This is a client-side hook. It executes right
        after the messages is kicked in broker.

        :param message: kicked message.
        """
        dict_message: dict[str, Any] = message.to_dict()
        await self._spawn_request(
            f"/api/tasks/{message.task_id}/queued",
            {
                "args": dict_message["args"],
                "kwargs": dict_message["kwargs"],
                "labels": dict_message["labels"],
                "queuedAt": self._now_iso(),
                "taskName": message.task_name,
                "worker": self.__ta_broker_name,
            },
        )

    async def pre_execute(self, message: SynTasksMessage) -> SynTasksMessage:
        """
        This hook is called before executing task.

        This is a worker-side hook, which means it
        executes in the worker process.

        :param message: incoming parsed syntasks message.
        :return: modified message.
        """
        dict_message: dict[str, Any] = message.to_dict()
        await self._spawn_request(
            f"/api/tasks/{message.task_id}/started",
            {
                "args": dict_message["args"],
                "kwargs": dict_message["kwargs"],
                "labels": dict_message["labels"],
                "startedAt": self._now_iso(),
                "taskName": message.task_name,
                "worker": self.__ta_broker_name,
            },
        )
        return message

    async def post_execute(
        self,
        message: SynTasksMessage,
        result: SynTasksResult[Any],
    ) -> None:
        """
        This hook executes after task is complete.

        This is a worker-side hook. It's called
        in worker process.

        :param message: incoming message.
        :param result: result of execution for current task.
        """
        dict_result: dict[str, Any] = result.to_dict()
        await self._spawn_request(
            f"/api/tasks/{message.task_id}/executed",
            {
                "finishedAt": self._now_iso(),
                "executionTime": result.execution_time,
                "error": None if result.error is None else repr(result.error),
                "returnValue": {"return_value": dict_result["return_value"]},
            },
        )
