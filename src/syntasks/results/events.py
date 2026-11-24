"""
Task events: a task's lifecycle and streamed output, pushed to clients.

With ``events=True`` a task publishes to the channel ``task:<id>``:
``queued``, ``started``, ``progress``, ``emit`` (from :class:`Emit`),
``retrying`` and one of ``succeeded`` / ``failed`` / ``cancelled``. Clients
follow it with ``task.events()``; a late subscriber replays what the
channel still keeps. Tasks without ``events`` publish nothing.

Example::

    @app.task(events=True)
    async def generate(prompt: str, emit: Annotated[Emit, Depends()]) -> str:
        async for token in model.stream(prompt):
            await emit(token)
        return "done"

    task = await generate.kiq("hello")
    async for event in task.events():
        print(event.kind, event.data)
"""

from __future__ import annotations

import contextlib
import logging
from typing import TYPE_CHECKING, Annotated, Any

from syntasks.context import Context
from syntasks.dependencies import Depends

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks

__all__ = ["TERMINAL", "Emit", "TaskEvents"]

logger = logging.getLogger("syntasks.events")

# Event kinds after which nothing more is published.
TERMINAL = frozenset({"succeeded", "failed", "cancelled"})


def channel_name(task_id: str) -> str:
    """The channel a task's events go to."""
    return f"task:{task_id}"


class TaskEvents:
    """Publishes task events (``app.events``)."""

    def __init__(self, app: SynTasks) -> None:
        self.app = app
        # Ids of runs in this process that publish events (for progress).
        self.active: set[str] = set()

    async def publish(self, task_id: str, kind: str, data: Any = None) -> None:
        """
        Publish one event of a task.

        :param task_id: task id.
        :param kind: event kind.
        :param data: JSON data.
        """
        try:
            await self.app.channels.publish(channel_name(task_id), data, kind=kind)
        except Exception:  # an event must never break a run
            logger.warning(
                "Could not publish %s event of %s.", kind, task_id, exc_info=True
            )


class Emit:
    """
    Stream partial output of a run to its ``task.events()`` readers.

    Needs ``events=True`` on the task; ``await emit(data)`` publishes an
    ``emit`` event.
    """

    def __init__(self, context: Annotated[Context, Depends()]) -> None:
        self._task_id = context.message.task_id
        self._app: Any = context.broker

    async def __call__(self, data: Any) -> None:
        """
        Publish ``data`` (JSON) as an ``emit`` event.

        :param data: the chunk.
        """
        events = getattr(self._app, "events", None)
        if events is None:
            return
        with contextlib.suppress(Exception):
            await events.publish(self._task_id, "emit", data)
