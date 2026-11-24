"""
Control bus: low-volume cluster events over Redis pub/sub.

Events are hints (wake the dispatcher, cancel a task, drain a node).
Pub/sub can lose messages during reconnects, so every consumer also
re-checks state periodically; nothing depends on an event arriving.
"""

import asyncio
import json
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any

from syntasks.cluster.keys import Keys
from syntasks.redis.client import Redis
from syntasks.redis.errors import RedisError
from syntasks.utils import maybe_awaitable

__all__ = ["ControlBus", "ControlHandler"]

logger = logging.getLogger("syntasks.cluster")

ControlHandler = Callable[[dict[str, Any]], Awaitable[None] | None]


class ControlBus:
    """Publish and receive control events of one app."""

    def __init__(self, redis: Redis, keys: Keys) -> None:
        self.redis = redis
        self.channel = keys("ctl")
        self._handlers: defaultdict[str, list[ControlHandler]] = defaultdict(list)
        self.subscribed = asyncio.Event()

    async def publish(self, kind: str, **data: Any) -> None:
        """
        Send an event to every node.

        :param kind: event kind, e.g. ``"schedule.changed"``.
        :param data: JSON-compatible event data.
        """
        await self.redis.publish(
            self.channel,
            json.dumps({"kind": kind, **data}, separators=(",", ":")),
        )

    def on(self, kind: str, handler: ControlHandler) -> None:
        """
        Register a handler for an event kind.

        :param kind: event kind.
        :param handler: sync or async callable taking the event dict.
        """
        self._handlers[kind].append(handler)

    async def _dispatch(self, raw: bytes) -> None:
        try:
            event = json.loads(raw)
            kind = event["kind"]
        except (ValueError, KeyError, TypeError):
            logger.warning("Ignoring malformed control event: %r", raw[:200])
            return
        for handler in self._handlers.get(kind, ()):
            try:
                await maybe_awaitable(handler(event))
            except Exception:
                logger.exception("Control handler for %s failed.", kind)

    async def run(self) -> None:
        """Receive events forever (until cancelled), reconnecting as needed."""
        attempt = 0
        while True:
            pubsub = self.redis.pubsub()
            try:
                await pubsub.subscribe(self.channel)
                self.subscribed.set()
                attempt = 0
                async for message in pubsub.listen():
                    await self._dispatch(message.data)
            except (RedisError, OSError) as exc:
                self.subscribed.clear()
                delay = min(0.1 * 2**attempt, 5.0)
                attempt += 1
                logger.warning("Control bus disconnected (%s); retrying.", exc)
                await asyncio.sleep(delay)
            finally:
                await pubsub.close()
