"""
Janitor: periodic cleanup, run by one node at a time (leased role).

Removes dead nodes and frees what they held, then runs cleanup hooks
registered by the subsystems (expired schedules, idle channels, ...).
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable

from syntasks.cluster.keys import Keys
from syntasks.cluster.lease import Lease
from syntasks.cluster.nodes import NodeRegistry
from syntasks.cluster.semaphore import release_holders_with_prefix
from syntasks.redis.client import Redis

__all__ = ["Janitor"]

logger = logging.getLogger("syntasks.cluster")

CleanupHook = Callable[[], Awaitable[object]]
DeadNodeHook = Callable[[str], Awaitable[None]]


class Janitor:
    """Cleanup work shared by the whole cluster."""

    def __init__(
        self,
        redis: Redis,
        keys: Keys,
        nodes: NodeRegistry,
        interval: float = 5.0,
    ) -> None:
        self.redis = redis
        self.keys = keys
        self.nodes = nodes
        self.interval = interval
        self.cleanup_hooks: list[CleanupHook] = []
        self.dead_node_hooks: list[DeadNodeHook] = []

    def add_cleanup(self, hook: CleanupHook) -> None:
        """
        Run ``hook`` on every janitor round.

        :param hook: async callable.
        """
        self.cleanup_hooks.append(hook)

    def add_dead_node_hook(self, hook: DeadNodeHook) -> None:
        """
        Run ``hook(node_id)`` for every node found dead.

        :param hook: async callable.
        """
        self.dead_node_hooks.append(hook)

    async def run_once(self) -> list[str]:
        """
        One cleanup round.

        :return: ids of nodes removed as dead.
        """
        dead = await self.nodes.dead()
        for node_id in dead:
            released = await release_holders_with_prefix(
                self.redis,
                self.keys,
                f"{node_id}|",
            )
            for hook in self.dead_node_hooks:
                try:
                    await hook(node_id)
                except Exception:
                    logger.exception("Dead-node hook failed for %s.", node_id)
            await self.nodes.remove(node_id)
            logger.warning(
                "Node %s stopped sending heartbeats; removed it and freed "
                "%d slot(s).",
                node_id,
                released,
            )
        for cleanup in self.cleanup_hooks:
            try:
                await cleanup()
            except Exception:
                logger.exception("Janitor hook %r failed.", cleanup)
        return dead

    async def work(self, _lease: Lease) -> None:
        """Cleanup loop, run while holding the janitor lease."""
        while True:
            await self.run_once()
            await asyncio.sleep(self.interval)
