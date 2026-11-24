"""
Cluster foundation: the Redis primitives every distributed feature uses.

* :class:`Keys` - key naming under ``syntasks:{<app>}:``;
* :class:`Script` - atomic Lua scripts on Redis server time;
* :class:`ClusterClock` - Redis time on the Python side;
* :class:`Lease` / :class:`LeasedRole` - single owner with takeover;
* :class:`DistributedSemaphore` - "at most N at once" cluster-wide;
* :class:`RateLimiter` - "N per period" cluster-wide;
* :class:`NodeRegistry` - live worker processes;
* :class:`ControlBus` - cluster events;
* :class:`Janitor` - cleanup of dead nodes and expired state.
"""

from syntasks.cluster.clock import ClusterClock, frozen_time
from syntasks.cluster.control import ControlBus
from syntasks.cluster.janitor import Janitor
from syntasks.cluster.keys import Keys, validate_name
from syntasks.cluster.lease import Lease, LeasedRole
from syntasks.cluster.nodes import NodeInfo, NodeRegistry, make_node_id
from syntasks.cluster.ratelimit import RateLimiter
from syntasks.cluster.scripts import Script
from syntasks.cluster.semaphore import DistributedSemaphore
from syntasks.redis.client import Redis
from syntasks.units import Rate

__all__ = [
    "Cluster",
    "ClusterClock",
    "ControlBus",
    "DistributedSemaphore",
    "Janitor",
    "Keys",
    "Lease",
    "LeasedRole",
    "NodeInfo",
    "NodeRegistry",
    "RateLimiter",
    "Script",
    "frozen_time",
    "make_node_id",
    "validate_name",
]


class Cluster:
    """
    The shared state of one app in Redis, seen from one process.

    Created by the app; both senders and workers use it. Only worker
    processes register in the node registry.
    """

    def __init__(
        self,
        redis: Redis,
        app_name: str,
        *,
        node_id: str | None = None,
        dead_after_ms: int = 30_000,
    ) -> None:
        self.redis = redis
        self.keys = Keys(app_name)
        self.node_id = node_id or make_node_id()
        self.clock = ClusterClock(redis)
        self.control = ControlBus(redis, self.keys)
        self.nodes = NodeRegistry(redis, self.keys, dead_after_ms)

    def holder(self, suffix: str) -> str:
        """
        Holder id for semaphores, tied to this node.

        :param suffix: unique part, e.g. a task id.
        :return: ``<node_id>|<suffix>``.
        """
        return f"{self.node_id}|{suffix}"

    def lease(self, name: str, ttl_ms: int) -> Lease:
        """
        Lease handle for this node.

        :param name: lease name.
        :param ttl_ms: lease time to live.
        :return: lease.
        """
        return Lease(
            self.redis,
            self.keys("lease", name),
            self.node_id,
            ttl_ms,
            self.keys("lease", "token"),
        )

    def semaphore(
        self,
        name: str,
        limit: int,
        ttl_ms: int = 30_000,
    ) -> DistributedSemaphore:
        """
        Cluster-wide semaphore.

        :param name: semaphore name.
        :param limit: slots.
        :param ttl_ms: holder lease time.
        :return: semaphore.
        """
        return DistributedSemaphore(self.redis, self.keys, name, limit, ttl_ms)

    def rate_limiter(self, name: str, rate: Rate) -> RateLimiter:
        """
        Cluster-wide rate limiter.

        :param name: limiter name.
        :param rate: allowed rate.
        :return: rate limiter.
        """
        return RateLimiter(self.redis, self.keys, name, rate)
