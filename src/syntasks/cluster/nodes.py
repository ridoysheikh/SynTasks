"""
Node registry: which worker processes are alive.

Every worker process registers itself and sends a heartbeat. A node that
misses heartbeats for ``dead_after`` is dead: the janitor removes it and
frees what it held (semaphore slots, channel handlers).
"""

import json
import os
import secrets
import socket
from dataclasses import asdict, dataclass, field
from typing import Any

from syntasks.cluster.keys import Keys
from syntasks.cluster.scripts import Script
from syntasks.redis.client import Redis

__all__ = ["NodeInfo", "NodeRegistry", "make_node_id"]

_BEAT = Script(
    "node_beat",
    """
-- KEYS[1] info hash, KEYS[2] alive zset; ARGV[2] node id, ARGV[3] info json
redis.call('HSET', KEYS[1], ARGV[2], ARGV[3])
redis.call('ZADD', KEYS[2], now_ms(), ARGV[2])
return 1
""",
)

_DEAD = Script(
    "node_dead",
    """
-- KEYS[1] alive zset; ARGV[2] dead after ms
return redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now_ms() - tonumber(ARGV[2]))
""",
)


def make_node_id() -> str:
    """Unique id of this process: ``<host>-<pid>-<random>``."""
    host = socket.gethostname().split(".")[0] or "host"
    return f"{host}-{os.getpid()}-{secrets.token_hex(2)}"


@dataclass(kw_only=True)
class NodeInfo:
    """What a node says about itself."""

    node_id: str
    host: str = field(default_factory=socket.gethostname)
    pid: int = field(default_factory=os.getpid)
    group: str = ""
    version: str = ""
    revision: int | None = None
    started: float = 0.0
    queues: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    capacity: int = 0
    running: int = 0
    running_tasks: dict[str, str] = field(default_factory=dict)
    channels: int = 0
    last_seen: float = 0.0
    alive: bool = True

    def to_json(self) -> str:
        """Encode for Redis."""
        data = asdict(self)
        data.pop("last_seen")
        data.pop("alive")
        return json.dumps(data, separators=(",", ":"))

    @classmethod
    def from_json(cls, raw: bytes | str, last_seen: float, alive: bool) -> "NodeInfo":
        """
        Decode from Redis.

        :param raw: stored JSON.
        :param last_seen: last heartbeat (Unix seconds).
        :param alive: whether the node is considered alive.
        :return: node info.
        """
        data: dict[str, Any] = json.loads(raw)
        known = set(cls.__dataclass_fields__)
        info = cls(**{k: v for k, v in data.items() if k in known})
        info.last_seen = last_seen
        info.alive = alive
        return info


class NodeRegistry:
    """Registration and heartbeats of nodes."""

    def __init__(self, redis: Redis, keys: Keys, dead_after_ms: int = 30_000) -> None:
        self.redis = redis
        self.keys = keys
        self.dead_after_ms = dead_after_ms
        self.info_key = keys("node", "info")
        self.alive_key = keys("node", "alive")

    async def beat(self, info: NodeInfo) -> None:
        """
        Register a node or refresh its heartbeat.

        :param info: current node info.
        """
        await _BEAT(
            self.redis,
            [self.info_key, self.alive_key],
            [info.node_id, info.to_json()],
        )

    async def remove(self, node_id: str) -> None:
        """
        Forget a node.

        :param node_id: node id.
        """
        pipe = self.redis.pipeline(transaction=True)
        pipe.add("HDEL", self.info_key, node_id)
        pipe.add("ZREM", self.alive_key, node_id)
        await pipe.execute()

    async def dead(self) -> list[str]:
        """Ids of nodes that stopped sending heartbeats."""
        reply = await _DEAD(self.redis, [self.alive_key], [self.dead_after_ms])
        return [raw.decode() for raw in reply]

    async def list(self) -> list[NodeInfo]:
        """All registered nodes, alive or not yet cleaned up."""
        infos = await self.redis.hgetall(self.info_key)
        seen = dict(await self.redis.zrange_withscores(self.alive_key, 0, -1))
        now_ms = (await self.redis.time()) * 1000
        nodes = []
        for node_id, raw in infos.items():
            last_ms = seen.get(node_id, 0.0)
            nodes.append(
                NodeInfo.from_json(
                    raw,
                    last_seen=last_ms / 1000,
                    alive=now_ms - last_ms < self.dead_after_ms,
                ),
            )
        nodes.sort(key=lambda node: node.node_id)
        return nodes
