"""
Distributed semaphore: "at most N at once" across all machines.

Holders live in a sorted set scored by lease expiry, so slots of a
crashed holder free themselves; the janitor frees slots of dead nodes
sooner. Releasing a slot pushes to a wake list, so waiters block on
``BLPOP`` instead of polling.
"""

from syntasks.cluster.keys import Keys
from syntasks.cluster.scripts import Script
from syntasks.redis.client import Redis

__all__ = ["DistributedSemaphore", "release_holders_with_prefix"]

WAKE_KEEP = 16

ACQUIRE = Script(
    "sem_acquire",
    """
-- KEYS[1] holders zset, KEYS[2] index set
-- ARGV[2] holder, ARGV[3] limit, ARGV[4] ttl ms, ARGV[5] name
local now = now_ms()
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if redis.call('ZSCORE', KEYS[1], ARGV[2]) then
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[4]), ARGV[2])
  return 1
end
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[3]) then
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[4]), ARGV[2])
  redis.call('SADD', KEYS[2], ARGV[5])
  return 1
end
return 0
""",
)

RENEW = Script(
    "sem_renew",
    """
-- KEYS[1] holders zset; ARGV[2] holder, ARGV[3] ttl ms
if redis.call('ZSCORE', KEYS[1], ARGV[2]) then
  redis.call('ZADD', KEYS[1], now_ms() + tonumber(ARGV[3]), ARGV[2])
  return 1
end
return 0
""",
)

RELEASE = Script(
    "sem_release",
    """
-- KEYS[1] holders zset, KEYS[2] wake list; ARGV[2] holder
local removed = redis.call('ZREM', KEYS[1], ARGV[2])
if removed == 1 then
  redis.call('LPUSH', KEYS[2], '1')
  redis.call('LTRIM', KEYS[2], 0, %d)
  redis.call('PEXPIRE', KEYS[2], 60000)
end
return removed
""" % (WAKE_KEEP - 1),
)

_RELEASE_PREFIX = Script(
    "sem_release_prefix",
    """
-- KEYS[1] holders zset, KEYS[2] wake list; ARGV[2] holder prefix
local released = 0
local cursor = '0'
repeat
  local reply = redis.call('ZSCAN', KEYS[1], cursor, 'MATCH', ARGV[2] .. '*', 'COUNT', 200)
  cursor = reply[1]
  local items = reply[2]
  for i = 1, #items, 2 do
    released = released + redis.call('ZREM', KEYS[1], items[i])
  end
until cursor == '0'
if released > 0 then
  redis.call('LPUSH', KEYS[2], '1')
  redis.call('LTRIM', KEYS[2], 0, %d)
  redis.call('PEXPIRE', KEYS[2], 60000)
end
return released
""" % (WAKE_KEEP - 1),  # noqa: E501
)


class DistributedSemaphore:
    """A named cluster-wide semaphore."""

    def __init__(
        self,
        redis: Redis,
        keys: Keys,
        name: str,
        limit: int,
        ttl_ms: int = 30_000,
    ) -> None:
        if limit < 1:
            raise ValueError("Semaphore limit must be at least 1.")
        self.redis = redis
        self.keys = keys
        self.name = name
        self.limit = limit
        self.ttl_ms = ttl_ms
        self.key = keys("lim", "sem", name)
        self.wake_key = keys("lim", "wake", name)
        self.index_key = keys("lim", "sems")

    def __repr__(self) -> str:
        return f"<DistributedSemaphore {self.name} limit={self.limit}>"

    async def acquire(self, holder: str) -> bool:
        """
        Take a slot without waiting.

        :param holder: unique holder id (prefix it with the node id).
        :return: True if a slot was taken (or already held by ``holder``).
        """
        return bool(
            await ACQUIRE(
                self.redis,
                [self.key, self.index_key],
                [holder, self.limit, self.ttl_ms, self.name],
            ),
        )

    async def renew(self, holder: str) -> bool:
        """
        Extend a held slot.

        :param holder: holder id.
        :return: False if the slot was lost.
        """
        return bool(await RENEW(self.redis, [self.key], [holder, self.ttl_ms]))

    async def release(self, holder: str) -> bool:
        """
        Free a slot and wake one waiter.

        :param holder: holder id.
        :return: True if the holder had a slot.
        """
        return bool(await RELEASE(self.redis, [self.key, self.wake_key], [holder]))

    async def wait(self, timeout: float) -> bool:
        """
        Block until a slot is released somewhere (or timeout).

        :param timeout: seconds to wait.
        :return: True if woken by a release.
        """
        return await self.redis.blpop(self.wake_key, timeout) is not None

    async def holders(self) -> list[str]:
        """Current (unexpired and expired-but-not-yet-cleaned) holders."""
        return [
            m.decode() for m, _ in await self.redis.zrange_withscores(self.key, 0, -1)
        ]

    async def count(self) -> int:
        """Number of holders."""
        return await self.redis.zcard(self.key)


async def release_holders_with_prefix(redis: Redis, keys: Keys, prefix: str) -> int:
    """
    Free every slot whose holder id starts with ``prefix`` (a dead node).

    :param redis: client.
    :param keys: app keys.
    :param prefix: holder prefix, e.g. ``"<node_id>|"``.
    :return: number of slots freed.
    """
    released = 0
    for raw in await redis.smembers(keys("lim", "sems")):
        name = raw.decode()
        released += await _RELEASE_PREFIX(
            redis,
            [keys("lim", "sem", name), keys("lim", "wake", name)],
            [prefix],
        )
    return released
