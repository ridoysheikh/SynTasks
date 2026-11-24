"""
Leases: exclusive ownership with expiry.

A lease is a Redis key holding ``<owner>:<token>`` with a TTL. The owner
renews it before it expires; if the owner dies, the key expires and
another node can take over. ``token`` comes from a counter that only
grows (a fencing token), so state written by an owner can carry the
token and scripts can reject writes from a stale owner.
"""

import asyncio
import contextlib
import logging
from collections.abc import Callable, Coroutine
from typing import Any

from syntasks.cluster.scripts import Script
from syntasks.redis.client import Redis
from syntasks.redis.errors import RedisError

__all__ = ["Lease", "LeasedRole"]

logger = logging.getLogger("syntasks.cluster")

_ACQUIRE = Script(
    "lease_acquire",
    """
-- KEYS[1] lease key, KEYS[2] token counter
-- ARGV[2] owner, ARGV[3] ttl ms, ARGV[4] current value ('' if none)
local current = redis.call('GET', KEYS[1])
if current then
  if ARGV[4] ~= '' and current == ARGV[4] then
    redis.call('PEXPIRE', KEYS[1], ARGV[3])
    return current
  end
  return false
end
local value = ARGV[2] .. ':' .. redis.call('INCR', KEYS[2])
redis.call('SET', KEYS[1], value, 'PX', ARGV[3])
return value
""",
)

_RENEW = Script(
    "lease_renew",
    """
-- KEYS[1] lease key; ARGV[2] value, ARGV[3] ttl ms
if redis.call('GET', KEYS[1]) == ARGV[2] then
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
  return 1
end
return 0
""",
)

_RELEASE = Script(
    "lease_release",
    """
-- KEYS[1] lease key; ARGV[2] value
if redis.call('GET', KEYS[1]) == ARGV[2] then
  return redis.call('DEL', KEYS[1])
end
return 0
""",
)


class Lease:
    """One node's handle on a lease key."""

    def __init__(
        self,
        redis: Redis,
        key: str,
        owner: str,
        ttl_ms: int,
        token_key: str,
    ) -> None:
        if ttl_ms <= 0:
            raise ValueError("Lease TTL must be positive.")
        self.redis = redis
        self.key = key
        self.owner = owner
        self.ttl_ms = ttl_ms
        self.token_key = token_key
        self.value: str | None = None

    def __repr__(self) -> str:
        return f"<Lease {self.key} held={self.held}>"

    @property
    def held(self) -> bool:
        """Whether this handle believes it holds the lease."""
        return self.value is not None

    @property
    def token(self) -> int | None:
        """Fencing token of the current hold."""
        if self.value is None:
            return None
        return int(self.value.rsplit(":", 1)[1])

    async def acquire(self) -> bool:
        """
        Try to take the lease (or extend it if already held).

        :return: True if this handle holds the lease now.
        """
        reply = await _ACQUIRE(
            self.redis,
            [self.key, self.token_key],
            [self.owner, self.ttl_ms, self.value or ""],
        )
        self.value = reply.decode() if reply else None
        return self.value is not None

    async def renew(self) -> bool:
        """
        Extend the lease.

        :return: False if the lease was lost (expired or taken over).
        """
        if self.value is None:
            return False
        if await _RENEW(self.redis, [self.key], [self.value, self.ttl_ms]):
            return True
        self.value = None
        return False

    async def release(self) -> None:
        """Give the lease up, if still held."""
        if self.value is None:
            return
        value, self.value = self.value, None
        await _RELEASE(self.redis, [self.key], [value])

    async def current_owner(self) -> str | None:
        """Owner currently holding the key, whoever it is."""
        raw = await self.redis.get(self.key)
        if raw is None:
            return None
        return raw.decode().rsplit(":", 1)[0]


class LeasedRole:
    """
    Run some work only while holding a lease.

    Many nodes run :meth:`run` for the same role; one of them holds the
    lease and runs ``work``; the others wait and take over within about
    one TTL if the holder dies. If a renewal fails, ``work`` is cancelled
    at once, so two nodes never keep doing the work after a takeover.
    """

    def __init__(
        self,
        name: str,
        lease: Lease,
        work: Callable[[Lease], Coroutine[Any, Any, None]],
    ) -> None:
        self.name = name
        self.lease = lease
        self.work = work
        self.interval = lease.ttl_ms / 3000

    @property
    def active(self) -> bool:
        """Whether this node currently runs the role."""
        return self.lease.held

    async def run(self) -> None:
        """Compete for the lease forever (until cancelled)."""
        try:
            while True:
                try:
                    acquired = await self.lease.acquire()
                except (RedisError, OSError) as exc:
                    logger.warning("Role %s: cannot reach Redis (%s).", self.name, exc)
                    acquired = False
                if acquired:
                    logger.info("Role %s: this node took over.", self.name)
                    await self._hold()
                    logger.info("Role %s: this node stopped.", self.name)
                await asyncio.sleep(self.interval)
        finally:
            with contextlib.suppress(RedisError, OSError):
                await asyncio.shield(self.lease.release())

    async def _hold(self) -> None:
        worker = asyncio.create_task(self.work(self.lease))
        try:
            while not worker.done():
                done, _ = await asyncio.wait({worker}, timeout=self.interval)
                if done:
                    break
                try:
                    still_held = await self.lease.renew()
                except (RedisError, OSError) as exc:
                    logger.warning(
                        "Role %s: renewal failed (%s); stopping.",
                        self.name,
                        exc,
                    )
                    still_held = False
                if not still_held:
                    break
        finally:
            if not worker.done():
                worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                try:
                    await worker
                except Exception:
                    logger.exception("Role %s failed.", self.name)
            with contextlib.suppress(RedisError, OSError):
                await self.lease.release()
