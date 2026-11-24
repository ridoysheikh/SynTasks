"""
Cluster clock: Redis server time.

With tens of machines, local clocks drift. Every due time, lease and
expiry is compared with Redis ``TIME``. Lua scripts read it directly;
Python code uses :class:`ClusterClock`, which keeps the offset between
local and server time.
"""

import time
from collections.abc import Iterator
from contextlib import contextmanager

from syntasks.redis.client import Redis

__all__ = ["ClusterClock", "forced_now_ms", "frozen_time"]

_FORCED_NOW_MS: int | None = None


def forced_now_ms() -> int | None:
    """Forced time in milliseconds (tests only), or None."""
    return _FORCED_NOW_MS


@contextmanager
def frozen_time(now_ms: int) -> Iterator[None]:
    """
    Freeze cluster time, in Python and in Lua scripts (for tests).

    :param now_ms: time to report, in Unix milliseconds.
    :yield: nothing.
    """
    global _FORCED_NOW_MS  # noqa: PLW0603
    previous = _FORCED_NOW_MS
    _FORCED_NOW_MS = now_ms
    try:
        yield
    finally:
        _FORCED_NOW_MS = previous


class ClusterClock:
    """Local view of Redis server time."""

    resync_interval = 60.0

    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self.offset = 0.0
        self._synced_at: float | None = None

    async def sync(self) -> None:
        """Measure the offset between local time and Redis time."""
        before = time.time()
        server = await self.redis.time()
        after = time.time()
        self.offset = server - (before + after) / 2
        self._synced_at = time.monotonic()

    async def maybe_sync(self) -> None:
        """Re-measure the offset if it is older than ``resync_interval``."""
        if (
            self._synced_at is None
            or time.monotonic() - self._synced_at > self.resync_interval
        ):
            await self.sync()

    def now(self) -> float:
        """Cluster time in Unix seconds."""
        if _FORCED_NOW_MS is not None:
            return _FORCED_NOW_MS / 1000
        return time.time() + self.offset

    def now_ms(self) -> int:
        """Cluster time in Unix milliseconds."""
        if _FORCED_NOW_MS is not None:
            return _FORCED_NOW_MS
        return int((time.time() + self.offset) * 1000)
