"""Cluster-wide rate limiting with GCRA (generic cell rate algorithm)."""

from syntasks.cluster.keys import Keys
from syntasks.cluster.scripts import Script
from syntasks.redis.client import Redis
from syntasks.units import Rate

__all__ = ["GCRA", "RateLimiter"]

# Returns 0 if allowed, else milliseconds until the next event is allowed.
GCRA = Script(
    "rate_gcra",
    """
-- KEYS[1] tat key; ARGV[2] emission ms, ARGV[3] burst
local now = now_ms()
local emission = tonumber(ARGV[2])
local tolerance = emission * tonumber(ARGV[3])
local tat = tonumber(redis.call('GET', KEYS[1]) or now)
if tat < now then tat = now end
local new_tat = tat + emission
local allow_at = new_tat - tolerance
if allow_at > now then
  return math.ceil(allow_at - now)
end
redis.call('SET', KEYS[1], tostring(new_tat), 'PX', math.ceil(new_tat - now) + 1000)
return 0
""",
)


class RateLimiter:
    """A named cluster-wide rate limit."""

    def __init__(self, redis: Redis, keys: Keys, name: str, rate: Rate) -> None:
        self.redis = redis
        self.name = name
        self.rate = rate
        self.key = keys("lim", "rate", name)

    def __repr__(self) -> str:
        return f"<RateLimiter {self.name} {self.rate}>"

    async def acquire(self) -> int:
        """
        Try to take one event.

        :return: 0 if allowed, otherwise milliseconds to wait.
        """
        return int(
            await GCRA(
                self.redis,
                [self.key],
                [repr(self.rate.emission_ms), self.rate.burst],
            ),
        )
