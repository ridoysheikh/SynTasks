"""
Lua scripts run atomically inside Redis.

Every script gets a small prelude with ``now_ms()``, which returns Redis
server time in milliseconds. All cluster-wide decisions use that clock,
so machines with drifting clocks still agree.

``ARGV[1]`` is reserved for a forced "now" used by tests (see
:func:`syntasks.cluster.clock.frozen_time`); script arguments start at
``ARGV[2]``.
"""

import hashlib
from collections.abc import Sequence
from typing import Any

from syntasks.cluster import clock
from syntasks.redis.client import Redis
from syntasks.redis.errors import NoScriptError
from syntasks.redis.protocol import EncodableT

__all__ = ["Script"]

_PRELUDE = """
local function now_ms()
  local forced = ARGV[1]
  if forced ~= nil and forced ~= '' then return tonumber(forced) end
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
"""


class Script:
    """
    A Lua script called by SHA, loaded on first use.

    ``EVALSHA`` is tried first; on ``NOSCRIPT`` (first call, or after a
    Redis restart or ``SCRIPT FLUSH``) the script is sent with ``EVAL``,
    which also caches it for the next call.
    """

    def __init__(self, name: str, body: str) -> None:
        self.name = name
        self.source = _PRELUDE + body
        self.sha = hashlib.sha1(self.source.encode()).hexdigest()  # noqa: S324

    def __repr__(self) -> str:
        return f"Script({self.name!r})"

    async def __call__(
        self,
        redis: Redis,
        keys: Sequence[EncodableT] = (),
        args: Sequence[EncodableT] = (),
    ) -> Any:
        """
        Run the script.

        :param redis: client.
        :param keys: KEYS.
        :param args: ARGV from ``ARGV[2]`` on.
        :return: the script's reply.
        """
        forced = clock.forced_now_ms()
        argv: list[EncodableT] = ["" if forced is None else forced, *args]
        try:
            return await redis.evalsha(self.sha, keys, argv)
        except NoScriptError:
            return await redis.eval(self.source, keys, argv)
