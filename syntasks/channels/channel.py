"""Channel messages and the handle a channel handler works with."""

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from syntasks.cluster.lease import Lease
from syntasks.cluster.scripts import Script
from syntasks.exceptions import SynTasksError
from syntasks.limits import dumps_bounded
from syntasks.redis.errors import RedisError

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.channels.manager import ChannelManager

__all__ = [
    "Channel",
    "ChannelFencedError",
    "ChannelFullError",
    "ChannelMessage",
    "ChannelOverflowError",
]

logger = logging.getLogger("syntasks.channels")

HANDLER_GROUP = "handler"


class ChannelFullError(SynTasksError):
    """The channel's inbox has too many unprocessed messages."""

    __template__ = "Channel {name!r} inbox is full."
    name: str


class ChannelFencedError(SynTasksError):
    """This handler lost ownership of the channel; another node runs it now."""

    __template__ = "Channel {name!r} is now handled by another node."
    name: str


class ChannelOverflowError(SynTasksError):
    """A reader fell too far behind; reconnect with ``last_id``."""

    __template__ = "Reader of channel {name!r} fell behind; resume from {last_id}."
    name: str
    last_id: str


@dataclass(frozen=True, kw_only=True)
class ChannelMessage:
    """A message on a channel."""

    id: str
    data: Any = None
    sender: str | None = None
    kind: str = "message"
    time_ms: int = 0

    @classmethod
    def from_entry(
        cls,
        entry_id: bytes,
        fields: dict[bytes, bytes],
    ) -> "ChannelMessage":
        """
        Build from a stream entry.

        :param entry_id: stream id.
        :param fields: entry fields.
        :return: message.
        """
        raw = fields.get(b"data")
        try:
            data = json.loads(raw) if raw is not None else None
        except ValueError:
            data = raw.decode(errors="replace") if raw else None
        sender = fields.get(b"sender")
        text = entry_id.decode()
        return cls(
            id=text,
            data=data,
            sender=sender.decode() if sender else None,
            kind=(fields.get(b"kind") or b"message").decode(),
            time_ms=int(text.split("-", 1)[0]),
        )

    def to_dict(self) -> dict[str, Any]:
        """Plain data (e.g. to send over a websocket)."""
        return {
            "id": self.id,
            "data": self.data,
            "sender": self.sender,
            "kind": self.kind,
            "time_ms": self.time_ms,
        }


# Publish to a channel's broadcast stream.
# KEYS: 1 out, 2 active zset, 3 owner lease (fence)
# ARGV: 2 data, 3 sender, 4 kind, 5 max length, 6 min id|'', 7 fence value|'',
#       8 name, 9 expiry ms
PUBLISH = Script(
    "channel_publish",
    """
if ARGV[7] ~= '' and redis.call('GET', KEYS[3]) ~= ARGV[7] then return {'fenced'} end
local id = redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[5], '*',
  'data', ARGV[2], 'sender', ARGV[3], 'kind', ARGV[4])
if ARGV[6] ~= '' then redis.call('XTRIM', KEYS[1], 'MINID', '~', ARGV[6]) end
redis.call('PEXPIRE', KEYS[1], ARGV[9])
redis.call('ZADD', KEYS[2], now_ms(), ARGV[8])
return {'ok', id}
""",
)


class Channel:
    """
    The handle a channel handler gets.

    Example::

        @app.channel("room:{room_id}", idle_timeout="10m")
        async def room(ch: Channel, room_id: str) -> None:
            async for msg in ch.inbox():
                ch.state["count"] = ch.state.get("count", 0) + 1
                await ch.publish({"from": msg.sender, "text": msg.data["text"]})
                await ch.save_state()
    """

    def __init__(
        self,
        manager: "ChannelManager",
        name: str,
        params: dict[str, str],
        lease: Lease,
        *,
        idle_timeout: float | None,
    ) -> None:
        self.manager = manager
        self.name = name
        self.params = params
        self.lease = lease
        self.idle_timeout = idle_timeout
        self.state: dict[str, Any] = {}
        self.consumer = manager.app.cluster.node_id
        self._last_activity = time.monotonic()
        self.processed = 0

    def __repr__(self) -> str:
        return f"<Channel {self.name}>"

    def _fence(self) -> str:
        if self.lease.value is None:
            raise ChannelFencedError(name=self.name)
        return self.lease.value

    async def load_state(self) -> dict[str, Any]:
        """Load the state the previous owner saved (called before the handler)."""
        raw = await self.manager.app.redis.hget(
            self.manager.key(self.name, "state"),
            "json",
        )
        self.state = json.loads(raw) if raw else {}
        return self.state

    async def save_state(self) -> None:
        """
        Save :attr:`state` so a takeover continues from it.

        :raises ChannelFencedError: if this node no longer owns the channel.
        """
        manager = self.manager
        encoded = dumps_bounded(
            self.state,
            max_bytes=manager.app.config.channels.max_message * 16,
            what=f"State of channel {self.name}",
        )
        ok = await _SAVE_STATE(
            manager.app.redis,
            [manager.key(self.name, "state"), manager.key(self.name, "owner")],
            [
                encoded,
                self._fence(),
                int(manager.app.config.channels.idle_expiry * 1000),
            ],
        )
        if not ok:
            raise ChannelFencedError(name=self.name)

    async def publish(self, data: Any, *, kind: str = "message") -> str:
        """
        Send a message to every reader of this channel.

        :param data: JSON data.
        :param kind: message kind.
        :raises ChannelFencedError: if this node no longer owns the channel.
        :return: message id.
        """
        return await self.manager.publish(
            self.name,
            data,
            kind=kind,
            sender=f"handler:{self.consumer}",
            fence=self._fence(),
        )

    async def readers(self) -> int:
        """Number of readers connected now."""
        return await self.manager.reader_count(self.name)

    async def inbox(self, *, batch: int = 10) -> AsyncIterator[ChannelMessage]:
        """
        Messages clients sent to this channel, oldest first.

        A message is acknowledged when the loop asks for the next one, so
        if the handler crashes in the middle of a message, the next owner
        gets it again. Iteration ends after ``idle_timeout`` without
        messages and without readers.

        :param batch: messages fetched per read.
        :yield: messages.
        """
        manager = self.manager
        redis = manager.app.redis
        key = manager.key(self.name, "in")
        await manager.ensure_inbox(self.name)
        pending: list[tuple[bytes, dict[bytes, bytes]]] = []
        # Take over everything the previous owner left unacknowledged.
        start: Any = "0-0"
        while True:
            start, claimed = await redis.xautoclaim(
                key,
                HANDLER_GROUP,
                self.consumer,
                0,
                start,
                count=100,
            )
            pending.extend(claimed)
            if start in (b"0-0", "0-0") or not claimed:
                break
        while True:
            if not pending:
                if self.lease.value is None:
                    return
                block = 1000
                batches = await redis.xreadgroup(
                    HANDLER_GROUP,
                    self.consumer,
                    {key: ">"},
                    count=batch,
                    block=block,
                )
                for _, entries in batches:
                    pending.extend(entries)
                if not pending:
                    if (
                        self.idle_timeout is not None
                        and (time.monotonic() - self._last_activity > self.idle_timeout)
                        and not await self.readers()
                    ):
                        return
                    continue
            entry_id, fields = pending.pop(0)
            self._last_activity = time.monotonic()
            yield ChannelMessage.from_entry(entry_id, fields)
            self.processed += 1
            pipe = redis.pipeline()
            pipe.add("XACK", key, HANDLER_GROUP, entry_id)
            pipe.add("XDEL", key, entry_id)
            with contextlib.suppress(RedisError, OSError):
                await pipe.execute()

    async def sleep(self, seconds: float) -> None:
        """
        Sleep, returning early if the channel is lost.

        :param seconds: seconds.
        :raises ChannelFencedError: if ownership was lost.
        """
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            if self.lease.value is None:
                raise ChannelFencedError(name=self.name)
            await asyncio.sleep(min(0.5, end - time.monotonic()))


_SAVE_STATE = Script(
    "channel_save_state",
    """
-- KEYS: 1 state hash, 2 owner; ARGV: 2 json, 3 fence, 4 expiry ms
if redis.call('GET', KEYS[2]) ~= ARGV[3] then return 0 end
redis.call('HSET', KEYS[1], 'json', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return 1
""",
)


@dataclass
class _HandlerSpec:
    pattern: str
    regex: Any
    func: Any
    idle_timeout: float | None
    max_lifetime: float | None
    max_restarts: int = 5
    restart_window: float = 600.0
    params: list[str] = field(default_factory=list)
