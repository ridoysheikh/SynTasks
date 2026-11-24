"""Channel operations, reader hubs and handler hosting (``app.channels``)."""

from __future__ import annotations

import asyncio
import builtins
import contextlib
import json
import logging
import re
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from syntasks.channels.channel import (
    HANDLER_GROUP,
    PUBLISH,
    Channel,
    ChannelDenied,
    ChannelFencedError,
    ChannelFullError,
    ChannelMessage,
    ChannelOverflowError,
    ConnectRequest,
    Presence,
    _HandlerSpec,
)
from syntasks.cluster.lease import Lease
from syntasks.cluster.scripts import Script
from syntasks.limits import dumps_bounded
from syntasks.redis.errors import RedisError, ResponseError
from syntasks.units import parse_duration

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks
    from syntasks.cluster.janitor import Janitor

__all__ = ["ChannelConnection", "ChannelInfo", "ChannelManager"]

logger = logging.getLogger("syntasks.channels")

_HOSTS_GROUP = "hosts"
_PATTERN_TTL = 60.0
_MAX_HOPS = 10
_SLOT_RETRY = 0.5  # seconds between tries for a channel waiting for max_tasks

# Send to a channel's inbox and request activation of its handler.
# KEYS: 1 in, 2 owner, 3 activating, 4 activate stream, 5 active, 6 meta
# ARGV: 2 data, 3 sender, 4 kind, 5 inbox max, 6 name, 7 group, 8 expiry ms
_SEND_IN = Script(
    "channel_send_in",
    """
if redis.call('XLEN', KEYS[1]) >= tonumber(ARGV[5]) then return {'full'} end
if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('XGROUP', 'CREATE', KEYS[1], ARGV[7], '0', 'MKSTREAM')
end
local id = redis.call('XADD', KEYS[1], '*', 'data', ARGV[2], 'sender', ARGV[3], 'kind', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[8])
redis.call('ZADD', KEYS[5], now_ms(), ARGV[6])
if redis.call('EXISTS', KEYS[2]) == 0 and redis.call('HGET', KEYS[6], 'failed') ~= '1'
   and redis.call('SET', KEYS[3], '1', 'NX', 'PX', 2000) then
  redis.call('XADD', KEYS[4], 'MAXLEN', '~', 10000, '*', 'name', ARGV[6], 'hops', '0')
end
return {'ok', id}
""",  # noqa: E501
)


def _id_tuple(entry_id: str) -> tuple[int, int]:
    ms, _, seq = entry_id.partition("-")
    return int(ms or 0), int(seq or 0)


def _compile(pattern: str) -> tuple[Any, list[str]]:
    params: builtins.list[str] = []
    regex = ""
    for part in re.split(r"(\{[A-Za-z_][A-Za-z0-9_]*\})", pattern):
        if part.startswith("{") and part.endswith("}"):
            params.append(part[1:-1])
            regex += f"(?P<{part[1:-1]}>[^:]+)"
        else:
            regex += re.escape(part)
    return re.compile(f"^{regex}$"), params


@dataclass(kw_only=True)
class ChannelInfo:
    """State of a channel."""

    name: str
    length: int
    inbox: int
    readers: int
    owner: str | None
    last_active_ms: int | None
    failed: bool = False
    error: str | None = None


class _Reader:
    def __init__(self, name: str, size: int) -> None:
        self.name = name
        self.queue: asyncio.Queue[ChannelMessage | BaseException] = asyncio.Queue(size)
        self.last_id = "0-0"
        self.catching_up = False
        self.stash: builtins.list[ChannelMessage] = []
        self.overflowed = False

    def deliver(self, message: ChannelMessage) -> None:
        if self.overflowed:
            return
        if self.catching_up:
            self.stash.append(message)
            return
        if _id_tuple(message.id) <= _id_tuple(self.last_id):
            return
        try:
            self.queue.put_nowait(message)
        except asyncio.QueueFull:
            self.overflowed = True
            with contextlib.suppress(asyncio.QueueFull):
                self.queue.get_nowait()
            self.queue.put_nowait(
                ChannelOverflowError(name=self.name, last_id=self.last_id),
            )
            return
        self.last_id = message.id

    def finish_catch_up(self) -> None:
        self.catching_up = False
        stash, self.stash = self.stash, []
        for message in stash:
            self.deliver(message)


class _Hub:
    """One blocking XREAD over many channels, fanned out to local readers."""

    def __init__(self, manager: ChannelManager, index: int) -> None:
        self.manager = manager
        self.streams: dict[str, tuple[str, set[_Reader]]] = {}
        self.wake_key = manager.app.keys(
            "ch",
            "hub",
            manager.app.cluster.node_id,
            str(index),
        )
        self.wake_cursor = "$"
        self.task: asyncio.Task[None] | None = None
        self.changed = asyncio.Event()

    @property
    def size(self) -> int:
        return len(self.streams)

    def add(self, key: str, reader: _Reader, cursor: str) -> None:
        current = self.streams.get(key)
        if current is None:
            self.streams[key] = (cursor, {reader})
        else:
            current[1].add(reader)
        self.changed.set()
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.run())

    def remove(self, key: str, reader: _Reader) -> None:
        current = self.streams.get(key)
        if current is None:
            return
        current[1].discard(reader)
        if not current[1]:
            del self.streams[key]

    async def wake(self) -> None:
        with contextlib.suppress(RedisError, OSError):
            await self.manager.app.redis.execute(
                "XADD",
                self.wake_key,
                "MAXLEN",
                "1",
                "*",
                "w",
                "1",
            )
            await self.manager.app.redis.expire(self.wake_key, 3600)

    async def run(self) -> None:  # noqa: C901
        redis = self.manager.app.redis
        attempt = 0
        while True:
            if not self.streams:
                self.changed.clear()
                await self.changed.wait()
                continue
            self.changed.clear()
            request = {key: cursor for key, (cursor, _) in self.streams.items()}
            if self.wake_cursor == "$":
                head = await redis.xrevrange(self.wake_key, count=1)
                self.wake_cursor = head[0][0].decode() if head else "0-0"
            request[self.wake_key] = self.wake_cursor
            try:
                batches = await redis.xread(request, count=200, block=1000)
                attempt = 0
            except (RedisError, OSError) as exc:
                attempt += 1
                logger.warning("Channel hub read failed (%s); retrying.", exc)
                await asyncio.sleep(min(0.1 * 2**attempt, 5.0))
                continue
            for raw_key, entries in batches:
                key = raw_key.decode()
                if not entries:
                    continue
                last = entries[-1][0].decode()
                if key == self.wake_key:
                    self.wake_cursor = last
                    continue
                current = self.streams.get(key)
                if current is None:
                    continue
                self.streams[key] = (last, current[1])
                for entry_id, fields in entries:
                    message = ChannelMessage.from_entry(entry_id, fields)
                    for reader in list(current[1]):
                        reader.deliver(message)


class ChannelConnection:
    """A reader (and sender) attached to one channel, e.g. one websocket."""

    def __init__(
        self,
        manager: ChannelManager,
        name: str,
        *,
        last_id: str | None,
        reader_id: str | None,
        presence: bool,
        identity: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        self.manager = manager
        self.name = name
        self.identity = identity
        self.meta = meta
        self.requested_last_id = last_id
        self.reader_id = reader_id or uuid.uuid4().hex
        self.presence = presence
        self.reader = _Reader(name, manager.app.config.channels.reader_buffer)
        self._heartbeat: asyncio.Task[None] | None = None
        self._hub: _Hub | None = None
        self._handler = False

    @property
    def last_id(self) -> str:
        """Id of the last message received (pass it to resume later)."""
        return self.reader.last_id

    @property
    def _info(self) -> dict[str, Any]:
        return {"identity": self.identity, "meta": self.meta}

    async def __aenter__(self) -> Self:
        manager = self.manager
        await manager.authorize(self.name, self.identity, self.meta, "read")
        self._hub = await manager.subscribe_reader(
            self.name,
            self.reader,
            self.requested_last_id,
        )
        if self.presence:
            await manager.touch_presence(self.name, self.reader_id, self._info)
            self._heartbeat = asyncio.create_task(self._beat())
            self._handler = await manager.has_handler(self.name)
            if self._handler:
                with contextlib.suppress(ChannelFullError):
                    await manager.send(
                        self.name,
                        self._info,
                        sender=self.reader_id,
                        kind="join",
                    )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat
        if self._hub is not None:
            self._hub.remove(self.manager.key(self.name, "out"), self.reader)
        if self.presence:
            with contextlib.suppress(RedisError, OSError):
                await self.manager.app.redis.zrem(
                    self.manager.key(self.name, "presence"),
                    self.reader_id,
                )
                await self.manager.app.redis.hdel(
                    self.manager.key(self.name, "presence:info"),
                    self.reader_id,
                )
                if self._handler:
                    with contextlib.suppress(ChannelFullError):
                        await self.manager.send(
                            self.name,
                            self._info,
                            sender=self.reader_id,
                            kind="leave",
                        )

    async def _beat(self) -> None:
        interval = max(self.manager.app.config.channels.presence_ttl / 3, 0.5)
        while True:
            await asyncio.sleep(interval)
            with contextlib.suppress(RedisError, OSError):
                await self.manager.touch_presence(self.name, self.reader_id, self._info)

    async def send(self, data: Any, *, kind: str = "message") -> str:
        """
        Send a message (to the handler if there is one, else to all readers).

        :param data: JSON data.
        :param kind: message kind.
        :raises ChannelDenied: if ``authorize`` refuses this sender.
        :return: message id.
        """
        await self.manager.authorize(self.name, self.identity, self.meta, "send")
        return await self.manager.send(
            self.name,
            data,
            sender=self.reader_id,
            kind=kind,
        )

    def __aiter__(self) -> ChannelConnection:
        return self

    async def __anext__(self) -> ChannelMessage:
        item = await self.reader.queue.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def receive(self, timeout: float | None = None) -> ChannelMessage | None:
        """
        Next message, or None after ``timeout`` seconds.

        :param timeout: seconds to wait (None waits forever).
        :return: message or None.
        """
        try:
            return await asyncio.wait_for(self.__anext__(), timeout)
        except TimeoutError:
            return None


class ChannelManager:
    """Channels of an app."""

    def __init__(self, app: SynTasks) -> None:
        self.app = app
        self.handlers: builtins.list[_HandlerSpec] = []
        self.hubs: builtins.list[_Hub] = []
        self.hosted: dict[str, asyncio.Task[None]] = {}
        self._retries: set[asyncio.Task[None]] = set()
        self._remote_patterns: builtins.list[Any] = []
        self._patterns_read = 0.0

    # -- keys -------------------------------------------------------------

    def key(self, name: str, part: str) -> str:
        """Key of one part of a channel."""
        return self.app.keys("ch", name, part)

    @property
    def _active_key(self) -> str:
        return self.app.keys("ch", "active")

    @property
    def _activate_key(self) -> str:
        return self.app.keys("ch", "activate")

    @property
    def _handlers_key(self) -> str:
        return self.app.keys("ch", "handlers")

    def _expiry_ms(self) -> int:
        return int(self.app.config.channels.idle_expiry * 1000)

    # -- handlers ----------------------------------------------------------

    def handler(
        self,
        pattern: str,
        *,
        idle_timeout: Any = "10m",
        max_lifetime: Any = None,
        max_restarts: int = 5,
        max_tasks: int | None = None,
        authorize: Callable[[ConnectRequest], Any] | None = None,
    ) -> Callable[[Callable[..., Awaitable[None]]], Callable[..., Awaitable[None]]]:
        """
        Register a long-running handler for channels matching ``pattern``.

        ``pattern`` may contain ``{name}`` parameters (one name segment
        each, no ``:``), passed to the handler as keyword arguments.

        :param pattern: e.g. ``"room:{room_id}"``.
        :param idle_timeout: stop after this long without messages and readers.
        :param max_lifetime: restart the handler after this long.
        :param max_restarts: crashes within 10 minutes before giving up.
        :param max_tasks: live handlers of this pattern across the cluster.
        :param authorize: ``fn(ConnectRequest) -> bool`` (or awaitable) checked
            where readers connect and send.
        :return: decorator.
        """
        regex, params = _compile(pattern)

        def decorator(
            func: Callable[..., Awaitable[None]],
        ) -> Callable[..., Awaitable[None]]:
            self.handlers.append(
                _HandlerSpec(
                    pattern=pattern,
                    regex=regex,
                    func=func,
                    idle_timeout=(
                        None if idle_timeout is None else parse_duration(idle_timeout)
                    ),
                    max_lifetime=(
                        None if max_lifetime is None else parse_duration(max_lifetime)
                    ),
                    max_restarts=max_restarts,
                    params=params,
                    max_tasks=max_tasks,
                    authorize=authorize,
                ),
            )
            return func

        return decorator

    def _match(self, name: str) -> tuple[_HandlerSpec, dict[str, str]] | None:
        for spec in self.handlers:
            found = spec.regex.match(name)
            if found:
                return spec, found.groupdict()
        return None

    async def authorize(
        self,
        name: str,
        identity: str | None,
        meta: dict[str, Any] | None,
        action: str,
    ) -> None:
        """
        Run the channel's ``authorize`` check (no-op without one).

        :param name: channel name.
        :param identity: who.
        :param meta: their metadata.
        :param action: ``read`` or ``send``.
        :raises ChannelDenied: if refused.
        """
        match = self._match(name)
        if match is None or match[0].authorize is None:
            return
        spec, params = match
        request = ConnectRequest(name, params, identity, meta, action)
        allowed = spec.authorize(request)
        if asyncio.iscoroutine(allowed) or isinstance(allowed, Awaitable):
            allowed = await allowed
        if not allowed:
            raise ChannelDenied(name=name, action=action)

    async def presence(self, name: str) -> builtins.list[Presence]:
        """
        Readers connected to a channel now, with identity and meta.

        :param name: channel name.
        :return: one entry per connection.
        """
        now = self.app.cluster.clock.now_ms()
        ids = await self.app.redis.execute(
            "ZRANGEBYSCORE",
            self.key(name, "presence"),
            now,
            "+inf",
        )
        if not ids:
            return []
        raw = await self.app.redis.execute(
            "HMGET", self.key(name, "presence:info"), *ids
        )
        out = []
        for reader_id, info in zip(ids, raw, strict=True):
            data = json.loads(info) if info else {}
            out.append(
                Presence(
                    reader_id=reader_id.decode(),
                    identity=data.get("identity"),
                    meta=data.get("meta"),
                ),
            )
        return out

    async def has_handler(self, name: str) -> bool:
        """
        Whether some worker registered a handler for this channel.

        :param name: channel name.
        :return: True if messages sent to it go to a handler.
        """
        if self._match(name) is not None:
            return True
        now = time.monotonic()
        if now - self._patterns_read > 5.0:
            raw = await self.app.redis.hgetall(self._handlers_key)
            cutoff = time.time() - _PATTERN_TTL
            self._remote_patterns = [
                _compile(pattern.decode())[0]
                for pattern, seen in raw.items()
                if float(seen) >= cutoff
            ]
            self._patterns_read = now
        return any(regex.match(name) for regex in self._remote_patterns)

    # -- sending -----------------------------------------------------------

    def _encode(self, name: str, data: Any) -> bytes:
        return dumps_bounded(
            data,
            max_bytes=self.app.config.channels.max_message,
            what=f"Message to channel {name}",
        )

    async def publish(
        self,
        name: str,
        data: Any,
        *,
        kind: str = "message",
        sender: str | None = None,
        fence: str | None = None,
    ) -> str:
        """
        Send a message to every reader of a channel.

        :param name: channel name.
        :param data: JSON data.
        :param kind: message kind.
        :param sender: sender id.
        :param fence: owner lease value (handlers only).
        :raises ChannelFencedError: if ``fence`` is stale.
        :return: message id.
        """
        config = self.app.config.channels
        min_id = ""
        if config.retention:
            min_id = str(
                max(self.app.cluster.clock.now_ms() - int(config.retention * 1000), 0),
            )
        reply = await PUBLISH(
            self.app.redis,
            [self.key(name, "out"), self._active_key, self.key(name, "owner")],
            [
                self._encode(name, data),
                sender or "",
                kind,
                config.max_length,
                min_id,
                fence or "",
                name,
                self._expiry_ms(),
            ],
        )
        if reply[0] == b"fenced":
            raise ChannelFencedError(name=name)
        return reply[1].decode()

    async def send(
        self,
        name: str,
        data: Any,
        *,
        sender: str | None = None,
        kind: str = "message",
    ) -> str:
        """
        Send a message to a channel.

        It goes to the channel's handler if it has one (started on demand),
        otherwise to all readers.

        :param name: channel name.
        :param data: JSON data.
        :param sender: sender id.
        :param kind: message kind.
        :raises ChannelFullError: if the handler's inbox is full.
        :return: message id.
        """
        if not await self.has_handler(name):
            return await self.publish(name, data, kind=kind, sender=sender)
        reply = await _SEND_IN(
            self.app.redis,
            [
                self.key(name, "in"),
                self.key(name, "owner"),
                self.key(name, "activating"),
                self._activate_key,
                self._active_key,
                self.key(name, "meta"),
            ],
            [
                self._encode(name, data),
                sender or "",
                kind,
                self.app.config.channels.inbox_max,
                name,
                HANDLER_GROUP,
                self._expiry_ms(),
            ],
        )
        if reply[0] == b"full":
            raise ChannelFullError(name=name)
        return reply[1].decode()

    async def ensure_inbox(self, name: str) -> None:
        """
        Create a channel's inbox and its consumer group.

        :param name: channel name.
        """
        with contextlib.suppress(ResponseError):
            await self.app.redis.xgroup_create(
                self.key(name, "in"),
                HANDLER_GROUP,
                "0",
                mkstream=True,
            )

    # -- reading -----------------------------------------------------------

    def connect(
        self,
        name: str,
        *,
        last_id: str | None = None,
        reader_id: str | None = None,
        presence: bool = True,
        identity: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> ChannelConnection:
        """
        Attach a reader to a channel (``async with``).

        :param name: channel name.
        :param last_id: resume after this message id; None for new messages only.
        :param reader_id: stable id of the reader (default random).
        :param presence: count this reader in presence (and tell the handler).
        :param identity: who connects, e.g. ``"user:42"`` (authorize, presence).
        :param meta: small JSON about them, shown in presence.
        :return: connection.
        """
        return ChannelConnection(
            self,
            name,
            last_id=last_id,
            reader_id=reader_id,
            presence=presence,
            identity=identity,
            meta=meta,
        )

    async def subscribe_reader(  # noqa: D102
        self,
        name: str,
        reader: _Reader,
        last_id: str | None,
    ) -> _Hub:
        key = self.key(name, "out")
        redis = self.app.redis
        if last_id in (None, "$"):
            head = await redis.xrevrange(key, count=1)
            resolved = head[0][0].decode() if head else "0-0"
            catch_up = False
        else:
            resolved = str(last_id)
            catch_up = True
        reader.last_id = resolved
        reader.catching_up = catch_up
        hub = self._hub_for(key)
        hub.add(key, reader, resolved)
        await hub.wake()
        if catch_up:
            first = await redis.xrange(key, count=1)
            if (
                resolved != "0-0"
                and first
                and _id_tuple(first[0][0].decode()) > _id_tuple(resolved)
                and not await redis.xrange(key, resolved, resolved)
            ):
                gap = ChannelMessage(id=resolved, kind="gap", data={"after": resolved})
                reader.queue.put_nowait(gap)
            start = f"({resolved}" if resolved != "0-0" else "-"
            while True:
                entries = await redis.xrange(key, start, "+", count=500)
                for entry_id, fields in entries:
                    message = ChannelMessage.from_entry(entry_id, fields)
                    reader.catching_up = False
                    reader.deliver(message)
                    reader.catching_up = True
                if len(entries) < 500:
                    break
                start = f"({entries[-1][0].decode()}"
            reader.finish_catch_up()
        return hub

    def _hub_for(self, key: str) -> _Hub:
        for hub in self.hubs:
            if key in hub.streams:
                return hub
        limit = self.app.config.channels.hub_max_streams
        for hub in self.hubs:
            if hub.size < limit:
                return hub
        hub = _Hub(self, len(self.hubs))
        self.hubs.append(hub)
        return hub

    async def touch_presence(  # noqa: D102
        self,
        name: str,
        reader_id: str,
        info: dict[str, Any] | None = None,
    ) -> None:
        ttl_ms = int(self.app.config.channels.presence_ttl * 1000)
        now = self.app.cluster.clock.now_ms()
        key = self.key(name, "presence")
        pipe = self.app.redis.pipeline()
        pipe.add("ZADD", key, now + ttl_ms, reader_id)
        pipe.add("PEXPIRE", key, max(ttl_ms * 2, 60_000))
        if info is not None:
            info_key = self.key(name, "presence:info")
            pipe.add(
                "HSET", info_key, reader_id, json.dumps(info, separators=(",", ":"))
            )
            pipe.add("PEXPIRE", info_key, max(ttl_ms * 2, 60_000))
        pipe.add("ZADD", self._active_key, now, name)
        await pipe.execute()

    async def reader_count(self, name: str) -> int:
        """
        Readers connected to a channel now.

        :param name: channel name.
        :return: count.
        """
        key = self.key(name, "presence")
        now = self.app.cluster.clock.now_ms()
        return int(await self.app.redis.execute("ZCOUNT", key, now, "+inf"))

    async def history(
        self,
        name: str,
        *,
        after: str | None = None,
        limit: int = 100,
    ) -> builtins.list[ChannelMessage]:
        """
        Messages still kept on a channel (within retention).

        :param name: channel name.
        :param after: only messages after this id.
        :param limit: max messages.
        :return: messages, oldest first.
        """
        start = f"({after}" if after else "-"
        entries = await self.app.redis.xrange(
            self.key(name, "out"),
            start,
            "+",
            count=limit,
        )
        return [
            ChannelMessage.from_entry(entry_id, fields) for entry_id, fields in entries
        ]

    async def consume(
        self,
        name: str,
        group: str,
        *,
        consumer: str | None = None,
        batch: int = 10,
    ) -> AsyncIterator[ChannelMessage]:
        """
        Share a channel's messages among many consumers.

        Each message goes to one consumer of ``group`` (e.g. fan-out
        pipelines).

        A message is acknowledged when the loop asks for the next one.

        :param name: channel name.
        :param group: consumer group.
        :param consumer: consumer name (default: this node).
        :param batch: messages per read.
        :yield: messages.
        """
        key = self.key(name, "out")
        redis = self.app.redis
        consumer = consumer or self.app.cluster.node_id
        with contextlib.suppress(ResponseError):
            await redis.xgroup_create(key, group, "$", mkstream=True)
        while True:
            batches = await redis.xreadgroup(
                group,
                consumer,
                {key: ">"},
                count=batch,
                block=1000,
            )
            for _, entries in batches:
                for entry_id, fields in entries:
                    yield ChannelMessage.from_entry(entry_id, fields)
                    await redis.xack(key, group, entry_id)

    # -- operations ---------------------------------------------------------

    async def info(self, name: str) -> ChannelInfo:
        """
        State of a channel.

        :param name: channel name.
        :return: info.
        """
        redis = self.app.redis
        pipe = redis.pipeline()
        pipe.add("XLEN", self.key(name, "out"))
        pipe.add("XLEN", self.key(name, "in"))
        pipe.add("GET", self.key(name, "owner"))
        pipe.add("ZSCORE", self._active_key, name)
        pipe.add("HMGET", self.key(name, "meta"), "failed", "error")
        length, inbox, owner, active, meta = await pipe.execute()
        return ChannelInfo(
            name=name,
            length=int(length),
            inbox=int(inbox),
            readers=await self.reader_count(name),
            owner=owner.decode().rsplit(":", 1)[0] if owner else None,
            last_active_ms=int(float(active)) if active else None,
            failed=meta[0] == b"1",
            error=meta[1].decode() if meta[1] else None,
        )

    async def list(self, prefix: str = "") -> builtins.list[ChannelInfo]:
        """
        Channels used recently, most recent first.

        :param prefix: only names starting with this.
        :return: infos.
        """
        names = await self.app.redis.execute("ZREVRANGE", self._active_key, 0, 999)
        return [
            await self.info(raw.decode())
            for raw in names
            if raw.decode().startswith(prefix)
        ]

    async def close(self, name: str | None = None) -> None:
        """
        Close a channel: tell readers, stop its handler and delete it.

        Without a name, stops this process's channel machinery (used on
        shutdown).

        :param name: channel name.
        """
        if name is None:
            for hub in self.hubs:
                if hub.task is not None:
                    hub.task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await hub.task
            self.hubs = []
            return
        with contextlib.suppress(ChannelFencedError):
            await self.publish(name, None, kind="close")
        await self.app.redis.delete(
            self.key(name, "in"),
            self.key(name, "state"),
            self.key(name, "presence"),
            self.key(name, "meta"),
            self.key(name, "owner"),
        )
        await self.app.redis.zrem(self._active_key, name)
        await self.app.cluster.control.publish("channel.closed", name=name)
        self._stop_local(name)

    async def restart(self, name: str) -> None:
        """
        Clear a channel's failed state and start its handler again.

        :param name: channel name.
        """
        await self.app.redis.delete(self.key(name, "meta"))
        await self.app.redis.execute(
            "XADD",
            self._activate_key,
            "*",
            "name",
            name,
            "hops",
            "0",
        )

    # -- hosting handlers (worker side) -------------------------------------

    def hosted_count(self) -> int:
        """Handlers running in this process."""
        return len(self.hosted)

    def _stop_local(self, name: str) -> None:
        task = self.hosted.get(name)
        if task is not None:
            task.cancel()

    async def _register_patterns(self) -> None:
        if not self.handlers:
            return
        now = str(time.time())
        await self.app.redis.hset(
            self._handlers_key,
            {spec.pattern: now for spec in self.handlers},
        )

    async def host(self, runtime: Any) -> None:  # noqa: C901
        """
        Run channel handlers in this worker (until cancelled).

        :param runtime: the worker runtime.
        """
        if not self.handlers:
            return
        redis = self.app.redis
        node = self.app.cluster.node_id
        self.app.cluster.control.on(
            "channel.closed",
            lambda event: self._stop_local(str(event.get("name"))),
        )
        with contextlib.suppress(ResponseError):
            await redis.xgroup_create(
                self._activate_key,
                _HOSTS_GROUP,
                "0",
                mkstream=True,
            )
        next_register = 0.0
        try:
            while True:
                if time.monotonic() >= next_register:
                    await self._register_patterns()
                    next_register = time.monotonic() + _PATTERN_TTL / 3
                free = self.app.config.channels.max_handlers - len(self.hosted)
                if free <= 0:
                    await asyncio.sleep(0.5)
                    continue
                try:
                    batches = await redis.xreadgroup(
                        _HOSTS_GROUP,
                        node,
                        {self._activate_key: ">"},
                        count=min(free, 10),
                        block=1000,
                    )
                except ResponseError as exc:
                    if "NOGROUP" in str(exc):
                        await redis.xgroup_create(
                            self._activate_key,
                            _HOSTS_GROUP,
                            "0",
                            mkstream=True,
                        )
                        continue
                    raise
                for _, entries in batches:
                    for entry_id, fields in entries:
                        pipe = redis.pipeline()
                        pipe.add("XACK", self._activate_key, _HOSTS_GROUP, entry_id)
                        pipe.add("XDEL", self._activate_key, entry_id)
                        await pipe.execute()
                        name = fields.get(b"name", b"").decode()
                        hops = int(fields.get(b"hops", b"0") or 0)
                        if name:
                            await self._activate(name, hops)
        finally:
            tasks = list(self.hosted.values())
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task

    async def _activate(self, name: str, hops: int) -> None:
        if name in self.hosted:
            return
        match = self._match(name)
        if match is None:
            if hops < _MAX_HOPS:
                await self.app.redis.execute(
                    "XADD",
                    self._activate_key,
                    "*",
                    "name",
                    name,
                    "hops",
                    str(hops + 1),
                )
            return
        spec, params = match
        slot = None
        if spec.max_tasks is not None:
            slot = self.app.cluster.semaphore(
                f"chan:{spec.pattern}",
                spec.max_tasks,
                ttl_ms=int(self.app.config.channels.lease_ttl * 1000),
            )
            if not await slot.acquire(self._slot_holder(name)):
                self._retry_later(name)
                return
        lease = Lease(
            self.app.redis,
            self.key(name, "owner"),
            self.app.cluster.node_id,
            int(self.app.config.channels.lease_ttl * 1000),
            self.app.keys("lease", "token"),
        )
        if not await lease.acquire():
            if slot is not None:
                await slot.release(self._slot_holder(name))
            return
        self.hosted[name] = asyncio.create_task(
            self._run(spec, name, params, lease, slot),
        )

    def _slot_holder(self, name: str) -> str:
        return f"{self.app.cluster.node_id}|{name}"

    def _retry_later(self, name: str) -> None:
        """Ask again for a channel waiting for a free handler slot."""

        async def retry() -> None:
            await asyncio.sleep(_SLOT_RETRY)
            with contextlib.suppress(RedisError, OSError):
                if await self.app.redis.xlen(self.key(name, "in")) > 0:
                    await self.app.redis.execute(
                        "XADD",
                        self._activate_key,
                        "*",
                        "name",
                        name,
                        "hops",
                        "0",
                    )

        task = asyncio.create_task(retry())
        self._retries.add(task)
        task.add_done_callback(self._retries.discard)

    async def _run(
        self,
        spec: _HandlerSpec,
        name: str,
        params: dict[str, str],
        lease: Lease,
        slot: Any = None,
    ) -> None:
        channel = Channel(self, name, params, lease, idle_timeout=spec.idle_timeout)
        crashed: BaseException | None = None
        holder = self._slot_holder(name)
        renewer = asyncio.create_task(self._renew(lease, slot, holder))
        handler: asyncio.Task[None] | None = None
        logger.info("Channel %s: handler started on this node.", name)
        try:
            await channel.load_state()
            handler = asyncio.create_task(spec.func(channel, **params))
            done, _ = await asyncio.wait(
                {handler, renewer},
                timeout=spec.max_lifetime,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if handler not in done:
                handler.cancel()
                with contextlib.suppress(asyncio.CancelledError, ChannelFencedError):
                    await handler
            else:
                handler.result()
        except asyncio.CancelledError:
            if handler is not None and not handler.done():
                handler.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await handler
            raise
        except ChannelFencedError:
            logger.warning("Channel %s: ownership moved to another node.", name)
        except Exception as exc:
            crashed = exc
            logger.exception("Channel %s: handler crashed.", name)
        finally:
            renewer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await renewer
            self.hosted.pop(name, None)
            with contextlib.suppress(RedisError, OSError):
                if slot is not None:
                    await asyncio.shield(slot.release(holder))
                await asyncio.shield(self._after_run(spec, name, lease, crashed))

    async def _renew(self, lease: Lease, slot: Any = None, holder: str = "") -> None:
        interval = lease.ttl_ms / 3000
        while True:
            await asyncio.sleep(interval)
            try:
                if not await lease.renew():
                    return
                if slot is not None:
                    await slot.renew(holder)
            except (RedisError, OSError):
                return

    async def _after_run(
        self,
        spec: _HandlerSpec,
        name: str,
        lease: Lease,
        crashed: BaseException | None,
    ) -> None:
        redis = self.app.redis
        meta = self.key(name, "meta")
        if crashed is not None:
            now = self.app.cluster.clock.now_ms()
            first = await redis.hget(meta, "first_crash")
            if first is None or now - int(first) > spec.restart_window * 1000:
                await redis.hset(meta, {"first_crash": now, "crashes": 0})
            crashes = await redis.execute("HINCRBY", meta, "crashes", 1)
            await redis.hset(meta, {"error": repr(crashed)[:1000]})
            if crashes >= spec.max_restarts:
                await redis.hset(meta, {"failed": "1"})
                logger.error(
                    "Channel %s: handler crashed %d times; not restarting it "
                    "(app.channels.restart(%r) to retry).",
                    name,
                    crashes,
                    name,
                )
                await lease.release()
                return
            await asyncio.sleep(min(2.0 ** int(crashes), 30.0))
        else:
            await redis.hdel(meta, "crashes", "first_crash")
        await lease.release()
        if await redis.xlen(self.key(name, "in")) > 0:
            await redis.execute(
                "XADD",
                self._activate_key,
                "*",
                "name",
                name,
                "hops",
                "0",
            )

    # -- cleanup -------------------------------------------------------------

    def add_janitor_hooks(self, janitor: Janitor) -> None:
        """
        Register channel cleanup.

        :param janitor: the janitor.
        """
        janitor.add_cleanup(self._cleanup)

    async def _cleanup(self) -> None:
        redis = self.app.redis
        now = self.app.cluster.clock.now_ms()
        await redis.execute(
            "ZREMRANGEBYSCORE",
            self._active_key,
            "-inf",
            now - self._expiry_ms(),
        )
        # Restart handlers whose node died while messages were waiting.
        recent = await redis.execute(
            "ZRANGEBYSCORE",
            self._active_key,
            now - 600_000,
            "+inf",
        )
        for raw in recent:
            name = raw.decode()
            pipe = redis.pipeline()
            pipe.add("XLEN", self.key(name, "in"))
            pipe.add("EXISTS", self.key(name, "owner"))
            pipe.add("HGET", self.key(name, "meta"), "failed")
            backlog, owned, failed = await pipe.execute()
            if int(backlog) and not int(owned) and failed != b"1":
                await redis.execute(
                    "XADD",
                    self._activate_key,
                    "*",
                    "name",
                    name,
                    "hops",
                    "0",
                )
