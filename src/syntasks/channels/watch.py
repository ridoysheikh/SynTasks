"""
Watch: one connection's view of many feeds, with groups and runtime changes.

A web server opens one :class:`Watch` per user connection (a websocket, say)
and the user subscribes and unsubscribes *groups* of channels while connected::

    async with app.feeds.watch("user:42") as watch:
        await watch.subscribe("watchlist", ["quote:AAPL", "quote:MSFT"])
        await watch.subscribe("header", ["quote:AAPL"])
        async for update in watch:          # one merged, conflated stream
            await websocket.send_json(update.to_dict())

Groups are saved per identity, so a new tab can ask for ``restore()``. A
channel held by two groups stays subscribed until both drop it. The data
frames of a feed are snapshots, so a slow reader gets the newest one per
channel instead of a growing backlog.

:meth:`Watch.handle` runs the same operations from a JSON request
(``subscribe``, ``unsubscribe``, ``list``, ``restore``), so a websocket
endpoint only has to pass messages through.
"""

from __future__ import annotations

import asyncio
import builtins
import contextlib
import json
import logging
from collections import Counter, OrderedDict, deque
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from syntasks.channels.manager import _Reader
from syntasks.exceptions import SynTasksError
from syntasks.redis.errors import RedisError

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.channels.channel import ChannelMessage
    from syntasks.channels.feeds import FeedManager
    from syntasks.channels.manager import _Hub

__all__ = ["Update", "Watch", "WatchError"]

logger = logging.getLogger("syntasks.feeds")

_MAX_GROUP = 64
_MAX_STATUS_QUEUED = 1000


class WatchError(SynTasksError):
    """A watch operation was refused; ``code`` is the short reason."""

    __template__ = "{code}: {description}"
    code: str
    description: str


@dataclass(frozen=True)
class Update:
    """
    What a watcher receives.

    :param op: ``"data"`` (a snapshot from the feed) or ``"status"`` (the
        feed's health changed: ``data`` is ``{"state": ...}``).
    :param channel: ``"kind:key"``.
    :param data: the snapshot, or the status.
    :param cached: True for the last known value, sent when you subscribe.
    """

    op: str
    channel: str
    data: Any = None
    cached: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Plain data, e.g. to send over a websocket."""
        return {
            "op": self.op,
            "channel": self.channel,
            "data": self.data,
            "cached": self.cached,
        }


class _WatchReader(_Reader):
    """A channel reader that hands messages straight to its watch."""

    def __init__(self, watch: Watch, channel: str) -> None:
        super().__init__(channel, 1)
        self.watch = watch

    def deliver(self, message: ChannelMessage) -> None:
        current = tuple(int(part) for part in self.last_id.split("-"))
        if tuple(int(part) for part in message.id.split("-")) <= current:
            return
        self.last_id = message.id
        self.watch._on_message(self.name, message)  # noqa: SLF001


class Watch:
    """One connection's subscriptions. Create it with ``app.feeds.watch(...)``."""

    def __init__(self, feeds: FeedManager, identity: str, max_channels: int) -> None:
        if not isinstance(identity, str) or not 0 < len(identity) <= 200:
            raise WatchError(
                code="bad_identity",
                description="identity is a string of 1-200 characters",
            )
        self.feeds = feeds
        self.identity = identity
        self.max_channels = max_channels
        self._groups: dict[str, set[str]] = {}
        self._refs: Counter[str] = Counter()
        self._readers: dict[str, tuple[_WatchReader, _Hub]] = {}
        self._ctrl: deque[Update] = deque(maxlen=_MAX_STATUS_QUEUED)
        self._data: OrderedDict[str, Update] = OrderedDict()
        self._wake = asyncio.Event()
        self._beat: asyncio.Task[None] | None = None
        self._closed = False

    def __repr__(self) -> str:
        return f"<Watch {self.identity!r} {len(self._refs)} channels>"

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    # -- subscriptions -----------------------------------------------------------

    def groups(self) -> dict[str, list[str]]:
        """The groups joined on this connection: ``{group: [channels]}``."""
        return {name: sorted(chans) for name, chans in self._groups.items()}

    async def saved(self) -> dict[str, list[str]]:
        """The groups saved for this identity (by any connection)."""
        raw = await self.feeds.app.redis.hgetall(self.feeds.saved_key(self.identity))
        out: dict[str, list[str]] = {}
        for group, value in raw.items():
            with contextlib.suppress(ValueError):
                out[group.decode()] = list(json.loads(value))
        return out

    async def subscribe(
        self,
        group: str,
        channels: builtins.list[str],
    ) -> tuple[builtins.list[str], builtins.list[dict[str, str]]]:
        """
        Add channels to a group, starting their feeds if nobody watches them yet.

        Partial success is normal: one bad symbol rejects that symbol, not the
        whole list.

        :param group: group name (1-64 characters).
        :param channels: ``"kind:key"`` names.
        :raises WatchError: if the group name is invalid.
        :return: ``(subscribed, rejected)``; each rejection is
            ``{"channel": ..., "error": code}``.
        """
        self._check_group(group)
        ok, rejected = await self._join(group, channels)
        if ok or group in self._groups:  # a rejected-only request keeps what's saved
            await self._save(group)
        return ok, rejected

    async def unsubscribe(
        self,
        group: str,
        channels: builtins.list[str] | None = None,
    ) -> tuple[builtins.list[str], builtins.list[dict[str, str]]]:
        """
        Remove channels from a group (all of them drops the group).

        A channel another group still holds stays subscribed. Its feed stops
        ``linger`` after the last watcher is gone.

        :param group: group name.
        :param channels: channels to remove; None drops the whole group.
        :raises WatchError: if the group isn't joined on this connection.
        :return: ``(unsubscribed, rejected)``.
        """
        held = self._groups.get(group)
        if held is None:
            raise WatchError(
                code="unknown_group",
                description="no such group on this connection",
            )
        targets = sorted(held) if channels is None else list(channels)
        ok: list[str] = []
        rejected: list[dict[str, str]] = []
        for raw in targets:
            try:
                channel = self.feeds.parse(raw)[0]
            except WatchError:
                channel = None
            if channel is None or channel not in held:
                rejected.append({"channel": raw, "error": "not_subscribed"})
                continue
            held.discard(channel)
            self._refs[channel] -= 1
            if self._refs[channel] <= 0:
                del self._refs[channel]
                self._detach(channel)
            ok.append(channel)
        if not held:
            del self._groups[group]
        await self._save(group)
        return ok, rejected

    async def restore(
        self,
    ) -> tuple[dict[str, builtins.list[str]], builtins.list[dict[str, str]]]:
        """
        Join every group saved for this identity.

        Explicit, never automatic: each tab shows its own panels, and a tab
        that restored every group another tab used would stream data it never
        displays.

        :return: ``({group: subscribed channels}, rejected)``.
        """
        restored: dict[str, list[str]] = {}
        rejected: list[dict[str, str]] = []
        for group, channels in (await self.saved()).items():
            ok, rej = await self._join(group, channels)
            restored[group] = ok
            rejected.extend(rej)
        return restored, rejected

    # -- the JSON protocol -------------------------------------------------------

    async def handle(self, request: dict[str, Any]) -> dict[str, Any]:
        """
        Run one request of the subscription protocol and return the reply.

        ``{"op": "subscribe", "group": "w", "channels": ["quote:AAPL"]}``,
        ``unsubscribe`` (``channels`` optional), ``list`` (the groups this
        connection has joined, and the groups saved for the identity) and
        ``restore`` (join the saved groups). An
        ``id`` in the request is echoed. Failures come back as
        ``{"op": "error", "error": code, "error_description": ...}``.

        :param request: the decoded JSON request.
        :return: the reply to send.
        """
        reply: dict[str, Any] = {}
        if isinstance(request, dict) and request.get("id") is not None:
            reply["id"] = request["id"]
        try:
            if not isinstance(request, dict):
                raise WatchError(code="bad_request", description="requests are objects")
            op = request.get("op")
            group = request.get("group")
            channels = request.get("channels")
            if channels is not None and (
                not isinstance(channels, list)
                or not all(isinstance(item, str) for item in channels)
            ):
                raise WatchError(
                    code="bad_request",
                    description="channels is a list of strings",
                )
            if op == "subscribe":
                group = self._check_group(group)
                ok, rejected = await self.subscribe(group, channels or [])
                return {
                    "op": "ack",
                    **reply,
                    "group": group,
                    "subscribed": ok,
                    "rejected": rejected,
                }
            if op == "unsubscribe":
                group = self._check_group(group)
                ok, rejected = await self.unsubscribe(group, channels or None)
                return {
                    "op": "ack",
                    **reply,
                    "group": group,
                    "unsubscribed": ok,
                    "rejected": rejected,
                }
            if op == "list":
                return {
                    "op": "list",
                    **reply,
                    "groups": self.groups(),
                    "saved": await self.saved(),
                    "sources": await self.feeds.states(),
                }
            if op == "restore":
                restored, rejected = await self.restore()
                return {
                    "op": "ack",
                    **reply,
                    "restored": restored,
                    "rejected": rejected,
                }
            raise WatchError(code="unknown_op", description=f"unknown op {op!r}")
        except WatchError as exc:
            return {
                "op": "error",
                **reply,
                "error": exc.code,
                "error_description": exc.description,
            }

    # -- receiving -------------------------------------------------------------------

    async def receive(self, timeout: float | None = None) -> Update | None:
        """
        The next update: status changes first, then the newest snapshots.

        :param timeout: seconds to wait; None waits for ever.
        :return: the update, or None on timeout or after :meth:`close`.
        """
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            self._wake.clear()
            if self._ctrl:
                return self._ctrl.popleft()
            if self._data:
                return self._data.popitem(last=False)[1]
            if self._closed:
                return None
            left = None if deadline is None else deadline - loop.time()
            if left is not None and left <= 0:
                return None
            try:
                await asyncio.wait_for(self._wake.wait(), left)
            except TimeoutError:
                return None

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Update:
        update = await self.receive()
        if update is None:
            raise StopAsyncIteration
        return update

    async def close(self) -> None:
        """Stop receiving. Feeds nobody watches stop after their linger."""
        self._closed = True
        if self._beat is not None:
            self._beat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._beat
            self._beat = None
        for channel in list(self._readers):
            self._detach(channel)
        self._groups.clear()
        self._refs.clear()
        self._wake.set()

    # -- internals ---------------------------------------------------------------

    @staticmethod
    def _check_group(group: Any) -> str:
        if not isinstance(group, str) or not 0 < len(group) <= _MAX_GROUP:
            raise WatchError(
                code="bad_group",
                description=f"group is required, at most {_MAX_GROUP} characters",
            )
        return group

    async def _join(
        self,
        group: str,
        channels: builtins.list[str],
    ) -> tuple[builtins.list[str], builtins.list[dict[str, str]]]:
        ok: list[str] = []
        rejected: list[dict[str, str]] = []
        fresh: list[str] = []
        held = self._groups.setdefault(group, set())
        for raw in channels:
            try:
                channel = self.feeds.parse(raw)[0]
            except WatchError as exc:
                rejected.append({"channel": raw, "error": exc.code})
                continue
            if channel in held:
                ok.append(channel)
                continue
            if self._refs[channel] == 0 and len(self._refs) >= self.max_channels:
                rejected.append({"channel": raw, "error": "channel_limit"})
                continue
            held.add(channel)
            self._refs[channel] += 1
            if self._refs[channel] == 1:
                fresh.append(channel)
            ok.append(channel)
        if not held:
            del self._groups[group]
        if fresh:
            await self._attach(fresh)
        return ok, rejected

    async def _attach(self, channels: builtins.list[str]) -> None:
        manager = self.feeds.app.channels
        for channel in channels:
            reader = _WatchReader(self, channel)
            hub = await manager.subscribe_reader(channel, reader, None)
            self._readers[channel] = (reader, hub)
        await self.feeds.demand(channels)  # before the values: don't miss a start
        for channel, value in await self.feeds.last_values(channels):
            if channel not in self._data and channel in self._readers:
                self._data[channel] = Update("data", channel, value, cached=True)
        for channel, state in await self.feeds.states_of(channels):
            if state != "live":
                self._ctrl.append(Update("status", channel, {"state": state}))
        self._wake.set()
        if self._beat is None and not self._closed:
            self._beat = asyncio.create_task(self._heartbeat())

    def _detach(self, channel: str) -> None:
        found = self._readers.pop(channel, None)
        if found is not None:
            reader, hub = found
            hub.remove(self.feeds.app.channels.key(channel, "out"), reader)
        self._data.pop(channel, None)

    def _on_message(self, channel: str, message: ChannelMessage) -> None:
        if message.kind == "snapshot":
            self._data[channel] = Update("data", channel, message.data)
            self._data.move_to_end(channel)
        elif message.kind == "status":
            self._ctrl.append(Update("status", channel, message.data))
        else:
            return
        self._wake.set()

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(self.feeds.beat_interval())
            try:
                await self.feeds.demand(list(self._refs))
            except (RedisError, OSError) as exc:
                logger.warning("Watch heartbeat failed (%s); retrying.", exc)

    async def _save(self, group: str) -> None:
        channels = sorted(self._groups.get(group, ()))
        try:
            await self.feeds.save_group(self.identity, group, channels)
        except (RedisError, OSError) as exc:
            logger.warning("Saving group %r of %s: %s", group, self.identity, exc)
