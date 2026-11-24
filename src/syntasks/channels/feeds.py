"""
Feeds: one shared data source per kind, running only while somebody watches.

A feed is a function. Two shapes cover most uses::

    # Polled: called for every watched key, every 2 seconds.
    @app.feed("quote", every="2s")
    async def quote(key: str) -> dict:
        return await vendor.get_quote(key)          # None = nothing new

    # Streamed: one upstream connection for the whole kind.
    @app.feed("market")
    async def market(feed: Feed) -> None:
        async with vendor.connect() as socket:
            ...
            added, removed = await feed.changes()   # keys to (un)subscribe
            await feed.publish("AAPL", {"price": 123.4})

SynTasks runs it once for the whole cluster (a channel handler, so it moves
to another machine if its own dies), only while keys are watched, and
``linger`` after the last watcher leaves. The last value of each key is kept,
and the feed's health (``live`` / ``degraded``) reaches every watcher.
Users watch through :class:`~syntasks.channels.watch.Watch`, which keeps
their groups and subscribes and unsubscribes at runtime.

Keys (under the app namespace):

==================================  ==============================================
``feed:<kind>:<shard>:watched``     ZSET key → expiry; a watcher refreshes it
                                    every ``linger / 3`` (the feed's demand)
``feed:<kind>:last:<key>``          the last snapshot, kept ``keep_last``
``feed:<kind>:state``               HASH shard → ``live`` / ``degraded`` / ...
``watch:<identity>``                HASH group → saved channels
==================================  ==============================================

The data of ``kind:key`` flows on the ordinary channel of that name, so one
Redis read per process serves every watcher there.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
import zlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from syntasks.channels.channel import Channel, ChannelFencedError
from syntasks.channels.watch import Watch, WatchError
from syntasks.config.loader import ConfigError
from syntasks.redis.errors import RedisError
from syntasks.units import parse_duration

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks

__all__ = ["Feed", "FeedManager", "FeedSpec", "build_spec"]

logger = logging.getLogger("syntasks.feeds")

_KIND = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_MAX_KEY = 128
# A fetch that takes longer than this counts as failed.
_FETCH_TIMEOUT = 30.0
# Reconnect delays of a feed whose function failed or returned.
_BACKOFF_START = 0.2
_BACKOFF_MAX = 30.0


@dataclass(frozen=True)
class FeedSpec:
    """A declared feed (see :func:`build_spec`)."""

    kind: str
    func: Callable[..., Awaitable[Any]]
    every: float | None
    linger: float
    keep_last: float
    shards: int
    concurrency: int
    normalize: Callable[[str], str] | None

    @property
    def pattern(self) -> str:
        """Channel pattern of the handlers that run this feed."""
        return f"feed:{self.kind}:{{shard}}"

    def shard_of(self, key: str) -> int:
        """The handler (shard) that serves a key."""
        return zlib.crc32(key.encode()) % self.shards

    def clean(self, key: str) -> str:
        """
        Normalize and check a key.

        :param key: key as the user wrote it.
        :raises ValueError: if it isn't a usable key.
        :return: the canonical key.
        """
        text = key.strip()
        if self.normalize:
            text = self.normalize(text)
        if not isinstance(text, str) or not text:
            raise ValueError("a key is required")
        if len(text) > _MAX_KEY:
            raise ValueError(f"a key has at most {_MAX_KEY} characters")
        if any(char.isspace() or ord(char) < 32 for char in text):
            raise ValueError("a key has no spaces or control characters")
        return text


def build_spec(
    kind: str,
    func: Callable[..., Awaitable[Any]],
    *,
    every: Any = None,
    linger: Any = "30s",
    keep_last: Any = "7d",
    shards: int = 1,
    concurrency: int = 20,
    normalize: Callable[[str], str] | None = None,
) -> FeedSpec:
    """
    Check a feed declaration.

    :param kind: feed name; channels are ``kind:key``.
    :param func: ``fetch(key)`` if ``every`` is given, else ``run(feed)``.
    :param every: poll interval.
    :param linger: how long the feed runs after the last watcher leaves.
    :param keep_last: how long the last value of a key is kept.
    :param shards: handlers to spread the keys over.
    :param concurrency: parallel fetches per round (polled feeds).
    :param normalize: canonical form of a key, e.g. ``str.upper``.
    :raises ConfigError: if something is invalid.
    :return: the spec.
    """
    if not isinstance(kind, str) or not _KIND.match(kind):
        raise ConfigError(
            detail=f"Feed kind {kind!r}: use 1-64 letters, digits, '_', '.' and '-'.",
        )
    if not inspect.iscoroutinefunction(func):
        raise ConfigError(detail=f"Feed {kind!r}: the function must be async.")
    if not inspect.signature(func).parameters:
        what = "key" if every is not None else "feed"
        raise ConfigError(detail=f"Feed {kind!r}: the function takes the {what}.")
    if not isinstance(shards, int) or not 1 <= shards <= 1024:
        raise ConfigError(detail=f"Feed {kind!r}: shards is between 1 and 1024.")
    if not isinstance(concurrency, int) or concurrency < 1:
        raise ConfigError(detail=f"Feed {kind!r}: concurrency is at least 1.")
    seconds: dict[str, float | None] = {}
    for name, value in (("every", every), ("linger", linger), ("keep_last", keep_last)):
        if value is None and name == "every":
            seconds[name] = None
            continue
        try:
            seconds[name] = parse_duration(value)
        except ValueError as exc:
            raise ConfigError(detail=f"Feed {kind!r}: {name}: {exc}") from exc
        if seconds[name] <= 0:  # type: ignore[operator]
            raise ConfigError(detail=f"Feed {kind!r}: {name} must be positive.")
    return FeedSpec(
        kind=kind,
        func=func,
        every=seconds["every"],
        linger=seconds["linger"],  # type: ignore[arg-type]
        keep_last=seconds["keep_last"],  # type: ignore[arg-type]
        shards=shards,
        concurrency=concurrency,
        normalize=normalize,
    )


class Feed:
    """
    What a streamed feed's function gets: the keys to serve and a way to publish.

    One instance serves one shard of one kind (all of it with ``shards=1``).
    """

    def __init__(
        self, manager: FeedManager, spec: FeedSpec, ch: Channel, shard: int
    ) -> None:
        self.manager = manager
        self.spec = spec
        self.channel = ch
        self.kind = spec.kind
        self.shard = shard
        #: Keys somebody watches now (this shard's).
        self.watched: frozenset[str] = frozenset()
        self._seen: frozenset[str] = frozenset()
        self._changed = asyncio.Event()
        self._added = asyncio.Event()
        self._state: str | None = None
        self._stored = False

    async def refresh(self) -> None:
        """Re-read which keys are watched."""
        redis = self.manager.app.redis
        zkey = self.manager.watched_key(self.kind, self.shard)
        now = self.manager.app.cluster.clock.now_ms()
        pipe = redis.pipeline()
        pipe.add("ZREMRANGEBYSCORE", zkey, "-inf", now)
        pipe.add("ZRANGE", zkey, 0, -1)
        keys = frozenset(raw.decode() for raw in (await pipe.execute())[1])
        if keys != self.watched:
            if keys - self.watched:
                self._added.set()
            self.watched = keys
            self._changed.set()

    async def changes(self) -> tuple[frozenset[str], frozenset[str]]:
        """
        Wait until the watched keys change.

        The first call returns every key watched at that point as added, so
        a restarted feed starts from the full set.

        :return: ``(added, removed)`` since the previous call.
        """
        while True:
            added = self.watched - self._seen
            removed = self._seen - self.watched
            if added or removed:
                self._seen = self.watched
                return added, removed
            self._changed.clear()
            await self._changed.wait()

    async def publish(self, key: str, data: Any) -> None:
        """
        Send a snapshot of a key to everyone watching it.

        It is also kept as the key's last value, and publishing marks the
        feed ``live``.

        :param key: the key (as given in ``watched``).
        :param data: JSON data.
        """
        if self._state != "live":
            await self.set_state("live")
        await self.manager.publish(self.kind, key, data)

    async def set_state(self, state: str) -> None:
        """
        Report the feed's health, e.g. ``"live"``, ``"degraded"`` or your own.

        Watchers of this shard's keys get a status update when it changes.

        :param state: short name of the state.
        """
        manager = self.manager
        redis = manager.app.redis
        skey = manager.state_key(self.kind)
        if self._state is None:
            raw = await redis.hget(skey, str(self.shard))
            self._state = raw.decode() if raw else "live"
            self._stored = raw is not None
        if state == self._state and self._stored:
            return
        previous, self._state = self._state, state
        await redis.hset(skey, {str(self.shard): state})
        self._stored = True
        if previous != state:
            for key in sorted(self.watched):
                await manager.app.channels.publish(
                    f"{self.kind}:{key}",
                    {"state": state},
                    kind="status",
                    sender=f"feed:{self.kind}",
                )


class FeedManager:
    """Feeds of an app (``app.feeds``)."""

    def __init__(self, app: SynTasks) -> None:
        self.app = app
        self.specs: dict[str, FeedSpec] = {}

    def declare(self, spec: FeedSpec) -> None:
        """
        Register a feed (``@app.feed`` does this).

        :param spec: the feed.
        :raises ConfigError: if the kind is taken.
        """
        if spec.kind in self.specs:
            raise ConfigError(detail=f"Feed {spec.kind!r} is already declared.")
        self.specs[spec.kind] = spec

    # -- keys ---------------------------------------------------------------------

    def watched_key(self, kind: str, shard: int) -> str:
        """Demand of one shard of a feed."""
        return self.app.keys("feed", kind, str(shard), "watched")

    def last_key(self, kind: str, key: str) -> str:
        """The last value of a key."""
        return self.app.keys("feed", kind, "last", key)

    def state_key(self, kind: str) -> str:
        """Health of a feed, per shard."""
        return self.app.keys("feed", kind, "state")

    def saved_key(self, identity: str) -> str:
        """Groups saved for an identity."""
        return self.app.keys("watch", identity)

    # -- watching -----------------------------------------------------------------

    def watch(self, identity: str, *, max_channels: int | None = None) -> Watch:
        """
        A session for one connection: groups and runtime subscriptions.

        :param identity: whose groups these are, e.g. ``"user:42"``.
        :param max_channels: distinct channels allowed on the connection
            (default ``[channels] max_per_watch``).
        :return: the watch (use ``async with``).
        """
        limit = max_channels or self.app.config.channels.max_per_watch
        return Watch(self, identity, limit)

    def parse(self, raw: str) -> tuple[str, FeedSpec, str]:
        """
        Resolve ``"kind:key"``.

        :param raw: the channel as a user wrote it.
        :raises WatchError: ``unknown_channel`` or ``invalid_key``.
        :return: ``(canonical channel, spec, canonical key)``.
        """
        kind, sep, key = str(raw).strip().partition(":")
        spec = self.specs.get(kind)
        if not sep or spec is None:
            raise WatchError(code="unknown_channel", description=f"no feed {kind!r}")
        try:
            key = spec.clean(key)
        except ValueError as exc:
            raise WatchError(code="invalid_key", description=str(exc)) from None
        return f"{kind}:{key}", spec, key

    def beat_interval(self) -> float:
        """How often a watcher renews its demand: a third of the shortest linger."""
        lingers = [spec.linger for spec in self.specs.values()]
        return min(max(min(lingers, default=30.0) / 3, 0.1), 10.0)

    async def demand(self, channels: list[str]) -> None:
        """
        Say that these channels are watched, and make sure their feeds run.

        A watcher calls this when it subscribes and then every ``linger / 3``;
        an entry that isn't renewed lapses after ``linger``, which is how a
        feed stops when the last watcher goes.

        :param channels: ``"kind:key"`` names.
        """
        now = self.app.cluster.clock.now_ms()
        pipe = self.app.redis.pipeline()
        handlers: set[str] = set()
        for channel in channels:
            kind, _, key = channel.partition(":")
            spec = self.specs[kind]
            shard = spec.shard_of(key)
            linger = int(spec.linger * 1000)
            zkey = self.watched_key(kind, shard)
            pipe.add("ZADD", zkey, "GT", now + linger, key)
            pipe.add("PEXPIRE", zkey, linger * 10 + 60_000)
            handlers.add(f"feed:{kind}:{shard}")
        await pipe.execute()
        await asyncio.gather(*(self.app.channels.wake(name) for name in handlers))

    async def last_values(self, channels: list[str]) -> list[tuple[str, Any]]:
        """The kept last value of each channel that has one."""
        keys = []
        for channel in channels:
            kind, _, key = channel.partition(":")
            keys.append(self.last_key(kind, key))
        found = []
        for channel, raw in zip(channels, await self.app.redis.mget(keys), strict=True):
            if raw:
                try:
                    found.append((channel, json.loads(raw)))
                except ValueError:
                    continue
        return found

    async def states(self) -> dict[str, str]:
        """
        Health of every declared feed.

        Each is ``live``, a degraded state, or ``idle`` if no handler has
        reported yet.

        :return: ``{kind: state}``.
        """
        out: dict[str, str] = {}
        for kind in self.specs:
            raw = await self.app.redis.hgetall(self.state_key(kind))
            values = {value.decode() for value in raw.values()}
            worse = sorted(values - {"live"})
            out[kind] = worse[0] if worse else ("live" if values else "idle")
        return out

    async def states_of(self, channels: list[str]) -> list[tuple[str, str]]:
        """The recorded state of the shard serving each channel."""
        pipe = self.app.redis.pipeline()
        for channel in channels:
            kind, _, key = channel.partition(":")
            shard = self.specs[kind].shard_of(key)
            pipe.add("HGET", self.state_key(kind), str(shard))
        replies = await pipe.execute()
        return [
            (channel, reply.decode())
            for channel, reply in zip(channels, replies, strict=True)
            if reply
        ]

    async def save_group(self, identity: str, group: str, channels: list[str]) -> None:
        """Remember a group of an identity (an empty one is forgotten)."""
        key = self.saved_key(identity)
        if not channels:
            await self.app.redis.hdel(key, group)
            return
        pipe = self.app.redis.pipeline()
        pipe.add("HSET", key, group, json.dumps(channels))
        pipe.add("PEXPIRE", key, int(self.app.config.channels.watch_keep * 1000))
        await pipe.execute()

    async def publish(self, kind: str, key: str, data: Any) -> None:
        """
        Send a snapshot to everyone watching ``kind:key``.

        It is also kept as the key's last value.

        :param kind: feed kind.
        :param key: key.
        :param data: JSON data.
        """
        spec = self.specs.get(kind)
        keep = int((spec.keep_last if spec else 7 * 86400.0) * 1000)
        name = f"{kind}:{key}"
        channels = self.app.channels
        await channels.publish(name, data, kind="snapshot", sender=f"feed:{kind}")
        await self.app.redis.set(
            self.last_key(kind, key), channels.encode(name, data), px=keep
        )

    # -- the handler --------------------------------------------------------------

    async def run(self, spec: FeedSpec, ch: Channel, shard: int) -> None:
        """
        The channel handler of one shard: run the feed while keys are watched.

        :param spec: the feed.
        :param ch: the handler's channel (its ownership lease).
        :param shard: which shard this handler serves.
        """
        feed = Feed(self, spec, ch, shard)
        await feed.refresh()
        loop = asyncio.get_running_loop()
        backoff = _BACKOFF_START
        every = max(min(spec.linger / 4, 0.5), 0.05)
        while feed.watched:
            feed._seen = frozenset()  # noqa: SLF001
            body: asyncio.Task[Any] = asyncio.ensure_future(
                self._poll(spec, feed) if spec.every else spec.func(feed),
            )
            watcher = asyncio.create_task(self._watch_demand(feed, every))
            started = loop.time()
            try:
                done, _ = await asyncio.wait(
                    {body, watcher},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in (body, watcher):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(body, watcher, return_exceptions=True)
            if watcher in done:
                if (error := watcher.exception()) is not None:
                    raise error
                return  # nobody watches any more
            error = body.exception()
            if error is None:
                logger.info("Feed %s: its function returned; restarting it.", spec.kind)
            else:
                logger.warning(
                    "Feed %s failed (%r); retrying in %.1fs.", spec.kind, error, backoff
                )
            await feed.set_state("degraded")
            if loop.time() - started > _BACKOFF_MAX:
                backoff = _BACKOFF_START
            await ch.sleep(backoff)
            backoff = min(backoff * 2, _BACKOFF_MAX)
            await feed.refresh()

    async def _watch_demand(self, feed: Feed, every: float) -> None:
        """Return when nothing is watched; raise if the channel was lost."""
        while True:
            await asyncio.sleep(every)
            if feed.channel.lease.value is None:
                raise ChannelFencedError(name=feed.channel.name)
            try:
                await feed.refresh()
            except (RedisError, OSError) as exc:
                logger.warning("Feed %s: reading demand failed (%s).", feed.kind, exc)
                continue
            if not feed.watched:
                return

    async def _poll(self, spec: FeedSpec, feed: Feed) -> None:  # noqa: C901
        """The built-in body of a polled feed: fetch every watched key each round."""
        assert spec.every is not None  # noqa: S101
        loop = asyncio.get_running_loop()
        gate = asyncio.Semaphore(spec.concurrency)
        sent: dict[str, str] = {}
        failing: set[str] = set()
        failed = object()

        async def fetch(key: str) -> Any:
            async with gate:
                try:
                    value = await asyncio.wait_for(spec.func(key), _FETCH_TIMEOUT)
                except Exception as exc:
                    if key not in failing:
                        logger.warning(
                            "Feed %s: fetching %r failed: %r", spec.kind, key, exc
                        )
                    failing.add(key)
                    return failed
            if key in failing:
                failing.discard(key)
                logger.info("Feed %s: fetching %r works again.", spec.kind, key)
            return value

        while True:
            feed._added.clear()  # noqa: SLF001
            started = loop.time()
            keys = sorted(feed.watched)
            values = await asyncio.gather(*(fetch(key) for key in keys))
            broken = sum(1 for value in values if value is failed)
            if keys:
                # One bad key (an unknown symbol) is not a sick feed; all of them are.
                await feed.set_state("degraded" if broken == len(keys) else "live")
            for key, value in zip(keys, values, strict=True):
                if value is failed or value is None:
                    continue
                encoded = json.dumps(value, sort_keys=True, default=str)
                if sent.get(key) != encoded:
                    sent[key] = encoded
                    await feed.publish(key, value)
            for gone in set(sent) - set(keys):
                del sent[gone]
            wait = max(spec.every - (loop.time() - started), 0.0)
            try:
                await asyncio.wait_for(feed._added.wait(), wait)  # noqa: SLF001
                await asyncio.sleep(0.02)  # let a burst of new keys land together
            except TimeoutError:
                pass
