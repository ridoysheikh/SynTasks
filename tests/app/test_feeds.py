"""Feeds: one shared source per kind, run only while somebody watches."""

import asyncio
import zlib
from collections import Counter
from typing import Any

import pytest

from syntasks import Feed, SynTasks, Update
from syntasks.config import Config, ConfigError

from .conftest import AppFactory, running, wait_until


def poll_feed(app: SynTasks, calls: Counter[str], **options: Any) -> None:
    """A polled feed whose "upstream" is a counter."""
    options.setdefault("every", "50ms")
    options.setdefault("linger", "400ms")

    @app.feed("quote", **options)
    async def quote(key: str) -> dict[str, Any]:
        calls[key] += 1
        return {"key": key, "n": calls[key]}


async def updates(watch: Any, count: int, timeout: float = 10) -> list[Update]:
    out: list[Update] = []
    async with asyncio.timeout(timeout):
        while len(out) < count:
            update = await watch.receive(timeout=timeout)
            assert update is not None
            out.append(update)
    return out


async def test_each_watched_key_is_fetched_once_however_many_watch_it(
    app_factory: AppFactory,
) -> None:
    calls: Counter[str] = Counter()
    worker = app_factory()
    poll_feed(worker, calls)
    # Two "web servers" only declare the feed; they never fetch.
    web_a, web_b = app_factory(), app_factory()
    poll_feed(web_a, Counter())
    poll_feed(web_b, Counter())

    async with (
        running(worker),
        web_a.feeds.watch("u1") as one,
        web_b.feeds.watch("u2") as two,
        web_a.feeds.watch("u3") as three,
    ):
        await one.subscribe("list", ["quote:AAPL", "quote:MSFT"])
        await two.subscribe("list", ["quote:AAPL"])
        await three.subscribe("list", ["quote:AAPL"])
        await wait_until(lambda: calls["AAPL"] >= 6 and calls["MSFT"] >= 6)
        for watch in (one, two, three):
            first = await watch.receive(timeout=5)
            assert first is not None and first.channel.startswith("quote:")
    # AAPL has three watchers and MSFT one, yet both are fetched at the same rate.
    assert abs(calls["AAPL"] - calls["MSFT"]) <= 3


async def test_last_value_is_delivered_on_subscribe(app_factory: AppFactory) -> None:
    calls: Counter[str] = Counter()
    app = app_factory()
    poll_feed(app, calls, every="1h")  # one round when the key is first watched
    async with running(app):
        async with app.feeds.watch("first") as first:
            await first.subscribe("g", ["quote:AAPL"])
            live = await first.receive(timeout=5)
            assert live is not None and live.cached is False
        async with app.feeds.watch("second") as second:
            await second.subscribe("g", ["quote:AAPL"])
            cached = await second.receive(timeout=1)
    assert cached is not None
    assert (cached.channel, cached.cached) == ("quote:AAPL", True)
    assert cached.data == live.data
    assert calls["AAPL"] == 1


async def test_feed_stops_after_linger_and_restarts_on_demand(
    app_factory: AppFactory,
) -> None:
    calls: Counter[str] = Counter()
    app = app_factory()
    poll_feed(app, calls, linger="300ms")
    async with running(app), app.feeds.watch("u") as watch:
        await watch.subscribe("g", ["quote:AAPL"])
        await wait_until(lambda: calls["AAPL"] >= 3)
        await watch.unsubscribe("g")
        await asyncio.sleep(1.2)  # linger, then the handler returns
        stopped = calls["AAPL"]
        await asyncio.sleep(0.5)
        assert calls["AAPL"] == stopped  # nobody watches: no upstream work
        await watch.subscribe("g", ["quote:AAPL"])
        await wait_until(lambda: calls["AAPL"] > stopped + 2)  # and back on demand


async def test_keys_nobody_watches_are_not_fetched(app_factory: AppFactory) -> None:
    calls: Counter[str] = Counter()
    app = app_factory()
    poll_feed(app, calls, linger="300ms")
    async with running(app), app.feeds.watch("u") as watch:
        await watch.subscribe("a", ["quote:AAPL"])
        await watch.subscribe("b", ["quote:MSFT"])
        await wait_until(lambda: calls["MSFT"] >= 2)
        await watch.unsubscribe("b")
        await asyncio.sleep(1.0)  # linger passes
        msft, aapl = calls["MSFT"], calls["AAPL"]
        await asyncio.sleep(0.5)
        assert calls["MSFT"] == msft  # nobody watches MSFT any more
        assert calls["AAPL"] > aapl  # AAPL is still watched, so it goes on


async def test_failing_upstream_is_reported_and_recovers(
    app_factory: AppFactory,
) -> None:
    mode = {"down": True}
    app = app_factory()

    @app.feed("quote", every="50ms", linger="1s")
    async def quote(key: str) -> dict[str, Any]:
        if mode["down"]:
            raise ConnectionError("upstream down")
        return {"key": key}

    async with running(app), app.feeds.watch("u") as watch:
        await watch.subscribe("g", ["quote:AAPL"])
        states = []
        while "degraded" not in states:
            update = await watch.receive(timeout=5)
            assert update is not None
            if update.op == "status":
                states.append(update.data["state"])
        assert (await app.feeds.states())["quote"] == "degraded"
        mode["down"] = False
        seen_data = False
        while not (seen_data and states[-1] == "live"):
            update = await watch.receive(timeout=5)
            assert update is not None
            if update.op == "status":
                states.append(update.data["state"])
            else:
                seen_data = True
        assert states[-1] == "live"
        assert (await app.feeds.states())["quote"] == "live"


async def test_one_failing_key_does_not_degrade_the_feed(
    app_factory: AppFactory,
) -> None:
    app = app_factory()

    @app.feed("quote", every="50ms", linger="1s")
    async def quote(key: str) -> dict[str, Any]:
        if key == "BAD":
            raise KeyError(key)
        return {"key": key}

    seen: list[Update] = []
    async with running(app), app.feeds.watch("u") as watch:
        await watch.subscribe("g", ["quote:BAD", "quote:AAPL"])
        async with asyncio.timeout(10):
            while not any(u.channel == "quote:AAPL" for u in seen):
                update = await watch.receive(timeout=5)
                assert update is not None
                seen.append(update)
        await asyncio.sleep(0.4)  # several rounds in which BAD fails
    assert (await app.feeds.states())["quote"] == "live"
    assert not [u for u in seen if u.op == "status"]


async def test_stream_feed_is_told_which_keys_to_add_and_remove(
    app_factory: AppFactory,
) -> None:
    added: list[str] = []
    removed: list[str] = []
    starts: list[int] = []
    app = app_factory()

    @app.feed("market", linger="300ms")
    async def market(feed: Feed) -> None:
        starts.append(1)  # one upstream connection for the whole kind
        while True:
            now_added, now_removed = await feed.changes()
            added.extend(sorted(now_added))
            removed.extend(sorted(now_removed))
            for key in now_added:
                await feed.publish(key, {"key": key, "px": 1})

    async with running(app), app.feeds.watch("u") as watch:
        await watch.subscribe("g", ["market:AAPL", "market:MSFT"])
        await wait_until(lambda: {"AAPL", "MSFT"} <= set(added))
        first = await updates(watch, 2)
        assert {u.channel for u in first} == {"market:AAPL", "market:MSFT"}
        await watch.unsubscribe("g", ["market:AAPL"])
        await wait_until(lambda: "AAPL" in removed)
        assert "MSFT" not in removed  # still watched, kept alive by the session
    assert starts == [1]


async def test_stream_feed_reconnects_after_a_crash(app_factory: AppFactory) -> None:
    attempts: list[int] = []
    app = app_factory()

    @app.feed("market", linger="1s")
    async def market(feed: Feed) -> None:
        attempts.append(1)
        if len(attempts) == 1:
            raise ConnectionError("socket dropped")
        while True:
            added, _ = await feed.changes()
            for key in added:
                await feed.publish(key, {"key": key, "attempt": len(attempts)})

    async with running(app), app.feeds.watch("u") as watch:
        await watch.subscribe("g", ["market:AAPL"])
        data: list[Update] = []
        states: list[Update] = []
        async with asyncio.timeout(15):
            while not data:
                update = await watch.receive(timeout=10)
                assert update is not None
                (states if update.op == "status" else data).append(update)
    assert len(attempts) == 2
    assert data[0].data["attempt"] == 2
    assert "degraded" in [u.data["state"] for u in states]


async def test_sharded_feed_splits_keys_over_handlers(app_factory: AppFactory) -> None:
    calls: Counter[str] = Counter()
    app = app_factory()
    poll_feed(app, calls, shards=2)
    keys = [f"K{n}" for n in range(40)]
    shard_a = next(k for k in keys if zlib.crc32(k.encode()) % 2 == 0)
    shard_b = next(k for k in keys if zlib.crc32(k.encode()) % 2 == 1)
    async with running(app), app.feeds.watch("u") as watch:
        await watch.subscribe("g", [f"quote:{shard_a}", f"quote:{shard_b}"])
        await wait_until(lambda: calls[shard_a] >= 2 and calls[shard_b] >= 2)
        owners = {
            shard: (await app.channels.info(f"feed:quote:{shard}")).owner
            for shard in (0, 1)
        }
    assert all(owner is not None for owner in owners.values())


async def test_poll_feed_publishes_only_changes(app_factory: AppFactory) -> None:
    rounds = 0
    app = app_factory()

    @app.feed("quote", every="50ms", linger="1s")
    async def quote(key: str) -> dict[str, Any]:
        nonlocal rounds
        rounds += 1
        return {"price": 10 if rounds < 6 else 11}

    async with running(app), app.feeds.watch("u") as watch:
        await watch.subscribe("g", ["quote:AAPL"])
        got = await updates(watch, 2)
        await asyncio.sleep(0.3)
        extra = await watch.receive(timeout=0.2)
    assert [u.data["price"] for u in got] == [10, 11]
    assert extra is None


async def test_feed_declaration_is_validated(app_factory: AppFactory) -> None:
    app = app_factory()

    with pytest.raises(ConfigError, match="kind"):

        @app.feed("bad kind", every="1s")
        async def spaced(key: str) -> None: ...

    with pytest.raises(ConfigError, match="async"):

        @app.feed("sync", every="1s")
        def not_async(key: str) -> None: ...

    with pytest.raises(ConfigError, match="shards"):

        @app.feed("zero", every="1s", shards=0)
        async def zero(key: str) -> None: ...

    with pytest.raises(ConfigError, match="every"):

        @app.feed("never", every="0s")
        async def never(key: str) -> None: ...

    @app.feed("once", every="1s")
    async def once(key: str) -> None: ...

    with pytest.raises(ConfigError, match="already"):

        @app.feed("once", every="1s")
        async def twice(key: str) -> None: ...


def test_feed_needs_room_for_every_shard() -> None:
    config = Config()
    app = SynTasks(
        config,
        search_config=False,
        timeout=None,
        retries=0,
        max_tasks=1,
    )
    with pytest.raises(ConfigError, match="max_tasks"):

        @app.feed("quote", every="1s", shards=4)
        async def quote(key: str) -> None: ...
