"""Watch: one connection's groups, runtime subscriptions and persistence."""

import asyncio
from collections import Counter
from typing import Any

import pytest

from syntasks import SynTasks, Update, WatchError
from syntasks.config import Config

from .conftest import AppFactory, running, wait_until


def poll_feed(app: SynTasks, calls: Counter[str] | None = None, **options: Any) -> None:
    calls = calls if calls is not None else Counter()
    options.setdefault("every", "50ms")
    options.setdefault("linger", "400ms")

    @app.feed("quote", **options)
    async def quote(key: str) -> dict[str, Any]:
        calls[key] += 1
        return {"key": key, "n": calls[key]}


def quiet_feed(app: SynTasks) -> None:
    """A feed that never publishes by itself: tests publish by hand."""

    @app.feed("quote", every="1h", linger="1s")
    async def quote(key: str) -> None:
        return None


async def drain(watch: Any, wait: float = 0.3) -> list[Update]:
    """Everything that arrives in the next ``wait`` seconds."""
    out: list[Update] = []
    end = asyncio.get_running_loop().time() + wait
    while (left := end - asyncio.get_running_loop().time()) > 0:
        update = await watch.receive(timeout=left)
        if update is not None:
            out.append(update)
    return out


async def test_a_channel_in_two_groups_survives_dropping_one(
    app_factory: AppFactory,
) -> None:
    app = app_factory()
    poll_feed(app)
    async with running(app), app.feeds.watch("u") as watch:
        await watch.subscribe("watchlist", ["quote:AAPL", "quote:MSFT"])
        await watch.subscribe("header", ["quote:AAPL"])
        await watch.unsubscribe("watchlist")  # no channels: drops the whole group
        assert watch.groups() == {"header": ["quote:AAPL"]}
        await drain(watch, wait=0.2)  # whatever was queued before
        later = await drain(watch, wait=0.6)
    assert later, "AAPL is still held by the header group"
    assert {u.channel for u in later} == {"quote:AAPL"}


async def test_unsubscribing_some_channels_of_a_group(app_factory: AppFactory) -> None:
    app = app_factory()
    poll_feed(app)
    async with running(app), app.feeds.watch("u") as watch:
        await watch.subscribe("g", ["quote:AAPL", "quote:MSFT", "quote:IBM"])
        ok, rejected = await watch.unsubscribe("g", ["quote:MSFT", "quote:NOPE"])
        assert ok == ["quote:MSFT"]
        assert rejected == [{"channel": "quote:NOPE", "error": "not_subscribed"}]
        assert watch.groups() == {"g": ["quote:AAPL", "quote:IBM"]}
        with pytest.raises(WatchError, match="unknown_group"):
            await watch.unsubscribe("missing")


async def test_rejections_are_per_channel(app_factory: AppFactory) -> None:
    app = app_factory()
    poll_feed(app)
    async with app.feeds.watch("u") as watch:
        ok, rejected = await watch.subscribe(
            "g",
            [
                "quote:AAPL",
                "nope:AAPL",
                "quote:",
                "quote:bad key",
                "quote:" + "X" * 200,
            ],
        )
    assert ok == ["quote:AAPL"]
    assert [(r["channel"][:12], r["error"]) for r in rejected] == [
        ("nope:AAPL", "unknown_channel"),
        ("quote:", "invalid_key"),
        ("quote:bad ke", "invalid_key"),
        ("quote:XXXXXX", "invalid_key"),
    ]


async def test_channel_limit_counts_distinct_channels(app_factory: AppFactory) -> None:
    def configure(config: Config) -> None:
        config.channels.max_per_watch = 2

    app = app_factory(configure)
    poll_feed(app)
    async with app.feeds.watch("u") as watch:
        await watch.subscribe("a", ["quote:AAPL", "quote:MSFT"])
        ok, rejected = await watch.subscribe("b", ["quote:AAPL", "quote:IBM"])
    assert ok == ["quote:AAPL"]  # already held: costs nothing
    assert rejected == [{"channel": "quote:IBM", "error": "channel_limit"}]


async def test_keys_are_normalized(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.feed("quote", every="1h", normalize=str.upper)
    async def quote(key: str) -> None:
        return None

    async with app.feeds.watch("u") as watch:
        ok, _ = await watch.subscribe("g", ["quote: aapl ", "quote:AAPL"])
    assert ok == ["quote:AAPL", "quote:AAPL"]


async def test_groups_are_saved_per_identity_and_restored_on_request(
    app_factory: AppFactory,
) -> None:
    app = app_factory()
    quiet_feed(app)
    async with app.feeds.watch("ada") as first:
        await first.subscribe("a", ["quote:AAPL"])
        await first.subscribe("b", ["quote:MSFT", "quote:AAPL"])
    async with app.feeds.watch("ada") as second:
        assert second.groups() == {}  # a new tab joins nothing by itself
        assert await second.saved() == {
            "a": ["quote:AAPL"],
            "b": ["quote:AAPL", "quote:MSFT"],
        }
        restored, rejected = await second.restore()
        assert rejected == []
        assert restored == {"a": ["quote:AAPL"], "b": ["quote:AAPL", "quote:MSFT"]}
        assert second.groups() == restored
        await second.unsubscribe("a")  # forgetting a group is saved too
    async with app.feeds.watch("ada") as third:
        assert list(await third.saved()) == ["b"]
    async with app.feeds.watch("alan") as other:
        assert await other.saved() == {}


async def test_a_slow_reader_gets_the_newest_snapshot_per_channel(
    app_factory: AppFactory,
) -> None:
    app = app_factory()
    quiet_feed(app)
    async with app.feeds.watch("u") as watch:
        await watch.subscribe("g", ["quote:AAPL", "quote:MSFT"])
        for n in range(50):
            await app.feeds.publish("quote", "AAPL", {"n": n})
        await app.feeds.publish("quote", "MSFT", {"n": "only"})
        await wait_until(lambda: len(watch._data) == 2)
        got = await drain(watch, wait=0.2)
    assert {u.channel: u.data for u in got} == {
        "quote:AAPL": {"n": 49},
        "quote:MSFT": {"n": "only"},
    }


async def test_closing_stops_delivery_and_frees_the_hub(
    app_factory: AppFactory,
) -> None:
    app = app_factory()
    quiet_feed(app)
    watch = app.feeds.watch("u")
    await watch.subscribe("g", ["quote:AAPL"])
    key = app.channels.key("quote:AAPL", "out")
    assert any(key in hub.streams for hub in app.channels.hubs)
    await watch.close()
    assert not any(key in hub.streams for hub in app.channels.hubs)
    await app.feeds.publish("quote", "AAPL", {"n": 1})
    assert await watch.receive(timeout=0.2) is None
    with pytest.raises(StopAsyncIteration):
        await watch.__anext__()


async def test_handle_speaks_the_subscription_protocol(app_factory: AppFactory) -> None:
    app = app_factory()
    quiet_feed(app)
    async with app.feeds.watch("u") as watch:
        sub = await watch.handle(
            {
                "op": "subscribe",
                "id": "1",
                "group": "w",
                "channels": ["quote:AAPL", "x:y"],
            },
        )
        assert sub == {
            "op": "ack",
            "id": "1",
            "group": "w",
            "subscribed": ["quote:AAPL"],
            "rejected": [{"channel": "x:y", "error": "unknown_channel"}],
        }
        listing = await watch.handle({"op": "list", "id": "2"})
        assert listing["groups"] == {"w": ["quote:AAPL"]}
        assert listing["sources"] == {"quote": "idle"}
        bad_group = await watch.handle({"op": "subscribe", "channels": ["quote:A"]})
        assert (bad_group["op"], bad_group["error"]) == ("error", "bad_group")
        bad_channels = await watch.handle(
            {"op": "subscribe", "group": "g", "channels": "quote:A"}
        )
        assert bad_channels["error"] == "bad_request"
        unknown = await watch.handle({"op": "dance", "id": "3"})
        assert (unknown["error"], unknown["id"]) == ("unknown_op", "3")
        unsub = await watch.handle({"op": "unsubscribe", "id": "4", "group": "w"})
        assert unsub["unsubscribed"] == ["quote:AAPL"]
        restored = await watch.handle({"op": "restore", "id": "5"})
        assert restored["op"] == "ack" and restored["restored"] == {}


async def test_list_shows_saved_groups_of_a_new_connection(
    app_factory: AppFactory,
) -> None:
    app = app_factory()
    quiet_feed(app)
    async with app.feeds.watch("ada") as first:
        await first.handle(
            {"op": "subscribe", "group": "w", "channels": ["quote:AAPL", "quote:MSFT"]}
        )
    async with app.feeds.watch("ada") as second:
        listing = await second.handle({"op": "list"})
        assert listing["groups"] == {}  # this connection has joined nothing yet
        assert listing["saved"] == {"w": ["quote:AAPL", "quote:MSFT"]}
        await second.handle({"op": "restore"})
        listing = await second.handle({"op": "list"})
        assert listing["groups"] == listing["saved"]
    async with app.feeds.watch("someone-else") as other:
        assert (await other.handle({"op": "list"}))["saved"] == {}


def test_update_to_dict() -> None:
    update = Update("data", "quote:AAPL", {"px": 1}, cached=True)
    assert update.to_dict() == {
        "op": "data",
        "channel": "quote:AAPL",
        "data": {"px": 1},
        "cached": True,
    }


async def test_identity_and_group_names_are_checked(app_factory: AppFactory) -> None:
    app = app_factory()
    quiet_feed(app)
    with pytest.raises(WatchError, match="bad_identity"):
        app.feeds.watch("")
    async with app.feeds.watch("u") as watch:
        for group in ("", "g" * 65, 5):
            with pytest.raises(WatchError, match="bad_group"):
                await watch.subscribe(group, ["quote:A"])  # type: ignore[arg-type]
