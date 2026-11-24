import asyncio
from typing import Any

import pytest

from syntasks import Channel, SynTasks
from syntasks.channels.channel import ChannelFullError
from syntasks.config import Config

from .conftest import AppFactory, running, wait_until


async def test_broadcast_replay_and_history(app_factory: AppFactory) -> None:
    app = app_factory()
    async with (
        app.channels.connect("news", presence=False) as first,
        app.channels.connect("news") as second,
    ):
        assert await app.channels.reader_count("news") == 1
        first_id = await app.channels.publish("news", {"n": 1})
        await app.channels.publish("news", {"n": 2})
        got_first = [await asyncio.wait_for(first.__anext__(), 5) for _ in range(2)]
        got_second = [await asyncio.wait_for(second.__anext__(), 5) for _ in range(2)]
    assert [m.data for m in got_first] == [{"n": 1}, {"n": 2}]
    assert [m.data for m in got_second] == [{"n": 1}, {"n": 2}]
    async with app.channels.connect("news", last_id=first_id) as late:
        replayed = await asyncio.wait_for(late.__anext__(), 5)
    assert replayed.data == {"n": 2}
    history = await app.channels.history("news")
    assert [m.data for m in history] == [{"n": 1}, {"n": 2}]
    async with app.channels.connect("news", last_id="0-0") as everything:
        assert (await everything.receive(5)).data == {"n": 1}  # type: ignore[union-attr]
    await app.channels.close(None)


async def test_many_readers_share_one_hub(app_factory: AppFactory) -> None:
    app = app_factory()
    readers = [
        app.channels.connect(f"room:{i % 20}", presence=False) for i in range(200)
    ]
    for reader in readers:
        await reader.__aenter__()
    assert len(app.channels.hubs) == 1
    for i in range(20):
        await app.channels.publish(f"room:{i}", i)
    for index, reader in enumerate(readers):
        message = await asyncio.wait_for(reader.__anext__(), 5)
        assert message.data == index % 20
    for reader in readers:
        await reader.__aexit__(None, None, None)
    await app.channels.close(None)


async def test_handler_echo_and_state(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.channel("room:{room_id}", idle_timeout="30s")
    async def room(ch: Channel, room_id: str) -> None:
        async for msg in ch.inbox():
            if msg.kind != "message":
                continue
            ch.state["count"] = ch.state.get("count", 0) + 1
            await ch.publish(
                {"room": room_id, "echo": msg.data, "count": ch.state["count"]},
            )
            await ch.save_state()

    async with running(app):
        client = app_factory()
        async with client.channels.connect("room:42") as conn:
            await conn.send("hi")
            await conn.send("again")
            replies = [await asyncio.wait_for(conn.__anext__(), 10) for _ in range(2)]
        info = await client.channels.info("room:42")
        assert info.owner == app.cluster.node_id
    assert [r.data for r in replies] == [
        {"room": "42", "echo": "hi", "count": 1},
        {"room": "42", "echo": "again", "count": 2},
    ]
    await client.channels.close(None)


async def test_handler_takeover_keeps_state_and_messages(
    app_factory: AppFactory,
) -> None:
    def config(c: Config) -> None:
        c.channels.lease_ttl = 1.0

    processed: list[Any] = []

    def build() -> SynTasks:
        app = app_factory(config)

        @app.channel("counter:{name}", idle_timeout=None)
        async def counter(ch: Channel, name: str) -> None:
            async for msg in ch.inbox():
                if msg.kind != "message":
                    continue
                ch.state["total"] = ch.state.get("total", 0) + int(msg.data)
                processed.append(msg.data)
                await ch.save_state()
                await ch.publish({"total": ch.state["total"]})

        return app

    first, second = build(), build()
    sender = app_factory(config)
    async with running(second):
        async with running(first):
            await sender.channels.send("counter:a", 1)
            await wait_until(lambda: processed == [1])
        # The first worker stopped; the second takes over.
        await sender.channels.send("counter:a", 2)
        await wait_until(lambda: processed == [1, 2], timeout=15)
        state = await sender.redis.hget(
            sender.channels.key("counter:a", "state"),
            "json",
        )
    assert state == b'{"total":3}'


async def test_inbox_full(app_factory: AppFactory) -> None:
    def config(c: Config) -> None:
        c.channels.inbox_max = 2

    app = app_factory(config)

    @app.channel("busy:{x}")
    async def busy(ch: Channel, x: str) -> None:  # never started here
        await asyncio.sleep(0)

    await app.channels.send("busy:1", 1)
    await app.channels.send("busy:1", 2)
    with pytest.raises(ChannelFullError):
        await app.channels.send("busy:1", 3)


async def test_consume_group(app_factory: AppFactory) -> None:
    app = app_factory()
    got: list[Any] = []

    async def worker(name: str) -> None:
        async for msg in app.channels.consume("jobs", "workers", consumer=name):
            got.append((name, msg.data))

    tasks = [asyncio.create_task(worker(f"w{i}")) for i in range(2)]
    await asyncio.sleep(0.2)
    for i in range(6):
        await app.channels.publish("jobs", i)
    await wait_until(lambda: len(got) == 6)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    assert sorted(data for _, data in got) == list(range(6))


async def test_close(app_factory: AppFactory) -> None:
    app = app_factory()
    async with app.channels.connect("temp", presence=False) as conn:
        await app.channels.close("temp")
        message = await asyncio.wait_for(conn.__anext__(), 5)
    assert message.kind == "close"
    assert [c.name for c in await app.channels.list()] == []
    await app.channels.close(None)


async def test_shared_feed_is_opened_once_for_many_watchers(
    app_factory: AppFactory,
) -> None:
    """The documented "one feed, many watchers" pattern: readers start the handler."""
    app = app_factory()
    opened: list[str] = []

    async def pump(ch: Channel, symbol: str) -> None:
        tick = 0
        while True:
            tick += 1
            await ch.publish({"symbol": symbol, "tick": tick})
            await ch.sleep(0.05)

    @app.channel("ticker:{symbol}", idle_timeout="1s")
    async def ticker(ch: Channel, symbol: str) -> None:
        opened.append(symbol)  # an upstream feed would be opened here
        feed = asyncio.create_task(pump(ch, symbol))
        try:
            async for _ in ch.inbox():
                pass
        finally:
            feed.cancel()

    async def watch(client: SynTasks, symbol: str) -> list[int]:
        seen: list[int] = []
        async with client.channels.connect(f"ticker:{symbol}", identity="u") as conn:
            async for message in conn:
                if message.kind == "message":
                    seen.append(message.data["tick"])
                    if len(seen) == 3:
                        break
        return seen

    async with running(app):
        web_nodes = [app_factory(), app_factory()]  # two "web servers"
        results = await asyncio.wait_for(
            asyncio.gather(
                *(watch(web_nodes[i % 2], "AAPL") for i in range(4)),
                watch(web_nodes[0], "MSFT"),
            ),
            15,
        )
        assert sorted(opened) == ["AAPL", "MSFT"]  # 5 watchers, 2 feeds
        assert all(len(r) == 3 for r in results)
        assert (await web_nodes[0].channels.info("ticker:AAPL")).owner is not None
        # Everyone left: the handler stops after idle_timeout, and the feed with it.
        await wait_until(
            lambda: _stopped(web_nodes[0], "ticker:AAPL"),
            timeout=15,
        )
    await web_nodes[0].channels.close(None)


async def _stopped(app: SynTasks, name: str) -> bool:
    return (await app.channels.info(name)).owner is None
