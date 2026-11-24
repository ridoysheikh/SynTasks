"""Task events and streaming; channel limits, authorization and presence."""

import asyncio
from typing import Annotated, Any

import pytest

from syntasks import Channel, ChannelDenied, ConnectRequest, Depends, Emit, SynTasks
from syntasks.config import Config
from syntasks.depends.progress_tracker import ProgressTracker, TaskState
from syntasks.options import MissingLimitError

from .conftest import AppFactory, running, wait_until


async def _kinds(task: Any, timeout: float = 10) -> list[tuple[str, Any]]:
    return [(event.kind, event.data) async for event in task.events(timeout=timeout)]


async def test_events_lifecycle_order(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.task(events=True)
    async def build(progress: Annotated[ProgressTracker[str], Depends()]) -> str:
        await progress.set_progress(TaskState.STARTED, meta="half")
        return "ok"

    async with running(app):
        task = await build.kiq()
        events = await _kinds(task)
    kinds = [kind for kind, _ in events]
    assert kinds == ["queued", "started", "progress", "succeeded"]
    assert events[2][1]["meta"] == "half"
    assert events[3][1]["return_value"] == "ok"


async def test_emit_streams_chunks_in_order(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.task(events=True)
    async def generate(prompt: str, emit: Annotated[Emit, Depends()]) -> str:
        words = prompt.split()
        for word in words:
            await emit(word)
            await asyncio.sleep(0.01)
        return " ".join(words)

    async with running(app):
        task = await generate.kiq("streams arrive in order")
        events = await _kinds(task)
    chunks = [data for kind, data in events if kind == "emit"]
    assert chunks == ["streams", "arrive", "in", "order"]
    assert events[-1][0] == "succeeded"


async def test_late_subscriber_replays(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.task(events=True)
    async def quick(emit: Annotated[Emit, Depends()]) -> None:
        await emit("one")

    async with running(app):
        task = await quick.kiq()
        await task.wait_result(timeout=10)
        events = await _kinds(task)
    assert [kind for kind, _ in events] == ["queued", "started", "emit", "succeeded"]


async def test_retry_and_failure_events(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.task(events=True, retries=1, retry_delay="50ms", dead_letter=False)
    async def flaky() -> None:
        raise ValueError("nope")

    async with running(app):
        task = await flaky.kiq()
        events = await _kinds(task)
    kinds = [kind for kind, _ in events]
    assert kinds == ["queued", "started", "retrying", "started", "failed"]
    assert events[-1][1]["error_type"] == "ValueError"


async def test_events_need_the_option(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.task
    async def silent() -> None: ...

    task = await silent.kiq()
    with pytest.raises(ValueError, match="events=True"):
        await _kinds(task)


async def test_channel_requires_limits(redis_url: str) -> None:
    config = Config()
    config.redis.url = redis_url
    app = SynTasks(config, search_config=False, timeout=None, retries=0)
    with pytest.raises(MissingLimitError, match="max_tasks"):

        @app.channel("room:{room}")
        async def room(ch: Channel, room: str) -> None: ...

    await app.redis.close()


async def test_authorize(app_factory: AppFactory) -> None:
    app = app_factory()
    asked: list[ConnectRequest] = []

    def members_only(request: ConnectRequest) -> bool:
        asked.append(request)
        if request.action == "send":
            return request.identity == "user:1"
        return request.identity in ("user:1", "user:2")

    @app.channel("team:{team}", authorize=members_only)
    async def team(ch: Channel, team: str) -> None:
        async for message in ch.inbox():
            if message.kind == "message":
                await ch.publish(message.data)

    async with running(app):
        with pytest.raises(ChannelDenied, match="team:a"):
            async with app.channels.connect("team:a", identity="user:3"):
                pass
        async with app.channels.connect("team:a", identity="user:2") as reader:
            with pytest.raises(ChannelDenied):
                await reader.send("hi")
            async with app.channels.connect("team:a", identity="user:1") as writer:
                await writer.send("hello")
                got = await reader.receive(timeout=10)
    assert got is not None and got.data == "hello"
    assert asked[0].params == {"team": "a"}
    assert {(r.identity, r.action) for r in asked} >= {
        ("user:3", "read"),
        ("user:2", "send"),
    }


async def test_presence_identity_and_meta(app_factory: AppFactory) -> None:
    app = app_factory()
    joins: list[tuple[str, Any]] = []

    @app.channel("room:{room}")
    async def room(ch: Channel, room: str) -> None:
        async for message in ch.inbox():
            if message.kind in ("join", "leave"):
                joins.append((message.kind, message.data))

    async with running(app):
        async with (
            app.channels.connect("room:1", identity="user:1", meta={"name": "Ada"}),
            app.channels.connect("room:1", identity="user:2", meta={"name": "Alan"}),
        ):
            present = await app.channels.presence("room:1")
            assert sorted((p.identity, (p.meta or {})["name"]) for p in present) == [
                ("user:1", "Ada"),
                ("user:2", "Alan"),
            ]
            await wait_until(lambda: len(joins) == 2, timeout=10)
        await wait_until(lambda: len(joins) == 4, timeout=10)
        assert await app.channels.presence("room:1") == []
    assert {data["identity"] for kind, data in joins if kind == "join"} == {
        "user:1",
        "user:2",
    }
    assert sorted(kind for kind, _ in joins) == ["join", "join", "leave", "leave"]


async def test_channel_max_tasks_per_pattern(app_factory: AppFactory) -> None:
    app = app_factory()
    live = 0
    peak = 0
    handled: list[str] = []

    @app.channel("job:{job}", max_tasks=1, idle_timeout="200ms")
    async def job(ch: Channel, job: str) -> None:
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        try:
            async for message in ch.inbox():
                if message.kind == "message":
                    handled.append(job)
        finally:
            live -= 1

    async with running(app):
        await app.channels.send("job:a", "go")
        await app.channels.send("job:b", "go")
        await wait_until(lambda: sorted(handled) == ["a", "b"], timeout=15)
    assert peak == 1


async def test_channel_timeout_is_max_lifetime(app_factory: AppFactory) -> None:
    app = app_factory()
    stopped = asyncio.Event()

    @app.channel("loop", timeout="300ms")
    async def forever(ch: Channel) -> None:
        try:
            await asyncio.sleep(60)
        finally:
            stopped.set()

    async with running(app):
        await app.channels.send("loop", "start")
        await asyncio.wait_for(stopped.wait(), 10)
