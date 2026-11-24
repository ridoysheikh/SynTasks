import asyncio
import logging
import time
from typing import Any

import pytest

from syntasks import SynTasks
from syntasks.dependencies import Depends
from syntasks.exceptions import (
    QueueFullError,
    TaskCancelledError,
    TaskMemoryError,
    TaskTimeoutError,
)
from syntasks.worker import receiver as receiver_module
from syntasks.worker.deadline import Deadline

from . import tasks_isolated
from .conftest import AppFactory, running, wait_until


async def test_basic_roundtrip(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.task
    async def add(a: int, b: int) -> int:
        return a + b

    async with running(app):
        task = await add.kiq(1, 2)
        result = await task.wait_result(timeout=10)
    assert result.return_value == 3
    assert not result.is_err


async def test_sync_task_and_send_by_name(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.task(task_name="mul")
    def mul(a: int, b: int) -> int:
        return a * b

    sender = app_factory()
    async with running(app):
        task = await sender.send("mul", 3, 4)
        result = await task.wait_result(timeout=10)
    assert result.return_value == 12


async def test_priorities(app_factory: AppFactory) -> None:
    def configure(config: Any) -> None:
        config.queues["default"] = type(config.queue("x"))(priorities=3)
        config.worker.max_async_tasks = 1
        config.worker.prefetch = 0

    app = app_factory(configure)
    order: list[str] = []

    @app.task
    async def job(name: str) -> None:
        order.append(name)

    for name in ("low1", "low2"):
        await job.kicker().with_priority(2).kiq(name)
    await job.kicker().with_priority(0).kiq("high")
    await job.kicker().with_priority(1).kiq("mid")
    async with running(app):
        await wait_until(lambda: len(order) == 4)
    assert order == ["high", "mid", "low1", "low2"]


async def test_named_queues(app_factory: AppFactory) -> None:
    def configure(config: Any) -> None:
        config.worker.queues = ["emails"]

    app = app_factory(configure)
    seen: list[str] = []

    @app.task(queue="emails")
    async def send_email(to: str) -> None:
        seen.append(to)

    @app.task
    async def other() -> None:
        seen.append("other")

    await other.kiq()
    async with running(app):
        await send_email.kiq("a@example.com")
        await wait_until(lambda: seen == ["a@example.com"])
        await asyncio.sleep(0.3)
    assert seen == ["a@example.com"]
    info = await app.queues.info("default")
    assert info.waiting == 1


async def test_delay(app_factory: AppFactory) -> None:
    app = app_factory()
    ran_at: list[float] = []

    @app.task
    async def job() -> None:
        ran_at.append(time.monotonic())

    async with running(app):
        start = time.monotonic()
        await job.kicker().with_delay(0.5).kiq()
        info = await app.queues.info("default")
        assert info.delayed == 1
        await wait_until(lambda: ran_at)
    assert ran_at[0] - start >= 0.45


async def test_retries_then_success(app_factory: AppFactory) -> None:
    app = app_factory()
    calls: list[int] = []

    @app.task(retries=3, retry_delay="50ms", retry_backoff=1)
    async def flaky() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("boom")
        return "ok"

    async with running(app):
        task = await flaky.kiq()
        result = await task.wait_result(timeout=10)
    assert result.return_value == "ok"
    assert len(calls) == 3
    assert await app.queues.dead_letters("default") == []


async def test_final_failure_dead_letter_and_replay(app_factory: AppFactory) -> None:
    app = app_factory()
    attempts: list[int] = []

    @app.task(retries=1, retry_delay="10ms")
    async def broken(x: int) -> None:
        attempts.append(x)
        raise ValueError("nope")

    async with running(app):
        task = await broken.kiq(7)
        result = await task.wait_result(timeout=10)
        assert result.is_err
        await wait_until(lambda: app.queues.dead_letters("default"))
        letters = await app.queues.dead_letters("default")
        assert letters[0].reason == "failed"
        assert "nope" in letters[0].error
        assert letters[0].task_id == task.task_id
        assert await app.queues.replay_dead("default") == 1
        await wait_until(lambda: len(attempts) == 4)
    assert attempts == [7, 7, 7, 7]


async def test_unique_for(app_factory: AppFactory) -> None:
    app = app_factory()
    runs: list[int] = []

    @app.task(unique_for="10s")
    async def once(x: int) -> None:
        runs.append(x)

    first = await once.kiq(1)
    second = await once.kiq(1)
    third = await once.kiq(2)
    assert first.task_id == second.task_id
    assert third.task_id != first.task_id
    async with running(app):
        await wait_until(lambda: len(runs) == 2)
        await asyncio.sleep(0.2)
    assert sorted(runs) == [1, 2]


async def test_max_length_rejects(app_factory: AppFactory) -> None:
    def configure(config: Any) -> None:
        config.queues["default"] = type(config.queue("x"))(max_length=2)

    app = app_factory(configure)

    @app.task
    async def job() -> None: ...

    await job.kiq()
    await job.kiq()
    with pytest.raises(QueueFullError):
        await job.kiq()


async def test_cluster_concurrency_across_workers(app_factory: AppFactory) -> None:
    active = 0
    peak = 0
    done = 0

    def build() -> SynTasks:
        app = app_factory()

        @app.task(task_name="limited", concurrency=2)
        async def limited() -> None:
            nonlocal active, peak, done
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.2)
            active -= 1
            done += 1

        return app

    first, second = build(), build()
    async with running(first), running(second):
        for _ in range(8):
            await first.send("limited")
        await wait_until(lambda: done == 8, timeout=20)
    assert peak == 2


async def test_node_concurrency(app_factory: AppFactory) -> None:
    app = app_factory()
    active = 0
    peak = 0
    done = 0

    @app.task(node_concurrency=1)
    async def serial() -> None:
        nonlocal active, peak, done
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.05)
        active -= 1
        done += 1

    async with running(app):
        for _ in range(5):
            await serial.kiq()
        await wait_until(lambda: done == 5)
    assert peak == 1


async def test_rate_limit(app_factory: AppFactory) -> None:
    app = app_factory()
    times: list[float] = []

    @app.task(rate="10/s")
    async def limited() -> None:
        times.append(time.monotonic())

    async with running(app):
        for _ in range(6):
            await limited.kiq()
        await wait_until(lambda: len(times) == 6)
    assert times[-1] - times[0] >= 0.4


async def test_key_concurrency_keeps_order(app_factory: AppFactory) -> None:
    app = app_factory()
    log: list[tuple[str, int]] = []
    active: dict[str, int] = {}
    overlap = False

    @app.task(concurrency_key="customer")
    async def charge(customer: str, n: int) -> None:
        nonlocal overlap
        active[customer] = active.get(customer, 0) + 1
        if active[customer] > 1:
            overlap = True
        await asyncio.sleep(0.05)
        log.append((customer, n))
        active[customer] -= 1

    for n in range(4):
        await charge.kiq("a", n)
        await charge.kiq("b", n)
    async with running(app):
        await wait_until(lambda: len(log) == 8, timeout=20)
    assert not overlap
    assert [n for c, n in log if c == "a"] == [0, 1, 2, 3]
    assert [n for c, n in log if c == "b"] == [0, 1, 2, 3]


async def test_timeout(app_factory: AppFactory) -> None:
    app = app_factory()
    cleaned = asyncio.Event()

    @app.task(timeout="200ms", grace="1s")
    async def slow() -> None:
        try:
            await asyncio.sleep(10)
        finally:
            cleaned.set()

    async with running(app):
        task = await slow.kiq()
        result = await task.wait_result(timeout=10)
    assert isinstance(result.error, TaskTimeoutError)
    assert cleaned.is_set()


async def test_soft_timeout_deadline(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.task(soft_timeout="100ms", timeout="5s")
    async def cooperative(deadline: Deadline = Depends()) -> str:
        await asyncio.wait_for(deadline.soft_reached.wait(), 3)
        return f"stopped soft={deadline.soft_expired}"

    async with running(app):
        task = await cooperative.kiq()
        result = await task.wait_result(timeout=10)
    assert result.return_value == "stopped soft=True"


async def test_cancel(app_factory: AppFactory) -> None:
    app = app_factory()
    started = asyncio.Event()

    @app.task
    async def long_job() -> None:
        started.set()
        await asyncio.sleep(30)

    async with running(app):
        task = await long_job.kiq()
        await asyncio.wait_for(started.wait(), 10)
        await wait_until(app.tasks.running)
        running_tasks = await app.tasks.running()
        assert running_tasks[0].task_id == task.task_id
        await app.tasks.cancel(task.task_id)
        result = await task.wait_result(timeout=10)
    assert isinstance(result.error, TaskCancelledError)


async def test_process_isolation(app_factory: AppFactory) -> None:
    app = app_factory()
    add = app.register_task(tasks_isolated.add, "iso_add", isolation="process")
    double = app.register_task(
        tasks_isolated.async_double,
        "iso_double",
        isolation="process",
    )
    forever = app.register_task(
        tasks_isolated.sleep_forever,
        "iso_forever",
        isolation="process",
        timeout="300ms",
        grace="100ms",
    )
    async with running(app):
        assert (await (await add.kiq(2, 3)).wait_result(timeout=20)).return_value == 5
        assert (await (await double.kiq(4)).wait_result(timeout=20)).return_value == 8
        result = await (await forever.kiq()).wait_result(timeout=20)
    assert isinstance(result.error, TaskTimeoutError)


@pytest.mark.skipif(
    not __import__("sys").platform.startswith("linux"),
    reason="RSS via /proc",
)
async def test_process_memory_limit(app_factory: AppFactory) -> None:
    app = app_factory()
    hog = app.register_task(
        tasks_isolated.hog_memory,
        "iso_hog",
        isolation="process",
        memory_limit="200MiB",
    )
    async with running(app):
        result = await (await hog.kiq(400)).wait_result(timeout=30)
    assert isinstance(result.error, (TaskMemoryError, MemoryError))


async def test_pause_resume(app_factory: AppFactory) -> None:
    app = app_factory()
    ran: list[int] = []

    @app.task
    async def job() -> None:
        ran.append(1)

    async with running(app):
        await app.queues.pause("default")
        await asyncio.sleep(0.5)
        await job.kiq()
        await asyncio.sleep(0.8)
        assert ran == []
        assert (await app.queues.info("default")).paused
        await app.queues.resume("default")
        await wait_until(lambda: ran == [1])


async def test_abandoned_message_taken_over(app_factory: AppFactory) -> None:
    app = app_factory()
    ran: list[int] = []

    @app.task
    async def job(x: int) -> None:
        ran.append(x)

    crashed = app_factory()
    crashed.engine.serve(["default"], block=0.1)
    await crashed.engine.ensure_groups()
    await job.kiq(5)
    gen = crashed.engine.listen()
    delivery = await anext(gen)
    assert delivery.task_name == job.task_name
    # The "crashed" consumer never acks; another worker takes over after claim_idle.
    crashed.engine._buffer.clear()
    crashed.engine._inflight.clear()
    await gen.aclose()
    async with running(app):
        await wait_until(lambda: ran == [5], timeout=15)


async def test_gated_task_sent_by_unaware_sender(app_factory: AppFactory) -> None:
    app = app_factory()
    ran: list[int] = []

    @app.task(task_name="gated", concurrency=1)
    async def gated(x: int) -> None:
        ran.append(x)

    sender = app_factory()  # doesn't know "gated" is limited
    async with running(app):
        await sender.send("gated", 1)
        await wait_until(lambda: ran == [1])


async def test_unknown_task_dead_lettered(
    app_factory: AppFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(receiver_module, "_MAX_UNKNOWN_REQUEUES", 0)
    app = app_factory()
    async with running(app):
        await app.send("nobody:has_this", 1)
        await wait_until(lambda: app.queues.dead_letters("default"))
    letters = await app.queues.dead_letters("default")
    assert letters[0].reason == "unknown_task"


async def test_log_capture(app_factory: AppFactory) -> None:
    app = app_factory()
    logger = logging.getLogger("tests.capture")

    @app.task(capture_logs=True)
    async def chatty(n: int) -> None:
        for i in range(n):
            logger.info("line %d", i)
        await asyncio.to_thread(logger.warning, "from a thread")

    @app.task
    async def quiet() -> None:
        logger.info("not captured")

    async with running(app):
        task = await chatty.kiq(3)
        other = await quiet.kiq()
        await task.wait_result(timeout=10)
        await other.wait_result(timeout=10)
    records = [r async for r in app.logs.read(task.task_id)]
    messages = [r.message for r in records if r.logger == "tests.capture"]
    assert messages == ["line 0", "line 1", "line 2", "from a thread"]
    assert [r async for r in app.logs.read(other.task_id)] == []


async def test_result_ttl_and_push_wait(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.task(result_ttl="2s")
    async def quick() -> int:
        await asyncio.sleep(0.2)
        return 1

    async with running(app):
        task = await quick.kiq()
        assert await app.result_backend.wait(task.task_id, timeout=10)
        ttl = await app.redis.execute(
            "PTTL",
            app.result_backend.result_key(task.task_id),
        )
    assert 0 < ttl <= 2000


async def test_nodes_registered(app_factory: AppFactory) -> None:
    app = app_factory()
    async with running(app):
        nodes = await app.nodes()
        assert [n.node_id for n in nodes] == [app.cluster.node_id]
        assert nodes[0].alive
    assert await app.nodes() == []
