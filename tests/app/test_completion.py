"""Callbacks, fan-in, request / reply and batches."""

import asyncio
from typing import Any

import pytest

from syntasks import BatchOutcome, GroupOutcome, TaskOutcome, TaskTimeoutError
from syntasks.message import SynTasksMessage

from .conftest import AppFactory, running, wait_until


async def test_on_success_receives_outcome(app_factory: AppFactory) -> None:
    app = app_factory()
    got: list[TaskOutcome] = []

    @app.task
    async def audit(outcome: TaskOutcome) -> None:
        got.append(outcome)

    @app.task(on_success=audit)
    async def charge(cents: int) -> int:
        return cents * 2

    async with running(app):
        task = await charge.kiq(21)
        await wait_until(lambda: got, timeout=10)
    outcome = got[0]
    assert (outcome.task_id, outcome.task_name, outcome.ok) == (
        task.task_id,
        charge.task_name,
        True,
    )
    assert outcome.return_value == 42
    assert outcome.error is None


async def test_on_failure_after_last_retry_only(app_factory: AppFactory) -> None:
    app = app_factory()
    attempts: list[int] = []
    failures: list[TaskOutcome] = []

    @app.task
    async def alert(outcome: TaskOutcome) -> None:
        failures.append(outcome)

    @app.task(
        retries=2,
        retry_delay="50ms",
        retry_backoff=1,
        on_failure=alert,
        dead_letter=False,
    )
    async def flaky() -> None:
        attempts.append(1)
        raise ValueError("partner down")

    async with running(app):
        await flaky.kiq()
        await wait_until(lambda: failures, timeout=10)
        await asyncio.sleep(0.3)
    assert len(attempts) == 3
    assert len(failures) == 1
    assert failures[0].ok is False
    assert failures[0].error_type == "ValueError"
    assert "partner down" in (failures[0].error or "")


async def test_then_and_catch_per_send(app_factory: AppFactory) -> None:
    app = app_factory()
    seen: list[tuple[str, bool]] = []

    @app.task
    async def done(outcome: TaskOutcome) -> None:
        seen.append(("then", outcome.ok))

    @app.task
    async def failed(outcome: TaskOutcome) -> None:
        seen.append(("catch", outcome.ok))

    @app.task(dead_letter=False)
    async def maybe(fail: bool) -> None:
        if fail:
            raise RuntimeError("no")

    async with running(app):
        await maybe.kicker().then(done).catch(failed).kiq(False)
        await maybe.kicker().then(done).catch(failed).kiq(True)
        await wait_until(lambda: len(seen) == 2, timeout=10)
    assert sorted(seen) == [("catch", False), ("then", True)]


async def test_callback_sent_once(app_factory: AppFactory) -> None:
    app = app_factory()
    got: list[TaskOutcome] = []

    @app.task
    async def audit(outcome: TaskOutcome) -> None:
        got.append(outcome)

    @app.task(on_success=audit)
    async def job() -> int:
        return 1

    async with running(app):
        task = await job.kiq()
        await wait_until(lambda: got, timeout=10)
        result = await task.wait_result(timeout=5)
        message = _message(app, job.task_name, task.task_id)
        await app.completion.after_run(message, result, app.options_for(job.task_name))
        await asyncio.sleep(0.3)
    assert len(got) == 1


def _message(app: Any, task_name: str, task_id: str) -> Any:
    return SynTasksMessage(
        task_id=task_id, task_name=task_name, labels={}, args=[], kwargs={}
    )


async def test_group_then_fan_in_across_two_workers(app_factory: AppFactory) -> None:
    calls: list[GroupOutcome] = []

    def build() -> Any:
        app = app_factory()

        @app.task("fan:square")
        async def square(n: int) -> int:
            await asyncio.sleep(0.05)
            return n * n

        @app.task("fan:collect")
        async def collect(outcome: GroupOutcome) -> None:
            calls.append(outcome)

        return app

    first, second = build(), build()
    async with running(first), running(second):
        group = first.group()
        for n in range(6):
            await group.kiq("fan:square", n)
        await group.then("fan:collect")
        await wait_until(lambda: calls, timeout=10)
        await asyncio.sleep(0.3)
    assert len(calls) == 1
    outcome = calls[0]
    assert (outcome.group_id, outcome.total, outcome.ok, outcome.failed) == (
        group.id,
        6,
        6,
        0,
    )
    assert sorted(outcome.task_ids) == sorted(h.task_id for h in group.handles)


async def test_group_then_after_members_finished(app_factory: AppFactory) -> None:
    app = app_factory()
    calls: list[GroupOutcome] = []

    @app.task
    async def one() -> int:
        return 1

    @app.task
    async def collect(outcome: GroupOutcome) -> None:
        calls.append(outcome)

    async with running(app):
        group = app.group()
        await group.kiq(one)
        await group.kiq(one)
        await group.wait(timeout=10)
        await group.then(collect)
        await wait_until(lambda: calls, timeout=10)
    assert calls[0].total == 2


async def test_call_returns_value_and_raises(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.task
    async def price(sku: str) -> int:
        if sku == "missing":
            raise KeyError(sku)
        return 1999

    @app.task
    async def slow() -> None:
        await asyncio.sleep(5)

    async with running(app):
        assert await price.call("KB-01") == 1999
        with pytest.raises(KeyError):
            await price.call("missing")
        with pytest.raises(TaskTimeoutError):
            await slow.kicker().with_timeout(0.3).call()


async def test_batch_pause_resume_cancel_progress(app_factory: AppFactory) -> None:
    app = app_factory()
    sent: list[str] = []

    @app.task(max_tasks_per_node=1)
    async def send_email(to: str) -> None:
        sent.append(to)
        await asyncio.sleep(0.1)

    campaign = await app.batch("spring", owner="team:marketing")
    await campaign.pause()
    await campaign.map(send_email, ({"to": f"u{n}@x"} for n in range(20)), chunk=7)
    async with running(app):
        await asyncio.sleep(0.5)
        assert sent == []  # paused: nothing runs
        progress = await campaign.progress()
        assert (progress.total, progress.done) == (20, 0)
        await campaign.resume()
        await wait_until(lambda: len(sent) >= 3, timeout=10)
        await campaign.cancel()

        async def drained() -> (
            bool
        ):  # cancelled members are dropped as workers reach them
            current = await campaign.progress()
            return current.done + current.cancelled == current.total

        await wait_until(drained, timeout=15)
        progress = await campaign.progress()
    assert progress.total == 20
    assert progress.done + progress.cancelled == 20
    assert progress.cancelled > 0
    assert progress.state == "cancelled"
    assert len(sent) == progress.done


async def test_batch_map_resumes_and_then(app_factory: AppFactory) -> None:
    app = app_factory()
    done: list[int] = []
    summaries: list[BatchOutcome] = []

    @app.task
    async def work(n: int) -> None:
        done.append(n)

    @app.task
    async def report(outcome: BatchOutcome) -> None:
        summaries.append(outcome)

    batch = await app.batch("nightly")
    items = list(range(10))
    await batch.map(work, items[:4], chunk=2, seal=False)  # interrupted after 4
    await batch.map(work, items, chunk=3, resume=True)  # skips the 4 already sent
    await batch.then(report)
    async with running(app):
        await wait_until(lambda: summaries, timeout=10)
    assert sorted(done) == items
    assert (summaries[0].batch_id, summaries[0].total, summaries[0].done) == (
        "nightly",
        10,
        10,
    )
