"""Fair queues, per-key rate limits, debounce and throttle."""

import asyncio
import time
from typing import Any

import pytest

from syntasks import TaskSupersededError
from syntasks.config import ConfigError

from .conftest import AppFactory, running, wait_until


def one_at_a_time(config: Any) -> None:
    config.worker.max_async_tasks = 1
    config.worker.prefetch = 0


async def test_fair_small_keys_not_starved(app_factory: AppFactory) -> None:
    app = app_factory(one_at_a_time)
    order: list[str] = []

    @app.task(fair=True, key="kwargs.tenant", max_tasks_per_node=1)
    async def work(tenant: str, n: int) -> None:
        order.append(tenant)
        await asyncio.sleep(0.005)

    for n in range(200):
        await work.kiq(tenant="big", n=n)
    small = [f"small-{i}" for i in range(10)]
    for tenant in small:
        await work.kiq(tenant=tenant, n=0)
    async with running(app):
        await wait_until(lambda: len(order) >= 25, timeout=20)
    first = order[:20]
    assert set(small) <= set(first), first


async def test_fair_keeps_order_per_key(app_factory: AppFactory) -> None:
    app = app_factory(one_at_a_time)
    seen: list[tuple[str, int]] = []

    @app.task(fair=True, key="args.0", max_tasks_per_node=1)
    async def work(tenant: str, n: int) -> None:
        seen.append((tenant, n))

    for n in range(5):
        await work.kiq("a", n)
        await work.kiq("b", n)
    async with running(app):
        await wait_until(lambda: len(seen) == 10, timeout=20)
    assert [n for t, n in seen if t == "a"] == list(range(5))
    assert [n for t, n in seen if t == "b"] == list(range(5))


async def test_debounce_last_wins(app_factory: AppFactory) -> None:
    app = app_factory()
    runs: list[int] = []

    @app.task(debounce="300ms", key="kwargs.doc")
    async def reindex(doc: str, version: int) -> int:
        runs.append(version)
        return version

    async with running(app):
        handles = [await reindex.kiq(doc="d1", version=v) for v in range(5)]
        last = await handles[-1].wait_result(timeout=10)
        await asyncio.sleep(0.5)
    assert runs == [4]
    assert last.return_value == 4
    for handle in handles[:-1]:
        result = await handle.wait_result(timeout=5)
        assert isinstance(result.error, TaskSupersededError)


async def test_debounce_last_wins_two_clients(app_factory: AppFactory) -> None:
    app = app_factory()
    other = app_factory()
    runs: list[int] = []

    for instance in (app, other):

        @instance.task("docs:reindex", debounce="300ms", key="kwargs.doc")
        async def reindex(doc: str, version: int) -> None:
            runs.append(version)

    async def burst(client: Any, versions: range) -> None:
        for version in versions:
            await client.send("docs:reindex", doc="d1", version=version)
            await asyncio.sleep(0.01)

    async with running(app):
        await asyncio.gather(burst(app, range(0, 10, 2)), burst(other, range(1, 10, 2)))
        last = await other.send("docs:reindex", doc="d1", version=99)
        await last.wait_result(timeout=10)
        await asyncio.sleep(0.5)
    assert runs == [99]


async def test_debounce_keys_independent(app_factory: AppFactory) -> None:
    app = app_factory()
    runs: list[str] = []

    @app.task(debounce="200ms", key="kwargs.doc")
    async def reindex(doc: str) -> None:
        runs.append(doc)

    async with running(app):
        await reindex.kiq(doc="a")
        await reindex.kiq(doc="b")
        await reindex.kiq(doc="a")
        await wait_until(lambda: len(runs) == 2, timeout=10)
        await asyncio.sleep(0.4)
    assert sorted(runs) == ["a", "b"]


async def test_throttle_drops_within_window_returns_first_handle(
    app_factory: AppFactory,
) -> None:
    app = app_factory()
    runs: list[int] = []

    @app.task(throttle="400ms", key="kwargs.user")
    async def notify(user: str, n: int) -> int:
        runs.append(n)
        return n

    async with running(app):
        first = await notify.kiq(user="u1", n=1)
        again = await notify.kiq(user="u1", n=2)
        other_user = await notify.kiq(user="u2", n=3)
        assert again.task_id == first.task_id
        assert (await again.wait_result(timeout=10)).return_value == 1
        await other_user.wait_result(timeout=10)
        await asyncio.sleep(0.5)
        later = await notify.kiq(user="u1", n=4)
        assert later.task_id != first.task_id
        await later.wait_result(timeout=10)
    assert sorted(runs) == [1, 3, 4]


async def test_rate_per_key_independent_keys(app_factory: AppFactory) -> None:
    app = app_factory()
    times: dict[str, list[float]] = {"a": [], "b": []}

    @app.task(rate_per_key="2/s", key="kwargs.account")
    async def call(account: str, n: int) -> None:
        times[account].append(time.monotonic())

    for n in range(3):
        await call.kiq(account="a", n=n)
    await call.kiq(account="b", n=0)
    start = time.monotonic()
    async with running(app):
        await wait_until(
            lambda: len(times["a"]) == 3 and len(times["b"]) == 1,
            timeout=10,
        )
    assert times["b"][0] - start < 0.4  # b doesn't wait behind a
    assert times["a"][-1] - times["a"][0] >= 0.9  # a: 3 runs at 2 a second


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"fair": True}, "key"),
        ({"debounce": "1s"}, "key"),
        ({"throttle": "1s"}, "key"),
        ({"rate_per_key": "1/s"}, "key"),
        ({"debounce": "1s", "throttle": "1s", "key": "args.0"}, "debounce"),
        ({"throttle": "1s", "unique_for": "1s", "key": "args.0"}, "throttle"),
    ],
)
async def test_flow_options_validated(
    app_factory: AppFactory,
    options: dict[str, Any],
    message: str,
) -> None:
    app = app_factory()
    with pytest.raises(ConfigError, match=message):

        @app.task(**options)
        async def job(x: str) -> None: ...
