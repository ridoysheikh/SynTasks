import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from syntasks.cluster import (
    Cluster,
    Janitor,
    Keys,
    LeasedRole,
    NodeInfo,
    frozen_time,
)
from syntasks.cluster.scripts import Script
from syntasks.redis import Redis
from syntasks.units import parse_rate

T0 = 1_800_000_000_000


@pytest.fixture
def cluster(redis: Redis) -> Cluster:
    return Cluster(redis, "test", node_id="node-a")


def test_keys() -> None:
    keys = Keys("billing")
    assert keys("q", "default") == "syntasks:{billing}:q:default"
    with pytest.raises(ValueError, match="Invalid app name"):
        Keys("bad name")


async def test_script_now_and_reload(redis: Redis) -> None:
    script = Script("now", "return now_ms()")
    server_ms = int(await redis.time() * 1000)
    now = await script(redis)
    assert abs(now - server_ms) < 1000
    with frozen_time(T0):
        assert await script(redis) == T0
    await redis.execute("SCRIPT", "FLUSH")
    assert await script(redis) > 0


async def test_clock(cluster: Cluster) -> None:
    await cluster.clock.sync()
    assert abs(cluster.clock.offset) < 1.0
    with frozen_time(T0):
        assert cluster.clock.now_ms() == T0


async def test_lease_exclusive_and_fencing(redis: Redis) -> None:
    a = Cluster(redis, "test", node_id="a").lease("role", 10_000)
    b = Cluster(redis, "test", node_id="b").lease("role", 10_000)
    assert await a.acquire()
    first_token = a.token
    assert not await b.acquire()
    assert await a.acquire()  # re-acquire extends
    assert a.token == first_token
    assert await a.renew()
    assert await b.current_owner() == "a"
    await a.release()
    assert not a.held
    assert await b.acquire()
    assert b.token is not None
    assert first_token is not None
    assert b.token > first_token
    # a lost it: renew fails
    a.value = f"a:{first_token}"
    assert not await a.renew()
    assert not a.held


async def test_lease_expires(redis: Redis) -> None:
    a = Cluster(redis, "test", node_id="a").lease("role", 50)
    b = Cluster(redis, "test", node_id="b").lease("role", 50)
    assert await a.acquire()
    await asyncio.sleep(0.1)
    assert await b.acquire()
    assert not await a.renew()


async def test_leased_role_takeover(redis: Redis) -> None:
    running: list[str] = []

    def make_work(name: str) -> Callable[[object], Coroutine[Any, Any, None]]:
        async def work(_lease: object) -> None:
            running.append(name)
            await asyncio.Event().wait()

        return work

    role_a = LeasedRole(
        "r",
        Cluster(redis, "test", node_id="a").lease("r", 300),
        make_work("a"),
    )
    role_b = LeasedRole(
        "r",
        Cluster(redis, "test", node_id="b").lease("r", 300),
        make_work("b"),
    )
    task_a = asyncio.create_task(role_a.run())
    await asyncio.sleep(0.15)
    task_b = asyncio.create_task(role_b.run())
    await asyncio.sleep(0.3)
    assert running == ["a"]
    assert role_a.active
    assert not role_b.active
    task_a.cancel()
    await asyncio.gather(task_a, return_exceptions=True)
    for _ in range(40):
        if running == ["a", "b"]:
            break
        await asyncio.sleep(0.05)
    assert running == ["a", "b"]
    task_b.cancel()
    await asyncio.gather(task_b, return_exceptions=True)


async def test_semaphore(cluster: Cluster) -> None:
    sem = cluster.semaphore("charge", limit=2, ttl_ms=10_000)
    assert await sem.acquire("node-a|1")
    assert await sem.acquire("node-a|2")
    assert not await sem.acquire("node-a|3")
    assert await sem.acquire("node-a|1")  # already held
    assert await sem.count() == 2
    assert await sem.renew("node-a|1")
    assert not await sem.renew("node-a|9")
    assert await sem.release("node-a|1")
    assert not await sem.release("node-a|1")
    assert await sem.wait(0.1)  # woken by the release
    assert await sem.acquire("node-a|3")


async def test_semaphore_expiry(cluster: Cluster) -> None:
    sem = cluster.semaphore("x", limit=1, ttl_ms=1000)
    with frozen_time(T0):
        assert await sem.acquire("h1")
        assert not await sem.acquire("h2")
    with frozen_time(T0 + 1001):
        assert await sem.acquire("h2")


async def test_rate_limiter(cluster: Cluster) -> None:
    limiter = cluster.rate_limiter("api", parse_rate("2/s"))
    with frozen_time(T0):
        assert await limiter.acquire() == 0
        wait = await limiter.acquire()
        assert 0 < wait <= 500
    with frozen_time(T0 + 500):
        assert await limiter.acquire() == 0
    burst = cluster.rate_limiter("burst", parse_rate("1/s", burst=3))
    with frozen_time(T0):
        assert [await burst.acquire() for _ in range(4)][:3] == [0, 0, 0]


async def test_nodes_and_janitor(redis: Redis) -> None:
    alive = Cluster(redis, "test", node_id="alive")
    dead = Cluster(redis, "test", node_id="dead", dead_after_ms=1000)
    sem = dead.semaphore("s", limit=5, ttl_ms=600_000)
    with frozen_time(T0):
        await dead.nodes.beat(NodeInfo(node_id="dead", queues=["default"]))
        assert await sem.acquire(dead.holder("t1"))
        assert await sem.acquire(alive.holder("t2"))
    with frozen_time(T0 + 5000):
        await alive.nodes.beat(NodeInfo(node_id="alive"))
        janitor = Janitor(redis, dead.keys, dead.nodes)
        seen: list[str] = []

        async def hook(node_id: str) -> None:
            seen.append(node_id)

        janitor.add_dead_node_hook(hook)
        removed = await janitor.run_once()
        assert removed == ["dead"]
        assert seen == ["dead"]
        assert await sem.holders() == ["alive|t2"]
    nodes = await alive.nodes.list()
    assert [n.node_id for n in nodes] == ["alive"]


async def test_control_bus(cluster: Cluster) -> None:
    got: list[dict[str, object]] = []
    cluster.control.on("ping", got.append)
    runner = asyncio.create_task(cluster.control.run())
    await asyncio.wait_for(cluster.control.subscribed.wait(), 5)
    await cluster.control.publish("ping", value=1)
    await cluster.control.publish("other", value=2)
    for _ in range(50):
        if got:
            break
        await asyncio.sleep(0.02)
    runner.cancel()
    await asyncio.gather(runner, return_exceptions=True)
    assert got == [{"kind": "ping", "value": 1}]
