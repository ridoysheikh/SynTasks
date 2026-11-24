import asyncio
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest

from syntasks import SynTasks
from syntasks.config import Config

AppFactory = Callable[..., SynTasks]

# Root options of test apps: the required limits, unbounded on purpose.
ROOT_OPTIONS: dict[str, Any] = {
    "timeout": None,
    "retries": 0,
    "max_tasks": None,
    "retry_jitter": False,
}


def fast_config(redis_url: str, name: str) -> Config:
    config = Config()
    config.app.name = name
    config.redis.url = redis_url
    config.worker.xread_block = 0.2
    config.worker.claim_idle = 2.0
    config.worker.processes = 1
    config.worker.shutdown_grace = 5.0
    config.worker.loop_block_warn = 0.0
    config.node.heartbeat = 0.5
    config.node.dead_after = 3.0
    config.scheduler.max_sleep = 0.1
    config.scheduler.lease_ttl = 1.0
    return config


@pytest.fixture
async def app_factory(redis_url: str) -> AsyncIterator[AppFactory]:
    name = f"t{uuid.uuid4().hex[:8]}"
    apps: list[SynTasks] = []

    def make(
        configure: Callable[[Config], None] | None = None,
        **root: Any,
    ) -> SynTasks:
        config = fast_config(redis_url, name)
        if configure is not None:
            configure(config)
        app = SynTasks(config, **{**ROOT_OPTIONS, **root})
        apps.append(app)
        return app

    yield make
    for app in apps:
        await app.redis.close()


@asynccontextmanager
async def running(app: SynTasks, **kwargs: Any) -> AsyncIterator[asyncio.Task[None]]:
    stop = asyncio.Event()
    task = asyncio.create_task(app.run_worker(stop, **kwargs))
    for _ in range(200):
        if task.done():
            task.result()
        if app.receiver_ready():
            break
        await asyncio.sleep(0.02)
    try:
        yield task
    finally:
        stop.set()
        await asyncio.wait_for(task, 30)


async def wait_until(predicate: Callable[[], Any], timeout: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        value = predicate()
        if asyncio.iscoroutine(value):
            value = await value
        if value:
            return
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.05)
