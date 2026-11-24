import asyncio

import pytest

from syntasks import Depends
from syntasks.depends.progress_tracker import ProgressTracker, TaskState
from syntasks.exceptions import SynTasksResultTimeoutError

from .conftest import AppFactory, running


async def test_group_wait_first_and_as_completed(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.task
    async def square(n: int) -> int:
        await asyncio.sleep(0.05 * (5 - n))
        return n * n

    async with running(app):
        group = app.group()
        for n in range(5):
            await group.kiq(square, n)
        results = await group.wait(timeout=10)
        assert [r.return_value for r in results] == [0, 1, 4, 9, 16]
        second = app.group()
        for n in range(5):
            await second.kiq(square, n)
        first = await second.first(2, timeout=10)
        assert [r.return_value for r in first] == [16, 9]
        order = [
            result.return_value async for _, result in second.as_completed(timeout=10)
        ]
        assert sorted(order) == [0, 1, 4, 9, 16]


async def test_wait_result_is_push_based_and_times_out(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.task
    async def slow() -> str:
        await asyncio.sleep(0.3)
        return "done"

    task = await slow.kiq()
    with pytest.raises(SynTasksResultTimeoutError):
        await task.wait_result(timeout=0.2)
    async with running(app):
        result = await task.wait_result(timeout=10)
    assert result.return_value == "done"


async def test_watch_progress(app_factory: AppFactory) -> None:
    app = app_factory()

    @app.task
    async def work(progress: ProgressTracker[int] = Depends()) -> None:
        for step in range(3):
            await progress.set_progress(TaskState.STARTED, step)
            await asyncio.sleep(0.05)

    async with running(app):
        task = await work.kiq()
        updates = [p.meta async for p in task.watch_progress(timeout=10)]
    assert updates == [0, 1, 2]
