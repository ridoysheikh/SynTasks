from typing import Any
from unittest.mock import AsyncMock

import pytest

from syntasks.exceptions import ResultIsReadyError, SynTasksResultTimeoutError
from syntasks.funcs import gather
from syntasks.task import AsyncSynTasksTask


async def test_gather() -> None:
    """Test successful task gathering."""
    rb_mock = AsyncMock()
    rb_mock.is_result_ready.return_value = True
    rb_mock.get_result.return_value = 1

    task1: AsyncSynTasksTask[Any] = AsyncSynTasksTask(
        task_id="1",
        result_backend=rb_mock,
    )
    task2: AsyncSynTasksTask[Any] = AsyncSynTasksTask(
        task_id="2",
        result_backend=rb_mock,
    )

    assert await gather(task1, task2) == (1, 1)  # type: ignore


async def test_gather_timeout() -> None:
    """Tests how gather works if timeout is reached."""
    rb_mock = AsyncMock()
    rb_mock.is_result_ready.return_value = False

    task1: AsyncSynTasksTask[Any] = AsyncSynTasksTask(
        task_id="1",
        result_backend=rb_mock,
    )
    task2: AsyncSynTasksTask[Any] = AsyncSynTasksTask(
        task_id="2",
        result_backend=rb_mock,
    )

    with pytest.raises(SynTasksResultTimeoutError):
        await gather(task1, task2, timeout=0.4)


async def test_gather_result_backend_error() -> None:
    """Test how gather works if result backend doesn't work."""
    rb_mock = AsyncMock()
    rb_mock.is_result_ready.side_effect = Exception

    task1: AsyncSynTasksTask[Any] = AsyncSynTasksTask(
        task_id="1",
        result_backend=rb_mock,
    )
    task2: AsyncSynTasksTask[Any] = AsyncSynTasksTask(
        task_id="2",
        result_backend=rb_mock,
    )

    with pytest.raises(ResultIsReadyError):
        await gather(task1, task2)
