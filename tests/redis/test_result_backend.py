import asyncio
import uuid
from dataclasses import dataclass

import pytest

from syntasks import RedisResultBackend, SynTasksResult
from syntasks.depends.progress_tracker import TaskProgress, TaskState
from syntasks.exceptions import ResultIsMissingError
from syntasks.serializers import PickleSerializer


@dataclass
class Payload:
    name: str
    count: int


def _result(
    value: object,
    error: BaseException | None = None,
) -> SynTasksResult[object]:
    return SynTasksResult(
        is_err=error is not None,
        return_value=value,
        execution_time=0.5,
        labels={"x": 1},
        error=error,
        log="log",
    )


async def test_roundtrip(redis_url: str) -> None:
    backend: RedisResultBackend[object] = RedisResultBackend(redis_url)
    task_id = uuid.uuid4().hex
    assert not await backend.is_result_ready(task_id)
    await backend.set_result(task_id, _result(Payload("a", 1)))
    assert await backend.is_result_ready(task_id)
    result = await backend.get_result(task_id)
    assert result.return_value == {"name": "a", "count": 1}
    assert result.labels == {"x": 1}
    assert result.log is None
    assert (await backend.get_result(task_id, with_logs=True)).log == "log"
    await backend.shutdown()


async def test_error_roundtrip(redis_url: str) -> None:
    backend: RedisResultBackend[object] = RedisResultBackend(redis_url)
    await backend.set_result("err", _result(None, KeyError("missing")))
    result = await backend.get_result("err")
    assert isinstance(result.error, KeyError)
    with pytest.raises(KeyError):
        result.raise_for_error()
    await backend.shutdown()


async def test_pickle_serializer(redis_url: str) -> None:
    backend: RedisResultBackend[object] = RedisResultBackend(
        redis_url,
        serializer=PickleSerializer(),
    )
    await backend.set_result("p", _result([1, 2]))
    assert (await backend.get_result("p")).return_value == [1, 2]
    await backend.shutdown()


async def test_missing_result(redis_url: str) -> None:
    backend: RedisResultBackend[object] = RedisResultBackend(redis_url)
    with pytest.raises(ResultIsMissingError):
        await backend.get_result("nope")
    await backend.shutdown()


async def test_keep_results_false(redis_url: str) -> None:
    backend: RedisResultBackend[object] = RedisResultBackend(
        redis_url,
        keep_results=False,
    )
    await backend.set_result("once", _result(1))
    assert (await backend.get_result("once")).return_value == 1
    assert not await backend.is_result_ready("once")
    await backend.shutdown()


async def test_expiry(redis_url: str) -> None:
    backend: RedisResultBackend[object] = RedisResultBackend(
        redis_url,
        result_px_time=100,
    )
    await backend.set_result("exp", _result(1))
    await backend.set_progress("exp", TaskProgress(state=TaskState.STARTED, meta=None))
    await asyncio.sleep(0.3)
    assert not await backend.is_result_ready("exp")
    assert await backend.get_progress("exp") is None
    await backend.shutdown()


async def test_progress(redis_url: str) -> None:
    backend: RedisResultBackend[object] = RedisResultBackend(redis_url)
    assert await backend.get_progress("t") is None
    await backend.set_progress(
        "t",
        TaskProgress(state=TaskState.STARTED, meta={"done": 3}),
    )
    progress = await backend.get_progress("t")
    assert progress is not None
    assert progress.state is TaskState.STARTED
    assert progress.meta == {"done": 3}
    await backend.set_progress("t", TaskProgress(state="custom", meta=None))
    progress = await backend.get_progress("t")
    assert progress is not None
    assert progress.state == "custom"
    await backend.shutdown()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"result_ex_time": 1, "result_px_time": 1},
        {"result_ex_time": 0},
        {"result_px_time": -1},
    ],
)
def test_invalid_expiry(kwargs: dict[str, int]) -> None:
    with pytest.raises(ValueError):
        RedisResultBackend(**kwargs)  # type: ignore[arg-type]
