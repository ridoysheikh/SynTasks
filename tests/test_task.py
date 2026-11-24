import uuid
from typing import TypeVar

import pytest
from pydantic import BaseModel

from syntasks import serializers
from syntasks.abc import AsyncResultBackend
from syntasks.abc.serializer import SynTasksSerializer
from syntasks.result import SynTasksResult
from syntasks.task import AsyncSynTasksTask

_ReturnType = TypeVar("_ReturnType")


class SerializingBackend(AsyncResultBackend[_ReturnType]):
    def __init__(self, serializer: SynTasksSerializer) -> None:
        self._serializer = serializer
        self._results: dict[str, bytes] = {}

    async def set_result(
        self,
        task_id: str,
        result: SynTasksResult[_ReturnType],  # type: ignore
    ) -> None:
        """Set result with dumping."""
        self._results[task_id] = self._serializer.dumpb(result.to_dict())

    async def is_result_ready(self, task_id: str) -> bool:
        """Check if result is ready."""
        return task_id in self._results

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> SynTasksResult[_ReturnType]:
        """Get result with loading."""
        data = self._results[task_id]
        return SynTasksResult.from_dict(self._serializer.loadb(data))


@pytest.mark.parametrize(
    "serializer",
    [
        serializers.MSGPackSerializer(),
        serializers.CBORSerializer(),
        serializers.PickleSerializer(),
        serializers.JSONSerializer(),
    ],
)
async def test_res_parsing_success(serializer: SynTasksSerializer) -> None:
    class MyResult(BaseModel):
        name: str
        age: int

    res = MyResult(name="test", age=10)
    res_back: AsyncResultBackend[MyResult] = SerializingBackend(serializer)
    test_id = str(uuid.uuid4())
    await res_back.set_result(
        test_id,
        SynTasksResult(
            is_err=False,
            return_value=res,
            execution_time=0.0,
        ),
    )
    sent_task = AsyncSynTasksTask(test_id, res_back, MyResult)
    parsed = await sent_task.wait_result()
    assert isinstance(parsed.return_value, MyResult)
