import datetime
import uuid
from typing import Any

import pytest

from syntasks.abc.serializer import SynTasksSerializer
from syntasks.serializers import (
    CBORSerializer,
    JSONSerializer,
    MSGPackSerializer,
    ORJSONSerializer,
    PickleSerializer,
)


@pytest.mark.parametrize(
    "serializer",
    [
        JSONSerializer(),
        ORJSONSerializer(),
        CBORSerializer(),
        MSGPackSerializer(),
        PickleSerializer(),
    ],
)
@pytest.mark.parametrize(
    "data",
    [
        None,
        1,
        "a",
        ["a"],
        {"a": "b"},
        1.3,
    ],
)
def test_generic_serializer(serializer: SynTasksSerializer, data: Any) -> None:
    assert serializer.loadb(serializer.dumpb(data)) == data


@pytest.mark.parametrize(
    "serializer",
    [
        ORJSONSerializer(),
        CBORSerializer(),
        PickleSerializer(),
    ],
)
def test_uuid_serialization(serializer: SynTasksSerializer) -> None:
    data = uuid.uuid4()
    assert str(serializer.loadb(serializer.dumpb(data))) == str(data)


@pytest.mark.parametrize(
    "serializer",
    [
        CBORSerializer(),
        MSGPackSerializer(),
        PickleSerializer(),
    ],
)
def test_datetime_serialization(serializer: SynTasksSerializer) -> None:
    now = datetime.datetime.now(tz=datetime.UTC)
    assert serializer.loadb(serializer.dumpb(now)) == now
