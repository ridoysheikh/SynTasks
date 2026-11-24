import dataclasses
import datetime as dt
import decimal
import enum
import uuid
from pathlib import Path
from typing import Annotated, Any, Literal, NamedTuple, TypedDict

import pytest

from syntasks.codec import (
    ConversionError,
    convert,
    format_duration,
    parse_duration,
    to_jsonable,
)


class Color(enum.Enum):
    RED = "red"
    GREEN = "green"


class Level(enum.IntEnum):
    LOW = 1
    HIGH = 2


@dataclasses.dataclass
class Point:
    x: int
    y: int


@dataclasses.dataclass
class Shape:
    name: str
    points: list[Point]
    color: Color = Color.RED


class Pair(NamedTuple):
    left: int
    right: str


class Movie(TypedDict):
    title: str
    year: int


@pytest.mark.parametrize(
    ("value", "annotation", "expected"),
    [
        ("5", int, 5),
        (5.0, int, 5),
        ("2.5", float, 2.5),
        (3, float, 3.0),
        ("true", bool, True),
        ("off", bool, False),
        (1, bool, True),
        ("abc", bytes, b"abc"),
        ("1.10", decimal.Decimal, decimal.Decimal("1.10")),
        ("red", Color, Color.RED),
        ("GREEN", Color, Color.GREEN),
        (2, Level, Level.HIGH),
        ("/tmp/x", Path, Path("/tmp/x")),
        ("2024-01-02", dt.date, dt.date(2024, 1, 2)),
        ("12:30:00", dt.time, dt.time(12, 30)),
        (
            "2024-01-02T03:04:05+00:00",
            dt.datetime,
            dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.UTC),
        ),
        ("PT1H30M", dt.timedelta, dt.timedelta(hours=1, minutes=30)),
        (90, dt.timedelta, dt.timedelta(seconds=90)),
        (["1", "2"], list[int], [1, 2]),
        (["1", "2"], tuple[int, ...], (1, 2)),
        ([1, "a"], tuple[int, str], (1, "a")),
        ([1, 1, 2], set[int], {1, 2}),
        ({"a": "1"}, dict[str, int], {"a": 1}),
        (None, int | None, None),
        ("5", int | None, 5),
        ("x", int | str, "x"),
        ("a", Literal["a", "b"], "a"),
        ("5", Annotated[int, "meta"], 5),
        ({"x": "1", "y": 2}, Point, Point(1, 2)),
        (
            {"name": "s", "points": [{"x": 1, "y": 2}], "color": "green"},
            Shape,
            Shape("s", [Point(1, 2)], Color.GREEN),
        ),
        ([1, "a"], Pair, Pair(1, "a")),
        ({"title": "t", "year": "1999"}, Movie, {"title": "t", "year": 1999}),
        ({"any": object}, Any, {"any": object}),
    ],
)
def test_convert(value: Any, annotation: Any, expected: Any) -> None:
    assert convert(value, annotation) == expected


def test_convert_uuid() -> None:
    value = uuid.uuid4()
    assert convert(str(value), uuid.UUID) == value


@pytest.mark.parametrize(
    ("value", "annotation"),
    [
        ("abc", int),
        (1, str),
        ("maybe", bool),
        ("purple", Color),
        ([1, 2, 3], tuple[int, int]),
        ("b", Literal["a"]),
        ({"x": 1}, Point),
        ("1", list[int]),
        (1, None),
    ],
)
def test_convert_fails(value: Any, annotation: Any) -> None:
    with pytest.raises(ConversionError):
        convert(value, annotation)


def test_to_jsonable_roundtrip() -> None:
    shape = Shape("s", [Point(1, 2)], Color.GREEN)
    data = {
        "shape": shape,
        "when": dt.datetime(2024, 1, 2, 3, 4, 5),
        "delta": dt.timedelta(minutes=5),
        "id": uuid.UUID(int=1),
        "money": decimal.Decimal("1.5"),
        "tags": {"a"},
        "raw": b"bytes",
    }
    plain = to_jsonable(data)
    assert plain == {
        "shape": {"name": "s", "points": [{"x": 1, "y": 2}], "color": "green"},
        "when": "2024-01-02T03:04:05",
        "delta": "PT5M",
        "id": "00000000-0000-0000-0000-000000000001",
        "money": "1.5",
        "tags": ["a"],
        "raw": "bytes",
    }
    assert convert(plain["shape"], Shape) == shape


@pytest.mark.parametrize(
    ("delta", "text"),
    [
        (dt.timedelta(0), "PT0S"),
        (dt.timedelta(hours=4), "PT4H"),
        (dt.timedelta(days=1, seconds=1), "P1DT1S"),
        (dt.timedelta(seconds=1.5), "PT1.5S"),
        (-dt.timedelta(minutes=2), "-PT2M"),
    ],
)
def test_duration_roundtrip(delta: dt.timedelta, text: str) -> None:
    assert format_duration(delta) == text
    assert parse_duration(text) == delta


@pytest.mark.parametrize("value", ["", "P", "PT", "1 hour", "US/Eastern", True])
def test_parse_duration_invalid(value: Any) -> None:
    with pytest.raises(ConversionError):
        parse_duration(value)


def test_pydantic_compatible_models() -> None:
    pydantic = pytest.importorskip("pydantic")

    class Model(pydantic.BaseModel):  # type: ignore[name-defined,misc]
        number: int

    assert convert({"number": "3"}, Model) == Model(number=3)
    assert to_jsonable(Model(number=3)) == {"number": 3}
