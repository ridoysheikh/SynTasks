"""
Conversion between Python objects and plain, JSON-compatible data.

SynTasks sends task arguments, results and schedules over the wire as
plain data (dicts, lists, strings, numbers, booleans and None). This
module does both directions without third-party libraries:

* :func:`to_jsonable` turns rich Python values (dataclasses, enums,
  datetimes, UUIDs, ...) into plain data.
* :func:`convert` turns plain data back into the type a function
  annotation asks for.

Objects that follow the pydantic v2 protocol (``model_dump`` /
``model_validate``) are supported through duck typing, so user code can
keep using such models without SynTasks depending on them.
"""

import dataclasses
import datetime as dt
import decimal
import enum
import inspect
import re
import types
import typing
import uuid
from collections import abc
from functools import lru_cache
from pathlib import PurePath
from typing import Any

__all__ = [
    "ConversionError",
    "convert",
    "format_duration",
    "parse_duration",
    "to_jsonable",
]


class ConversionError(ValueError):
    """Raised when a value cannot be converted to the requested type."""


# --------------------------------------------------------------------------
# Durations (ISO 8601), compatible with the format pydantic produces.
# --------------------------------------------------------------------------

_DURATION_RE = re.compile(
    r"^(?P<sign>[-+]?)P"
    r"(?:(?P<weeks>\d+(?:\.\d+)?)W)?"
    r"(?:(?P<days>\d+(?:\.\d+)?)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+(?:\.\d+)?)H)?"
    r"(?:(?P<minutes>\d+(?:\.\d+)?)M)?"
    r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?"
    r")?$",
)
_CLOCK_RE = re.compile(
    r"^(?P<sign>[-+]?)(?:(?P<days>\d+)[dD]?[ ,]+)?"
    r"(?P<hours>\d+):(?P<minutes>\d{1,2}):(?P<seconds>\d{1,2}(?:\.\d+)?)$",
)


def format_duration(value: dt.timedelta) -> str:
    """
    Format a timedelta as an ISO 8601 duration, e.g. ``PT4H30M``.

    :param value: duration to format.
    :return: ISO 8601 duration string.
    """
    sign = "-" if value < dt.timedelta(0) else ""
    value = abs(value)
    days = value.days
    seconds = value.seconds
    micros = value.microseconds
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    out = f"{sign}P"
    if days:
        out += f"{days}D"
    time_part = ""
    if hours:
        time_part += f"{hours}H"
    if minutes:
        time_part += f"{minutes}M"
    if seconds or micros or not (days or hours or minutes):
        if micros:
            frac = f"{micros:06d}".rstrip("0")
            time_part += f"{seconds}.{frac}S"
        else:
            time_part += f"{seconds}S"
    if time_part:
        out += f"T{time_part}"
    return out


def parse_duration(value: Any) -> dt.timedelta:
    """
    Parse a duration.

    Accepts a timedelta, a number of seconds, an ISO 8601 duration
    (``PT1H30M``) or a clock string (``01:30:00``, ``1 day, 01:30:00``).

    :param value: value to parse.
    :raises ConversionError: if the value is not a duration.
    :return: parsed timedelta.
    """
    if isinstance(value, dt.timedelta):
        return value
    if isinstance(value, bool):
        raise ConversionError(f"Cannot use a boolean as a duration: {value!r}")
    if isinstance(value, (int, float)):
        return dt.timedelta(seconds=value)
    if not isinstance(value, str):
        raise ConversionError(f"Cannot parse {type(value).__name__} as a duration.")
    text = value.strip()
    try:
        return dt.timedelta(seconds=float(text))
    except ValueError:
        pass
    match = _DURATION_RE.match(text.upper())
    if match and text.upper() not in {"P", "PT", "-P", "+P", "-PT", "+PT"}:
        parts = {k: float(v) for k, v in match.groupdict().items() if v and k != "sign"}
        result = dt.timedelta(**parts)
        return -result if match.group("sign") == "-" else result
    match = _CLOCK_RE.match(text)
    if match:
        result = dt.timedelta(
            days=int(match.group("days") or 0),
            hours=int(match.group("hours")),
            minutes=int(match.group("minutes")),
            seconds=float(match.group("seconds")),
        )
        return -result if match.group("sign") == "-" else result
    raise ConversionError(f"Invalid duration: {value!r}")


# --------------------------------------------------------------------------
# Python -> plain data
# --------------------------------------------------------------------------


def _is_model_instance(value: Any) -> bool:
    return not isinstance(value, type) and callable(getattr(value, "model_dump", None))


def to_jsonable(value: Any) -> Any:  # noqa: C901, PLR0911
    """
    Convert a value to plain data that ``json.dumps`` accepts.

    Unknown objects are returned unchanged, so the serializer can still
    decide what to do with them.

    :param value: value to convert.
    :return: plain data.
    """
    if value is None or isinstance(value, (str, bool, int, float)):
        if isinstance(value, enum.Enum):
            return to_jsonable(value.value)
        return value
    if isinstance(value, enum.Enum):
        return to_jsonable(value.value)
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return format_duration(value)
    if isinstance(value, (uuid.UUID, decimal.Decimal, PurePath)):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8")
    if isinstance(value, abc.Mapping):
        return {to_jsonable(k): to_jsonable(v) for k, v in value.items()}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: to_jsonable(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    if _is_model_instance(value):
        return value.model_dump(mode="json")
    if isinstance(value, (list, tuple, set, frozenset, abc.Generator)):
        return [to_jsonable(item) for item in value]
    return value


# --------------------------------------------------------------------------
# Plain data -> Python
# --------------------------------------------------------------------------

_TRUE = {"true", "t", "yes", "y", "on", "1"}
_FALSE = {"false", "f", "no", "n", "off", "0"}
_LIST_ORIGINS = {
    list,
    abc.Sequence,
    abc.MutableSequence,
    abc.Iterable,
    abc.Collection,
}
_SET_ORIGINS = {set, abc.Set, abc.MutableSet}
_MAPPING_ORIGINS = {dict, abc.Mapping, abc.MutableMapping}


@lru_cache(maxsize=1024)
def _type_hints(cls: type) -> dict[str, Any]:
    return typing.get_type_hints(cls)


def _fail(value: Any, annotation: Any, reason: str = "") -> typing.NoReturn:
    name = getattr(annotation, "__name__", None) or repr(annotation)
    msg = f"Cannot convert {type(value).__name__} {value!r} to {name}"
    if reason:
        msg += f": {reason}"
    raise ConversionError(msg)


def _iterable_items(value: Any, annotation: Any) -> list[Any]:
    if isinstance(value, (str, bytes, bytearray, abc.Mapping)) or not isinstance(
        value,
        abc.Iterable,
    ):
        _fail(value, annotation, "expected a sequence")
    return list(value)


def _convert_union(value: Any, members: tuple[Any, ...], annotation: Any) -> Any:
    if value is None and type(None) in members:
        return None
    # Prefer a member the value already is an instance of.
    for member in members:
        if isinstance(member, type) and typing.get_origin(member) is None:
            if isinstance(value, bool) and member in {int, float}:
                continue
            if isinstance(value, member):
                return value
    errors = []
    for member in members:
        if member is type(None):
            continue
        try:
            return convert(value, member)
        except ConversionError as exc:
            errors.append(str(exc))
    _fail(value, annotation, "; ".join(errors))


def _convert_generic(  # noqa: C901, PLR0911
    value: Any,
    annotation: Any,
    origin: Any,
    args: tuple[Any, ...],
) -> Any:
    if origin in _LIST_ORIGINS:
        item_type = args[0] if args else Any
        return [convert(item, item_type) for item in _iterable_items(value, annotation)]
    if origin in _SET_ORIGINS or origin is frozenset:
        item_type = args[0] if args else Any
        converted = {
            convert(item, item_type) for item in _iterable_items(value, annotation)
        }
        return frozenset(converted) if origin is frozenset else converted
    if origin is tuple:
        items = _iterable_items(value, annotation)
        if not args:
            return tuple(items)
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(convert(item, args[0]) for item in items)
        if args == ((),):
            args = ()
        if len(items) != len(args):
            _fail(value, annotation, f"expected {len(args)} items, got {len(items)}")
        return tuple(convert(item, arg) for item, arg in zip(items, args, strict=True))
    if origin in _MAPPING_ORIGINS:
        if not isinstance(value, abc.Mapping):
            _fail(value, annotation, "expected a mapping")
        key_type, value_type = args if len(args) == 2 else (Any, Any)
        return {convert(k, key_type): convert(v, value_type) for k, v in value.items()}
    if origin is type or origin is abc.Callable:
        return value
    # A user-defined generic class, e.g. ``MyModel[int]``.
    return convert(value, origin)


def _convert_scalar(  # noqa: C901, PLR0911, PLR0912, PLR0915
    value: Any,
    annotation: type,
) -> Any:
    if annotation is bool:
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in _TRUE:
                return True
            if lowered in _FALSE:
                return False
        elif isinstance(value, (int, float)) and value in {0, 1}:
            return bool(value)
        _fail(value, annotation)
    if annotation is int:
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, (str, bytes)):
            try:
                return int(value.strip())
            except ValueError:
                _fail(value, annotation)
        if isinstance(value, decimal.Decimal) and value == value.to_integral_value():
            return int(value)
        _fail(value, annotation)
    if annotation is float:
        if isinstance(value, (int, decimal.Decimal)) and not isinstance(value, bool):
            return float(value)
        if isinstance(value, (str, bytes)):
            try:
                return float(value.strip())
            except ValueError:
                _fail(value, annotation)
        _fail(value, annotation)
    if annotation is str:
        if isinstance(value, enum.Enum) and isinstance(value.value, str):
            return value.value
        _fail(value, annotation)
    if annotation in {bytes, bytearray}:
        if isinstance(value, str):
            return annotation(value.encode("utf-8"))
        if isinstance(value, (bytes, bytearray, memoryview)):
            return annotation(value)
        _fail(value, annotation)
    if annotation is decimal.Decimal:
        if isinstance(value, (str, int, float)) and not isinstance(value, bool):
            try:
                return decimal.Decimal(str(value))
            except decimal.InvalidOperation:
                _fail(value, annotation)
        _fail(value, annotation)
    if annotation is uuid.UUID:
        try:
            if isinstance(value, str):
                return uuid.UUID(value)
            if isinstance(value, bytes) and len(value) == 16:
                return uuid.UUID(bytes=value)
        except ValueError:
            pass
        _fail(value, annotation)
    if annotation is dt.datetime:
        if isinstance(value, str):
            try:
                return dt.datetime.fromisoformat(value)
            except ValueError:
                _fail(value, annotation)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return dt.datetime.fromtimestamp(value, tz=dt.UTC)
        _fail(value, annotation)
    if annotation is dt.date:
        if isinstance(value, str):
            try:
                return dt.date.fromisoformat(value)
            except ValueError:
                _fail(value, annotation)
        _fail(value, annotation)
    if annotation is dt.time:
        if isinstance(value, str):
            try:
                return dt.time.fromisoformat(value)
            except ValueError:
                _fail(value, annotation)
        _fail(value, annotation)
    if annotation is dt.timedelta:
        return parse_duration(value)
    raise LookupError


def _convert_class(  # noqa: C901, PLR0911, PLR0912
    value: Any,
    annotation: type,
) -> Any:
    if typing.is_typeddict(annotation):
        if not isinstance(value, abc.Mapping):
            _fail(value, annotation, "expected a mapping")
        hints = _type_hints(annotation)
        return {
            key: convert(item, hints[key]) if key in hints else item
            for key, item in value.items()
        }
    if isinstance(value, annotation):
        return value
    try:
        return _convert_scalar(value, annotation)
    except LookupError:
        pass
    if issubclass(annotation, enum.Enum):
        try:
            return annotation(value)
        except ValueError:
            if isinstance(value, str) and value in annotation.__members__:
                return annotation[value]
            _fail(value, annotation)
    if issubclass(annotation, PurePath):
        if isinstance(value, str):
            return annotation(value)
        _fail(value, annotation)
    if callable(getattr(annotation, "model_validate", None)):
        # pydantic v2 compatible models.
        return annotation.model_validate(value)  # type: ignore[attr-defined]
    if dataclasses.is_dataclass(annotation):
        return _convert_fields(value, annotation, dataclasses.fields(annotation))
    if issubclass(annotation, tuple) and hasattr(annotation, "_fields"):
        hints = _type_hints(annotation)
        fields = annotation._fields
        if isinstance(value, abc.Mapping):
            data = dict(value)
        else:
            data = dict(zip(fields, _iterable_items(value, annotation), strict=False))
        try:
            return annotation(
                **{k: convert(v, hints.get(k, Any)) for k, v in data.items()},
            )
        except TypeError as exc:
            _fail(value, annotation, str(exc))
    if annotation in {list, tuple, set, frozenset, dict}:
        return _convert_generic(value, annotation, annotation, ())
    _fail(value, annotation, "unsupported type")


def _convert_fields(
    value: Any,
    annotation: type,
    fields: tuple[dataclasses.Field[Any], ...],
) -> Any:
    if not isinstance(value, abc.Mapping):
        _fail(value, annotation, "expected a mapping")
    hints = _type_hints(annotation)
    kwargs = {}
    for field in fields:
        if not field.init or field.name not in value:
            continue
        kwargs[field.name] = convert(value[field.name], hints.get(field.name, Any))
    try:
        return annotation(**kwargs)
    except TypeError as exc:
        _fail(value, annotation, str(exc))


def convert(value: Any, annotation: Any) -> Any:  # noqa: C901, PLR0911
    """
    Convert plain data to the type described by ``annotation``.

    Supports builtins, ``Optional``/unions, ``Literal``, ``Annotated``,
    generic containers, enums, datetimes, UUIDs, decimals, paths,
    dataclasses, NamedTuples, TypedDicts, and pydantic-compatible models.

    :param value: data to convert.
    :param annotation: target type.
    :raises ConversionError: if the value doesn't fit the type.
    :return: converted value.
    """
    if (
        annotation is Any
        or annotation is object
        or annotation is inspect.Parameter.empty
        or isinstance(annotation, (str, typing.TypeVar, typing.ForwardRef))
    ):
        return value
    if annotation is None or annotation is type(None):
        if value is None:
            return None
        _fail(value, annotation, "expected None")

    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is typing.Annotated:
        return convert(value, args[0])
    if origin is typing.Union or origin is types.UnionType:
        return _convert_union(value, args, annotation)
    if origin is typing.Literal:
        if value in args:
            return value
        _fail(value, annotation, f"expected one of {args}")
    if origin in {typing.ClassVar, typing.Final, typing.Required, typing.NotRequired}:
        return convert(value, args[0])
    if origin is not None:
        return _convert_generic(value, annotation, origin, args)
    if isinstance(annotation, typing.NewType):
        return convert(value, annotation.__supertype__)
    if isinstance(annotation, type):
        return _convert_class(value, annotation)
    return value
