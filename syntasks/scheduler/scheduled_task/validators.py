from datetime import timedelta
from typing import Any, TypeAlias

from syntasks.codec import ConversionError, format_duration, parse_duration

CronOffset: TypeAlias = timedelta | str
"""Offset for cron schedules: either a timedelta or a timezone name."""


def parse_cron_offset(value: Any) -> CronOffset | None:
    """
    Normalize a cron offset.

    Strings that look like durations (``PT2H``, ``01:00:00``, ``3600``)
    become timedeltas; any other string is treated as a timezone name
    (``US/Eastern``).

    :param value: raw offset.
    :raises ValueError: if the value is neither a duration nor a string.
    :return: normalized offset.
    """
    if value is None or isinstance(value, timedelta):
        return value
    if isinstance(value, str):
        try:
            return parse_duration(value)
        except ConversionError:
            return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return timedelta(seconds=value)
    raise ValueError(f"Invalid cron offset: {value!r}")


def dump_cron_offset(value: CronOffset | None) -> str | None:
    """
    Convert a cron offset to plain data.

    :param value: offset.
    :return: ISO 8601 duration, timezone name or None.
    """
    if isinstance(value, timedelta):
        return format_duration(value)
    return value


def parse_interval(value: Any) -> int | timedelta | None:
    """
    Normalize an interval value to int seconds or a timedelta.

    :param value: raw interval.
    :raises ValueError: if the value is not an interval.
    :return: normalized interval.
    """
    if value is None or isinstance(value, timedelta):
        return value
    if isinstance(value, bool):
        raise ValueError(f"Invalid interval: {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else timedelta(seconds=value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            pass
        try:
            return parse_duration(value)
        except ConversionError as exc:
            raise ValueError(f"Invalid interval: {value!r}") from exc
    raise ValueError(f"Invalid interval: {value!r}")


def validate_interval_value(
    value: int | timedelta | None,
) -> None:
    """Validate that the given interval value meets required constraints.

    :param value: The interval value to validate.
    :raises ValueError: ValueError: if value is int < 1,
        or timedelta with fractional seconds, or < 1 second.
    """
    if value is None:
        return
    if isinstance(value, int):
        if value < 1:
            raise ValueError(
                f"Interval must be at least 1 second, got {value} seconds",
            )
    else:
        # For timedelta, check that it's at least 1 second
        # and has no fractional seconds
        total_seconds = value.total_seconds()
        if total_seconds != int(total_seconds):
            raise ValueError(
                f"Fractional intervals are not supported, got {total_seconds} seconds",
            )
        if total_seconds < 1:
            raise ValueError(
                f"Interval must be at least 1 second, got {total_seconds} seconds",
            )
