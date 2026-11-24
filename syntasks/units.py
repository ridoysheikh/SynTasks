"""
Human-friendly units used by the config file and task options.

* durations: ``250ms``, ``30s``, ``5m``, ``1h30m``, ``2d``, numbers (seconds),
  ``timedelta``;
* sizes: ``512``, ``64KiB``, ``10MB``, ``2GiB``;
* rates: ``100/s``, ``5/m``, ``1000/h``, ``10/30s``.
"""

import datetime as dt
import re
from dataclasses import dataclass
from typing import Any

__all__ = [
    "Rate",
    "format_duration",
    "parse_duration",
    "parse_duration_ms",
    "parse_rate",
    "parse_size",
]

_DURATION_PART = re.compile(r"(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)", re.IGNORECASE)
_DURATION_FULL = re.compile(
    r"^\s*(?:\d+(?:\.\d+)?\s*(?:ms|s|m|h|d|w)\s*)+$",
    re.IGNORECASE,
)
_DURATION_UNITS = {
    "ms": 0.001,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
    "d": 86400.0,
    "w": 604800.0,
}
_SIZE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kmgt]?i?b?)\s*$", re.IGNORECASE)
_SIZE_UNITS = {
    "": 1,
    "b": 1,
    "k": 1000,
    "kb": 1000,
    "m": 1000**2,
    "mb": 1000**2,
    "g": 1000**3,
    "gb": 1000**3,
    "t": 1000**4,
    "tb": 1000**4,
    "kib": 1024,
    "ki": 1024,
    "mib": 1024**2,
    "mi": 1024**2,
    "gib": 1024**3,
    "gi": 1024**3,
    "tib": 1024**4,
    "ti": 1024**4,
}


def parse_duration(value: Any) -> float:
    """
    Parse a duration into seconds.

    :param value: ``"1h30m"``, ``"250ms"``, a number of seconds or a timedelta.
    :raises ValueError: if the value is not a valid, non-negative duration.
    :return: seconds.
    """
    if isinstance(value, dt.timedelta):
        seconds = value.total_seconds()
    elif isinstance(value, bool):
        raise ValueError(f"Invalid duration: {value!r}")
    elif isinstance(value, (int, float)):
        seconds = float(value)
    elif isinstance(value, str):
        text = value.strip()
        try:
            seconds = float(text)
        except ValueError:
            if not _DURATION_FULL.match(text):
                raise ValueError(
                    f"Invalid duration {value!r}; use e.g. '250ms', '30s', "
                    "'5m', '1h30m', '2d'.",
                ) from None
            seconds = sum(
                float(number) * _DURATION_UNITS[unit.lower()]
                for number, unit in _DURATION_PART.findall(text)
            )
    else:
        raise ValueError(f"Invalid duration: {value!r}")
    if seconds < 0 or seconds != seconds:  # noqa: PLR0124
        raise ValueError(f"Duration must not be negative: {value!r}")
    return seconds


def parse_duration_ms(value: Any) -> int:
    """
    Parse a duration into whole milliseconds.

    :param value: any value :func:`parse_duration` accepts.
    :return: milliseconds.
    """
    return round(parse_duration(value) * 1000)


def format_duration(seconds: float) -> str:
    """
    Format seconds compactly, e.g. ``90`` -> ``"1m30s"``.

    :param seconds: duration in seconds.
    :return: formatted duration.
    """
    ms = round(seconds * 1000)
    if ms == 0:
        return "0s"
    parts = []
    for unit, size in (("d", 86_400_000), ("h", 3_600_000), ("m", 60_000)):
        count, ms = divmod(ms, size)
        if count:
            parts.append(f"{count}{unit}")
    whole, rest = divmod(ms, 1000)
    if whole:
        parts.append(f"{whole}s")
    if rest:
        parts.append(f"{rest}ms")
    return "".join(parts)


def parse_size(value: Any) -> int:
    """
    Parse a size into bytes.

    :param value: ``"64KiB"``, ``"10MB"``, ``"2GiB"`` or a number of bytes.
    :raises ValueError: if the value is not a valid size.
    :return: bytes.
    """
    if isinstance(value, bool):
        raise ValueError(f"Invalid size: {value!r}")
    if isinstance(value, int):
        size = value
    elif isinstance(value, float):
        size = int(value)
    elif isinstance(value, str):
        match = _SIZE.match(value)
        if not match or match.group(2).lower() not in _SIZE_UNITS:
            raise ValueError(
                f"Invalid size {value!r}; use e.g. '512', '64KiB', '10MB', '2GiB'.",
            )
        size = int(float(match.group(1)) * _SIZE_UNITS[match.group(2).lower()])
    else:
        raise ValueError(f"Invalid size: {value!r}")
    if size < 0:
        raise ValueError(f"Size must not be negative: {value!r}")
    return size


@dataclass(frozen=True, slots=True)
class Rate:
    """``count`` events per ``period`` seconds, allowing ``burst`` at once."""

    count: int
    period: float
    burst: int = 1

    @property
    def emission_ms(self) -> float:
        """Milliseconds between two events at the steady rate."""
        return self.period * 1000 / self.count

    def __str__(self) -> str:
        text = f"{self.count}/{format_duration(self.period)}"
        return text.replace("/1s", "/s").replace("/1m", "/m").replace("/1h", "/h")


def parse_rate(value: Any, burst: int | None = None) -> Rate:
    """
    Parse a rate such as ``"100/s"``, ``"5/m"`` or ``"10/30s"``.

    :param value: rate text or a :class:`Rate`.
    :param burst: how many events may happen at once (default 1).
    :raises ValueError: if the rate is invalid.
    :return: parsed rate.
    """
    if isinstance(value, Rate):
        rate = value
    elif isinstance(value, str) and "/" in value:
        count_text, period_text = value.split("/", 1)
        period_text = period_text.strip()
        if period_text and period_text[0].isalpha():
            period_text = "1" + period_text
        try:
            count = int(count_text.strip())
        except ValueError:
            raise ValueError(f"Invalid rate {value!r}; use e.g. '100/s'.") from None
        rate = Rate(count=count, period=parse_duration(period_text))
    else:
        raise ValueError(f"Invalid rate {value!r}; use e.g. '100/s', '5/m'.")
    if burst is not None:
        rate = Rate(rate.count, rate.period, burst)
    if rate.count <= 0 or rate.period <= 0 or rate.burst <= 0:
        raise ValueError(f"Rate must be positive: {value!r}")
    return rate
