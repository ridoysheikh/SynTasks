import datetime as dt

import pytest

from syntasks.units import (
    Rate,
    format_duration,
    parse_duration,
    parse_duration_ms,
    parse_rate,
    parse_size,
)


@pytest.mark.parametrize(
    ("value", "seconds"),
    [
        ("250ms", 0.25),
        ("30s", 30),
        ("5m", 300),
        ("1h30m", 5400),
        ("2d", 172800),
        ("1w", 604800),
        ("1.5s", 1.5),
        (" 1h 2m 3s ", 3723),
        (10, 10),
        (0.5, 0.5),
        ("12", 12),
        (dt.timedelta(minutes=2), 120),
    ],
)
def test_parse_duration(value: object, seconds: float) -> None:
    assert parse_duration(value) == pytest.approx(seconds)


@pytest.mark.parametrize("value", ["", "abc", "5x", "-1s", -3, True, None, [1]])
def test_parse_duration_invalid(value: object) -> None:
    with pytest.raises(ValueError):
        parse_duration(value)


def test_duration_ms_and_format() -> None:
    assert parse_duration_ms("1.5s") == 1500
    assert format_duration(5400) == "1h30m"
    assert format_duration(0.25) == "250ms"
    assert format_duration(0) == "0s"
    assert format_duration(90061.5) == "1d1h1m1s500ms"


@pytest.mark.parametrize(
    ("value", "size"),
    [
        (512, 512),
        ("512", 512),
        ("64KiB", 65536),
        ("10MB", 10_000_000),
        ("2GiB", 2 * 1024**3),
        ("1.5k", 1500),
        ("8 b", 8),
    ],
)
def test_parse_size(value: object, size: int) -> None:
    assert parse_size(value) == size


@pytest.mark.parametrize("value", ["", "lots", "5XB", -1, True])
def test_parse_size_invalid(value: object) -> None:
    with pytest.raises(ValueError):
        parse_size(value)


def test_parse_rate() -> None:
    assert parse_rate("100/s") == Rate(100, 1.0)
    assert parse_rate("5/m") == Rate(5, 60.0)
    assert parse_rate("10/30s") == Rate(10, 30.0)
    assert parse_rate("10/s", burst=5).burst == 5
    assert parse_rate("4/s").emission_ms == 250
    assert str(parse_rate("5/m")) == "5/m"


@pytest.mark.parametrize("value", ["100", "0/s", "x/s", "5/zz", 5])
def test_parse_rate_invalid(value: object) -> None:
    with pytest.raises(ValueError):
        parse_rate(value)
