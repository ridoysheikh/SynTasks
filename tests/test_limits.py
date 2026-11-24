import json

import pytest

from syntasks.limits import (
    PayloadInvalidError,
    PayloadTooLargeError,
    check_json,
    dumps_bounded,
)


def test_dumps_bounded_ok() -> None:
    data = dumps_bounded({"a": [1, 2.5, "x", None, True]}, max_bytes=100)
    assert json.loads(data) == {"a": [1, 2.5, "x", None, True]}
    assert b" " not in data


def test_too_large() -> None:
    with pytest.raises(PayloadTooLargeError, match="limit is 10 bytes"):
        dumps_bounded({"text": "x" * 20}, max_bytes=10)


@pytest.mark.parametrize(
    ("value", "where"),
    [
        ({"a": {1: 2}}, ".a"),
        ({"a": [1, float("nan")]}, ".a[1]"),
        ({"a": {"b": object()}}, ".a.b"),
        ({"when": b"bytes"}, ".when"),
    ],
)
def test_invalid(value: object, where: str) -> None:
    with pytest.raises(PayloadInvalidError) as info:
        check_json(value)
    assert where in str(info.value)


def test_depth() -> None:
    nested: object = 1
    for _ in range(40):
        nested = [nested]
    with pytest.raises(PayloadInvalidError, match="deeper"):
        check_json(nested, max_depth=32)
    check_json(nested, max_depth=64)
