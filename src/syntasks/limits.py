"""
Bounded JSON: every payload that crosses the network has a size limit.

Used for task messages, schedule arguments and channel messages.
"""

import json
import math
from typing import Any

from syntasks.exceptions import SynTasksError

__all__ = [
    "DEFAULT_MAX_DEPTH",
    "PayloadError",
    "PayloadInvalidError",
    "PayloadTooLargeError",
    "check_json",
    "dumps_bounded",
]

DEFAULT_MAX_DEPTH = 32


class PayloadError(SynTasksError, ValueError):
    """A payload can't be sent."""

    __template__ = "{detail}"
    detail: str = "Invalid payload"


class PayloadTooLargeError(PayloadError):
    """A payload is bigger than allowed."""


class PayloadInvalidError(PayloadError):
    """A payload is not plain JSON."""


def _path(parts: list[str]) -> str:
    return "".join(parts) or "<root>"


def check_json(value: Any, *, max_depth: int = DEFAULT_MAX_DEPTH) -> None:  # noqa: C901
    """
    Check that a value is plain JSON.

    Allowed: dict with str keys, list, tuple, str, int, float (finite),
    bool and None, nested at most ``max_depth`` levels.

    :param value: value to check.
    :param max_depth: maximum nesting.
    :raises PayloadInvalidError: naming the path of the first bad value.
    """
    stack: list[tuple[Any, list[str], int]] = [(value, [], 0)]
    while stack:
        item, path, depth = stack.pop()
        if depth > max_depth:
            raise PayloadInvalidError(
                detail=f"Payload is nested deeper than {max_depth} at {_path(path)}.",
            )
        if item is None or isinstance(item, (str, bool, int)):
            continue
        if isinstance(item, float):
            if not math.isfinite(item):
                raise PayloadInvalidError(
                    detail=f"Payload has a non-finite number at {_path(path)}.",
                )
            continue
        if isinstance(item, dict):
            for key, sub in item.items():
                if not isinstance(key, str):
                    raise PayloadInvalidError(
                        detail=f"Payload has a non-string key {key!r} "
                        f"at {_path(path)}.",
                    )
                stack.append((sub, [*path, f".{key}"], depth + 1))
            continue
        if isinstance(item, (list, tuple)):
            for index, sub in enumerate(item):
                stack.append((sub, [*path, f"[{index}]"], depth + 1))
            continue
        raise PayloadInvalidError(
            detail=f"Payload has a {type(item).__name__} at {_path(path)}, "
            "which is not JSON.",
        )


def dumps_bounded(
    value: Any,
    *,
    max_bytes: int | None,
    max_depth: int = DEFAULT_MAX_DEPTH,
    what: str = "Payload",
) -> bytes:
    """
    Encode a value as compact JSON, enforcing structure and size.

    :param value: plain JSON value.
    :param max_bytes: size limit; None for no limit.
    :param max_depth: maximum nesting.
    :param what: name used in error messages.
    :raises PayloadInvalidError: if the value is not plain JSON.
    :raises PayloadTooLargeError: if the encoded value is too big.
    :return: UTF-8 JSON.
    """
    check_json(value, max_depth=max_depth)
    data = json.dumps(
        value,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()
    check_size(data, max_bytes=max_bytes, what=what)
    return data


def check_size(data: bytes, *, max_bytes: int | None, what: str = "Payload") -> None:
    """
    Check an encoded payload's size.

    :param data: encoded payload.
    :param max_bytes: size limit; None for no limit.
    :param what: name used in error messages.
    :raises PayloadTooLargeError: if it is too big.
    """
    if max_bytes is not None and len(data) > max_bytes:
        raise PayloadTooLargeError(
            detail=f"{what} is {len(data)} bytes; the limit is {max_bytes} bytes.",
        )
