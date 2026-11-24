"""RESP2 encoding and decoding."""

import asyncio
from typing import Any, TypeAlias

from syntasks.redis.errors import (
    ProtocolError,
    RedisConnectionError,
    ResponseError,
    make_response_error,
)

EncodableT: TypeAlias = bytes | bytearray | memoryview | str | int | float

_CRLF = b"\r\n"
_SIMPLE_STRING = b"+"
_ERROR = b"-"
_INTEGER = b":"
_BULK_STRING = b"$"
_ARRAY = b"*"


def _to_bytes(value: EncodableT) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, bool):
        raise TypeError("Booleans are ambiguous in Redis commands; use 0/1.")
    if isinstance(value, int):
        return str(value).encode()
    if isinstance(value, float):
        return repr(value).encode()
    raise TypeError(f"Cannot send {type(value).__name__} to Redis.")


def encode_command(*args: EncodableT) -> bytes:
    """
    Encode one command as a RESP array of bulk strings.

    :param args: command name and arguments.
    :return: bytes to write to the socket.
    """
    parts = [b"*%d\r\n" % len(args)]
    for arg in args:
        data = _to_bytes(arg)
        parts.append(b"$%d\r\n" % len(data))
        parts.append(data)
        parts.append(_CRLF)
    return b"".join(parts)


async def _readline(reader: asyncio.StreamReader) -> bytes:
    try:
        line = await reader.readuntil(_CRLF)
    except asyncio.IncompleteReadError as exc:
        raise RedisConnectionError("Connection closed by Redis.") from exc
    except asyncio.LimitOverrunError as exc:
        raise ProtocolError("Reply line is too long.") from exc
    return line[:-2]


async def read_reply(reader: asyncio.StreamReader) -> Any:  # noqa: PLR0911
    """
    Read one reply from Redis.

    Error replies are returned (not raised) as :class:`ResponseError`
    instances, so errors nested inside arrays (``EXEC``) keep their place.

    :param reader: stream connected to Redis.
    :raises RedisConnectionError: if the connection is closed.
    :raises ProtocolError: if the reply is malformed.
    :return: decoded reply.
    """
    line = await _readline(reader)
    if not line:
        raise ProtocolError("Empty reply line.")
    prefix, payload = line[:1], line[1:]
    if prefix == _SIMPLE_STRING:
        return payload
    if prefix == _ERROR:
        return make_response_error(payload.decode(errors="replace"))
    if prefix == _INTEGER:
        return int(payload)
    if prefix == _BULK_STRING:
        length = int(payload)
        if length == -1:
            return None
        try:
            data = await reader.readexactly(length + 2)
        except asyncio.IncompleteReadError as exc:
            raise RedisConnectionError("Connection closed by Redis.") from exc
        return data[:-2]
    if prefix == _ARRAY:
        length = int(payload)
        if length == -1:
            return None
        return [await read_reply(reader) for _ in range(length)]
    raise ProtocolError(f"Unknown reply type: {line[:20]!r}")


def raise_if_error(reply: Any) -> Any:
    """
    Raise the reply if it is an error, otherwise return it.

    :param reply: decoded reply.
    :raises ResponseError: if the reply is an error.
    :return: the reply.
    """
    if isinstance(reply, ResponseError):
        raise reply
    return reply
