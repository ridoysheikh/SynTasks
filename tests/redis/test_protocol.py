import asyncio
import ssl
from typing import Any

import pytest

from syntasks.redis import ConnectionOptions
from syntasks.redis.errors import (
    AuthenticationError,
    ProtocolError,
    RedisConnectionError,
    ResponseError,
)
from syntasks.redis.protocol import encode_command, read_reply


def _reader(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


def test_encode_command() -> None:
    assert encode_command("SET", "k", b"v\r\n", 1, 1.5) == (
        b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\nv\r\n\r\n$1\r\n1\r\n$3\r\n1.5\r\n"
    )


def test_encode_rejects_bool() -> None:
    with pytest.raises(TypeError):
        encode_command("SET", "k", True)


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        (b"+OK\r\n", b"OK"),
        (b":42\r\n", 42),
        (b"$5\r\nhello\r\n", b"hello"),
        (b"$0\r\n\r\n", b""),
        (b"$-1\r\n", None),
        (b"*-1\r\n", None),
        (b"*2\r\n$1\r\na\r\n:1\r\n", [b"a", 1]),
        (b"*1\r\n*1\r\n+x\r\n", [[b"x"]]),
    ],
)
async def test_read_reply(data: bytes, expected: Any) -> None:
    assert await read_reply(_reader(data)) == expected


async def test_read_error_reply() -> None:
    reply = await read_reply(_reader(b"-WRONGTYPE bad\r\n"))
    assert isinstance(reply, ResponseError)
    assert reply.code == "WRONGTYPE"
    auth = await read_reply(_reader(b"-WRONGPASS invalid\r\n"))
    assert isinstance(auth, AuthenticationError)


async def test_read_closed_connection() -> None:
    with pytest.raises(RedisConnectionError):
        await read_reply(_reader(b"$5\r\nhe"))


async def test_read_unknown_type() -> None:
    with pytest.raises(ProtocolError):
        await read_reply(_reader(b"?what\r\n"))


def test_url_parsing() -> None:
    opts = ConnectionOptions.from_url(
        "redis://user:p%40ss@example.com:6380/3?client_name=w&socket_timeout=2.5",
    )
    assert (opts.host, opts.port, opts.db) == ("example.com", 6380, 3)
    assert (opts.username, opts.password) == ("user", "p@ss")
    assert opts.client_name == "w"
    assert opts.socket_timeout == 2.5
    assert opts.ssl is None


def test_url_password_only() -> None:
    opts = ConnectionOptions.from_url("redis://:secret@localhost")
    assert opts.username is None
    assert opts.password == "secret"
    assert opts.port == 6379
    assert opts.db == 0


def test_url_tls() -> None:
    opts = ConnectionOptions.from_url("rediss://host?ssl_cert_reqs=none")
    assert isinstance(opts.ssl, ssl.SSLContext)
    assert opts.ssl.verify_mode == ssl.CERT_NONE


def test_url_unix() -> None:
    opts = ConnectionOptions.from_url("unix:///tmp/redis.sock?db=2")
    assert opts.unix_path == "/tmp/redis.sock"
    assert opts.db == 2


def test_url_overrides() -> None:
    opts = ConnectionOptions.from_url("redis://localhost/1", db=5, password="x")
    assert opts.db == 5
    assert opts.password == "x"


@pytest.mark.parametrize("url", ["http://localhost", "redis://localhost/abc"])
def test_url_invalid(url: str) -> None:
    with pytest.raises(ValueError):
        ConnectionOptions.from_url(url)
