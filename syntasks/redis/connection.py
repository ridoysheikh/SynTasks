"""A single connection to a Redis server."""

import asyncio
import contextlib
import dataclasses
import ssl as _ssl
from collections.abc import Sequence
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from syntasks.redis.errors import (
    ProtocolError,
    RedisConnectionError,
    RedisTimeoutError,
)
from syntasks.redis.protocol import (
    EncodableT,
    encode_command,
    raise_if_error,
    read_reply,
)

DEFAULT_PORT = 6379
# Large replies (e.g. big task payloads) must fit in one StreamReader buffer.
_READER_LIMIT = 2**26

_TRUE = {"1", "true", "yes", "on"}
_SSL_CERT_REQS = {
    "none": _ssl.CERT_NONE,
    "optional": _ssl.CERT_OPTIONAL,
    "required": _ssl.CERT_REQUIRED,
}


@dataclasses.dataclass(kw_only=True)
class ConnectionOptions:
    """Everything needed to open a connection to Redis."""

    host: str = "localhost"
    port: int = DEFAULT_PORT
    unix_path: str | None = None
    db: int = 0
    username: str | None = None
    password: str | None = None
    ssl: _ssl.SSLContext | None = None
    client_name: str | None = None
    connect_timeout: float | None = 10.0
    socket_timeout: float | None = None

    @classmethod
    def from_url(  # noqa: C901, PLR0912
        cls,
        url: str,
        **overrides: Any,
    ) -> "ConnectionOptions":
        """
        Parse a Redis URL.

        Supported forms::

            redis://[[username]:password@]host[:port][/db][?options]
            rediss://...            (TLS)
            unix:///path/to/redis.sock[?db=0&password=...]

        Query options: ``db``, ``username``, ``password``, ``client_name``,
        ``connect_timeout``, ``socket_timeout``, and for ``rediss://``
        ``ssl_cert_reqs`` (none/optional/required), ``ssl_ca_certs``,
        ``ssl_certfile``, ``ssl_keyfile``, ``ssl_check_hostname``.

        Keyword arguments override values from the URL.

        :param url: Redis URL.
        :param overrides: explicit option values.
        :raises ValueError: if the URL is not a Redis URL.
        :return: parsed options.
        """
        parts = urlsplit(url)
        query = {k: v[-1] for k, v in parse_qs(parts.query).items()}
        values: dict[str, Any] = {}

        if parts.scheme == "unix":
            values["unix_path"] = unquote(parts.path)
        elif parts.scheme in {"redis", "rediss"}:
            values["host"] = unquote(parts.hostname or "localhost")
            values["port"] = parts.port or DEFAULT_PORT
            path = parts.path.lstrip("/")
            if path:
                if not path.isdigit():
                    raise ValueError(f"Invalid database number in Redis URL: {path!r}")
                values["db"] = int(path)
        else:
            raise ValueError(
                f"Unsupported Redis URL scheme {parts.scheme!r}. "
                "Use redis://, rediss:// or unix://.",
            )

        if parts.username:
            values["username"] = unquote(parts.username)
        if parts.password:
            values["password"] = unquote(parts.password)

        if "db" in query:
            values["db"] = int(query["db"])
        for key in ("username", "password", "client_name"):
            if key in query:
                values[key] = query[key]
        for key in ("connect_timeout", "socket_timeout"):
            if key in query:
                values[key] = float(query[key])

        if parts.scheme == "rediss":
            context = _ssl.create_default_context(cafile=query.get("ssl_ca_certs"))
            if "ssl_certfile" in query:
                context.load_cert_chain(query["ssl_certfile"], query.get("ssl_keyfile"))
            cert_reqs = _SSL_CERT_REQS.get(
                query.get("ssl_cert_reqs", "required").lower(),
            )
            if cert_reqs is None:
                raise ValueError("ssl_cert_reqs must be none, optional or required.")
            check_hostname = query.get("ssl_check_hostname")
            if cert_reqs == _ssl.CERT_NONE:
                context.check_hostname = False
            elif check_hostname is not None:
                context.check_hostname = check_hostname.lower() in _TRUE
            context.verify_mode = cert_reqs
            values["ssl"] = context

        values.update(overrides)
        return cls(**values)

    @property
    def address(self) -> str:
        """Human-readable address, for logs and errors."""
        if self.unix_path:
            return f"unix://{self.unix_path}"
        return f"{self.host}:{self.port}"


class Connection:
    """
    One connection to Redis.

    A connection runs one command (or one pipeline) at a time;
    the connection pool makes sure it's never shared concurrently.
    If a command fails halfway (timeout, cancellation, I/O error),
    the connection is closed, because its read position is unknown.
    """

    def __init__(self, options: ConnectionOptions) -> None:
        self.options = options
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    def __repr__(self) -> str:
        return f"<Connection {self.options.address} db={self.options.db}>"

    @property
    def is_connected(self) -> bool:
        """Whether the connection is open and usable."""
        return (
            self._reader is not None
            and self._writer is not None
            and not self._writer.is_closing()
            and not self._reader.at_eof()
        )

    async def connect(self) -> None:
        """
        Open the connection and run the handshake.

        :raises RedisConnectionError: if the server can't be reached.
        :raises RedisTimeoutError: if connecting takes too long.
        """
        if self.is_connected:
            return
        opts = self.options
        try:
            async with asyncio.timeout(opts.connect_timeout):
                if opts.unix_path:
                    self._reader, self._writer = await asyncio.open_unix_connection(
                        opts.unix_path,
                        limit=_READER_LIMIT,
                    )
                else:
                    self._reader, self._writer = await asyncio.open_connection(
                        opts.host,
                        opts.port,
                        ssl=opts.ssl,
                        limit=_READER_LIMIT,
                    )
                await self._handshake()
        except TimeoutError as exc:
            self._abort()
            raise RedisTimeoutError(f"Timed out connecting to {opts.address}.") from exc
        except OSError as exc:
            self._abort()
            raise RedisConnectionError(
                f"Cannot connect to Redis at {opts.address}: {exc}",
            ) from exc
        except BaseException:
            self._abort()
            raise

    async def _handshake(self) -> None:
        opts = self.options
        commands: list[tuple[EncodableT, ...]] = []
        if opts.password is not None:
            if opts.username is not None:
                commands.append(("AUTH", opts.username, opts.password))
            else:
                commands.append(("AUTH", opts.password))
        if opts.db:
            commands.append(("SELECT", opts.db))
        if opts.client_name:
            commands.append(("CLIENT", "SETNAME", opts.client_name))
        if commands:
            for reply in await self._run_many(commands, timeout=None):
                raise_if_error(reply)

    async def execute(
        self,
        *args: EncodableT,
        timeout: float | None = -1,
    ) -> Any:
        """
        Run one command and return its reply.

        :param args: command and arguments.
        :param timeout: seconds to wait for the reply; ``-1`` means the
            connection's ``socket_timeout``; ``None`` waits forever.
        :raises ResponseError: if Redis returns an error.
        :return: decoded reply.
        """
        (reply,) = await self._run_many([args], timeout=timeout)
        return raise_if_error(reply)

    async def execute_many(
        self,
        commands: Sequence[Sequence[EncodableT]],
        timeout: float | None = -1,
    ) -> list[Any]:
        """
        Send several commands at once and read all replies (pipelining).

        Error replies are returned in place as ``ResponseError`` objects.

        :param commands: commands to run.
        :param timeout: like in :meth:`execute`.
        :return: one reply per command.
        """
        return await self._run_many(commands, timeout=timeout)

    async def send(self, *args: EncodableT) -> None:
        """
        Send a command without waiting for its reply (used by pub/sub).

        :param args: command and arguments.
        """
        writer = self._require_writer()
        try:
            writer.write(encode_command(*args))
            await writer.drain()
        except OSError as exc:
            self._abort()
            raise RedisConnectionError(f"Lost connection to Redis: {exc}") from exc
        except BaseException:
            self._abort()
            raise

    async def read(self, timeout: float | None = None) -> Any:
        """
        Read the next reply (used by pub/sub).

        :param timeout: seconds to wait; ``None`` waits forever.
        :raises RedisTimeoutError: if nothing arrives in time.
        :return: decoded reply.
        """
        reader = self._require_reader()
        try:
            async with asyncio.timeout(timeout):
                return await read_reply(reader)
        except TimeoutError as exc:
            raise RedisTimeoutError("Timed out waiting for Redis.") from exc
        except (OSError, RedisConnectionError, ProtocolError) as exc:
            self._abort()
            if isinstance(exc, OSError):
                raise RedisConnectionError(f"Lost connection to Redis: {exc}") from exc
            raise

    async def _run_many(
        self,
        commands: Sequence[Sequence[EncodableT]],
        timeout: float | None,
    ) -> list[Any]:
        writer = self._require_writer()
        reader = self._require_reader()
        if timeout == -1:
            timeout = self.options.socket_timeout
        try:
            async with asyncio.timeout(timeout):
                writer.write(b"".join(encode_command(*cmd) for cmd in commands))
                await writer.drain()
                return [await read_reply(reader) for _ in commands]
        except TimeoutError as exc:
            self._abort()
            raise RedisTimeoutError("Timed out waiting for Redis to reply.") from exc
        except OSError as exc:
            self._abort()
            raise RedisConnectionError(f"Lost connection to Redis: {exc}") from exc
        except BaseException:
            # Cancelled or broken mid-command: the read position is unknown.
            self._abort()
            raise

    def _require_writer(self) -> asyncio.StreamWriter:
        if self._writer is None or self._writer.is_closing():
            raise RedisConnectionError("Connection is not open.")
        return self._writer

    def _require_reader(self) -> asyncio.StreamReader:
        if self._reader is None:
            raise RedisConnectionError("Connection is not open.")
        return self._reader

    def _abort(self) -> None:
        writer = self._writer
        self._reader = None
        self._writer = None
        if writer is not None:
            writer.close()

    async def close(self) -> None:
        """Close the connection."""
        writer = self._writer
        self._reader = None
        self._writer = None
        if writer is None:
            return
        writer.close()
        with contextlib.suppress(OSError, RedisConnectionError):
            await writer.wait_closed()
