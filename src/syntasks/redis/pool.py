"""Connection pools for the built-in Redis client."""

import asyncio
import dataclasses
import logging
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

from syntasks.redis.connection import Connection, ConnectionOptions
from syntasks.redis.errors import (
    MasterNotFoundError,
    RedisConnectionError,
    RedisError,
    RedisTimeoutError,
)

logger = logging.getLogger(__name__)

DEFAULT_MAX_CONNECTIONS = 50
_MASTER_ROLE = b"master"


class ConnectionPool:
    """
    A bounded pool of connections to one Redis server.

    At most ``max_connections`` connections are open at a time; callers
    wait for a free one. Idle connections are reused, and connections
    broken by an error are dropped and replaced on demand.

    Calling :meth:`disconnect` closes idle connections; the pool stays
    usable and opens new connections when needed.
    """

    def __init__(
        self,
        options: ConnectionOptions | None = None,
        *,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
    ) -> None:
        if max_connections < 1:
            raise ValueError("max_connections must be at least 1.")
        self.options = options or ConnectionOptions()
        self.max_connections = max_connections
        self._idle: list[Connection] = []
        self._in_use: set[Connection] = set()
        self._semaphore: asyncio.Semaphore | None = None

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        **options: Any,
    ) -> "ConnectionPool":
        """
        Create a pool from a Redis URL.

        :param url: ``redis://``, ``rediss://`` or ``unix://`` URL.
        :param max_connections: upper bound of open connections.
        :param options: overrides for :class:`ConnectionOptions` fields.
        :return: new pool.
        """
        return cls(
            ConnectionOptions.from_url(url, **options),
            max_connections=max_connections,
        )

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.options.address} db={self.options.db}>"

    def _get_semaphore(self) -> asyncio.Semaphore:
        # Created lazily, inside a running event loop.
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_connections)
        return self._semaphore

    async def _new_connection(self) -> Connection:
        connection = Connection(self.options)
        await connection.connect()
        return connection

    async def acquire(self) -> Connection:
        """
        Take a connection from the pool, opening one if needed.

        Always pair with :meth:`release`, or use :meth:`connection`.

        :return: connected connection.
        """
        semaphore = self._get_semaphore()
        await semaphore.acquire()
        try:
            while self._idle:
                connection = self._idle.pop()
                if connection.is_connected:
                    break
                await connection.close()
            else:
                connection = await self._new_connection()
        except BaseException:
            semaphore.release()
            raise
        self._in_use.add(connection)
        return connection

    def release(self, connection: Connection) -> None:
        """
        Give a connection back to the pool.

        :param connection: connection taken with :meth:`acquire`.
        """
        if connection not in self._in_use:
            return
        self._in_use.discard(connection)
        if connection.is_connected:
            self._idle.append(connection)
        self._get_semaphore().release()

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Connection]:
        """
        Borrow a connection for the duration of the block.

        :yield: connected connection.
        """
        connection = await self.acquire()
        try:
            yield connection
        finally:
            self.release(connection)

    async def disconnect(self) -> None:
        """Close idle connections (connections in use close when released)."""
        idle, self._idle = self._idle, []
        for connection in idle:
            await connection.close()
        for connection in list(self._in_use):
            await connection.close()


class SentinelConnectionPool(ConnectionPool):
    """
    A pool that finds the current master through Redis Sentinel.

    Every new connection asks the Sentinels for the master's address
    and checks that the server really is a master, so after a failover
    the pool follows the new master as soon as old connections break.
    """

    def __init__(
        self,
        service_name: str,
        sentinels: Sequence[tuple[str, int]],
        options: ConnectionOptions | None = None,
        *,
        sentinel_options: ConnectionOptions | None = None,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
    ) -> None:
        """
        Create a Sentinel-backed pool.

        :param service_name: master name configured in Sentinel.
        :param sentinels: ``(host, port)`` of each Sentinel.
        :param options: options for connections to the master (host and
            port are ignored; they come from Sentinel).
        :param sentinel_options: options for connections to the Sentinels
            themselves, e.g. a Sentinel password.
        :param max_connections: upper bound of open connections.
        """
        if not sentinels:
            raise ValueError("At least one Sentinel address is required.")
        super().__init__(options, max_connections=max_connections)
        self.service_name = service_name
        self.sentinels = list(sentinels)
        self.sentinel_options = sentinel_options or ConnectionOptions()

    def __repr__(self) -> str:
        return f"<SentinelConnectionPool service={self.service_name!r}>"

    async def discover_master(self) -> tuple[str, int]:
        """
        Ask the Sentinels where the master is.

        Sentinels that answer are moved to the front of the list.

        :raises MasterNotFoundError: if no Sentinel knows the master.
        :return: ``(host, port)`` of the master.
        """
        errors: list[str] = []
        for index, (host, port) in enumerate(self.sentinels):
            options = dataclasses.replace(
                self.sentinel_options,
                host=host,
                port=port,
                unix_path=None,
                db=0,
            )
            connection = Connection(options)
            try:
                await connection.connect()
                reply = await connection.execute(
                    "SENTINEL",
                    "GET-MASTER-ADDR-BY-NAME",
                    self.service_name,
                )
            except (RedisError, OSError) as exc:
                errors.append(f"{host}:{port}: {exc}")
                continue
            finally:
                await connection.close()
            if reply:
                if index:
                    self.sentinels.insert(0, self.sentinels.pop(index))
                return reply[0].decode(), int(reply[1])
            errors.append(f"{host}:{port}: master unknown")
        raise MasterNotFoundError(
            f"No Sentinel knows master {self.service_name!r}: {'; '.join(errors)}",
        )

    async def _new_connection(self) -> Connection:
        host, port = await self.discover_master()
        options = dataclasses.replace(
            self.options,
            host=host,
            port=port,
            unix_path=None,
        )
        connection = Connection(options)
        await connection.connect()
        try:
            role = await connection.execute("ROLE")
        except (RedisConnectionError, RedisTimeoutError):
            await connection.close()
            raise
        if not role or role[0] != _MASTER_ROLE:
            await connection.close()
            raise RedisConnectionError(
                f"Sentinel reported {host}:{port} as master of "
                f"{self.service_name!r}, but it is not a master.",
            )
        return connection
