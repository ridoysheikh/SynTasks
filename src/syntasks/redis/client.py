"""High-level async Redis client."""

import asyncio
import builtins
import contextlib
from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

from syntasks.redis.connection import Connection
from syntasks.redis.errors import ResponseError
from syntasks.redis.pool import ConnectionPool
from syntasks.redis.protocol import EncodableT

_PONG = b"PONG"
_SCAN_DONE = b"0"
_MESSAGE = b"message"
_PATTERN_MESSAGE = b"pmessage"

# Extra seconds a blocking command may take on top of its own timeout.
_BLOCKING_GRACE = 10.0


def _pairs_to_dict(items: Sequence[Any]) -> dict[Any, Any]:
    return dict(zip(items[::2], items[1::2], strict=True))


class Redis:
    """
    Async Redis client built on the standard library.

    Every command borrows a connection from the pool for its duration.
    Replies are returned as Redis sends them: bulk strings as ``bytes``,
    integers as ``int``, arrays as ``list``.

    Example::

        redis = Redis.from_url("redis://localhost:6379/0")
        await redis.set("key", "value", ex=60)
        assert await redis.get("key") == b"value"
        await redis.close()
    """

    def __init__(self, pool: ConnectionPool | None = None) -> None:
        self.pool = pool or ConnectionPool()

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> "Redis":
        """
        Create a client from a Redis URL.

        :param url: ``redis://``, ``rediss://`` or ``unix://`` URL.
        :param kwargs: ``max_connections`` and connection option overrides.
        :return: new client.
        """
        return cls(ConnectionPool.from_url(url, **kwargs))

    def __repr__(self) -> str:
        return f"<Redis {self.pool!r}>"

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the pool's connections. The client can still be used later."""
        await self.pool.disconnect()

    async def execute(self, *args: EncodableT, timeout: float | None = -1) -> Any:
        """
        Run any Redis command.

        :param args: command name and arguments, e.g. ``("SET", "k", "v")``.
        :param timeout: seconds to wait for the reply; ``-1`` uses the
            connection's ``socket_timeout``, ``None`` waits forever.
        :raises ResponseError: if Redis answers with an error.
        :return: the reply.
        """
        async with self.pool.connection() as connection:
            return await connection.execute(*args, timeout=timeout)

    def pipeline(self, transaction: bool = False) -> "Pipeline":
        """
        Batch several commands into one round trip.

        :param transaction: wrap the commands in MULTI/EXEC.
        :return: a pipeline; add commands and ``await pipe.execute()``.
        """
        return Pipeline(self, transaction=transaction)

    def pubsub(self) -> "PubSub":
        """
        Open a pub/sub session on its own connection.

        :return: pub/sub session.
        """
        return PubSub(self.pool)

    # -- server ----------------------------------------------------------

    async def ping(self) -> bool:
        """Check the server answers."""
        return await self.execute("PING") == _PONG

    async def flushdb(self) -> None:
        """Delete every key of the current database."""
        await self.execute("FLUSHDB")

    async def info(self, section: str | None = None) -> dict[str, str]:
        """
        Read ``INFO`` as a dict of strings.

        :param section: optional INFO section.
        :return: field -> value.
        """
        raw = await self.execute("INFO", *([section] if section else []))
        result = {}
        for line in raw.decode().splitlines():
            if line and not line.startswith("#") and ":" in line:
                key, value = line.split(":", 1)
                result[key] = value
        return result

    # -- keys and strings -------------------------------------------------

    async def get(self, key: EncodableT) -> bytes | None:
        """Get a string value."""
        return await self.execute("GET", key)

    async def getdel(self, key: EncodableT) -> bytes | None:
        """Get a string value and delete the key (Redis 6.2+)."""
        return await self.execute("GETDEL", key)

    async def mget(self, keys: Iterable[EncodableT]) -> list[bytes | None]:
        """Get several string values."""
        keys = list(keys)
        if not keys:
            return []
        return await self.execute("MGET", *keys)

    async def set(
        self,
        key: EncodableT,
        value: EncodableT,
        *,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
    ) -> bool:
        """
        Set a string value.

        :param key: key.
        :param value: value.
        :param ex: expire after this many seconds.
        :param px: expire after this many milliseconds.
        :param nx: only set if the key doesn't exist.
        :param xx: only set if the key exists.
        :param keepttl: keep the existing TTL.
        :return: True if the value was set.
        """
        args: list[EncodableT] = ["SET", key, value]
        if ex is not None:
            args += ["EX", ex]
        if px is not None:
            args += ["PX", px]
        if nx:
            args.append("NX")
        if xx:
            args.append("XX")
        if keepttl:
            args.append("KEEPTTL")
        return await self.execute(*args) is not None

    async def delete(self, *keys: EncodableT) -> int:
        """Delete keys; returns how many existed."""
        if not keys:
            return 0
        return await self.execute("DEL", *keys)

    async def exists(self, *keys: EncodableT) -> int:
        """Count how many of the keys exist."""
        return await self.execute("EXISTS", *keys)

    async def expire(self, key: EncodableT, seconds: int) -> bool:
        """Set a TTL in seconds."""
        return await self.execute("EXPIRE", key, seconds) == 1

    async def pexpire(self, key: EncodableT, milliseconds: int) -> bool:
        """Set a TTL in milliseconds."""
        return await self.execute("PEXPIRE", key, milliseconds) == 1

    async def ttl(self, key: EncodableT) -> int:
        """Remaining TTL in seconds (-1 no TTL, -2 no key)."""
        return await self.execute("TTL", key)

    async def incr(self, key: EncodableT, amount: int = 1) -> int:
        """Increment an integer value."""
        return await self.execute("INCRBY", key, amount)

    async def scan_iter(
        self,
        match: str | None = None,
        count: int | None = None,
    ) -> AsyncIterator[bytes]:
        """
        Iterate over keys with SCAN.

        :param match: glob pattern.
        :param count: hint for keys per round trip.
        :yield: keys.
        """
        cursor = _SCAN_DONE
        while True:
            args: list[EncodableT] = ["SCAN", cursor]
            if match is not None:
                args += ["MATCH", match]
            if count is not None:
                args += ["COUNT", count]
            cursor, keys = await self.execute(*args)
            for key in keys:
                yield key
            if cursor == _SCAN_DONE:
                return

    # -- lists -------------------------------------------------------------

    async def lpush(self, key: EncodableT, *values: EncodableT) -> int:
        """Push to the head of a list."""
        return await self.execute("LPUSH", key, *values)

    async def rpush(self, key: EncodableT, *values: EncodableT) -> int:
        """Push to the tail of a list."""
        return await self.execute("RPUSH", key, *values)

    async def llen(self, key: EncodableT) -> int:
        """Length of a list."""
        return await self.execute("LLEN", key)

    async def lrange(self, key: EncodableT, start: int, stop: int) -> list[bytes]:
        """Range of list elements."""
        return await self.execute("LRANGE", key, start, stop)

    async def brpop(
        self,
        keys: EncodableT | Sequence[EncodableT],
        timeout: float = 0,
    ) -> tuple[bytes, bytes] | None:
        """
        Pop from the tail of the first non-empty list, waiting if needed.

        :param keys: one key or several keys.
        :param timeout: seconds to wait; 0 waits forever.
        :return: ``(key, value)`` or None on timeout.
        """
        key_list: list[EncodableT] = (
            list(keys)
            if isinstance(keys, Sequence) and not isinstance(keys, (str, bytes))
            else [keys]
        )
        reply = await self.execute(
            "BRPOP",
            *key_list,
            timeout,
            timeout=None if timeout == 0 else timeout + _BLOCKING_GRACE,
        )
        return None if reply is None else (reply[0], reply[1])

    # -- hashes ------------------------------------------------------------

    async def hset(self, key: EncodableT, mapping: dict[Any, Any]) -> int:
        """Set hash fields; returns how many were new."""
        args: list[EncodableT] = []
        for field, value in mapping.items():
            args += [field, value]
        return await self.execute("HSET", key, *args)

    async def hget(self, key: EncodableT, field: EncodableT) -> bytes | None:
        """Get one hash field."""
        return await self.execute("HGET", key, field)

    async def hdel(self, key: EncodableT, *fields: EncodableT) -> int:
        """Delete hash fields."""
        return await self.execute("HDEL", key, *fields)

    async def hgetall(self, key: EncodableT) -> dict[bytes, bytes]:
        """Get every field of a hash."""
        return _pairs_to_dict(await self.execute("HGETALL", key))

    async def hlen(self, key: EncodableT) -> int:
        """Number of hash fields."""
        return await self.execute("HLEN", key)

    async def hscan_iter(
        self,
        key: EncodableT,
        count: int | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        """
        Iterate over a hash with HSCAN.

        :param key: hash key.
        :param count: hint for fields per round trip.
        :yield: ``(field, value)`` pairs.
        """
        cursor = _SCAN_DONE
        while True:
            args: list[EncodableT] = ["HSCAN", key, cursor]
            if count is not None:
                args += ["COUNT", count]
            cursor, items = await self.execute(*args)
            for field, value in zip(items[::2], items[1::2], strict=True):
                yield field, value
            if cursor == _SCAN_DONE:
                return

    # -- sorted sets ---------------------------------------------------------

    async def zadd(self, key: EncodableT, mapping: dict[Any, float]) -> int:
        """Add members with scores."""
        args: list[EncodableT] = []
        for member, score in mapping.items():
            args += [score, member]
        return await self.execute("ZADD", key, *args)

    async def zrem(self, key: EncodableT, *members: EncodableT) -> int:
        """Remove members."""
        return await self.execute("ZREM", key, *members)

    async def zcard(self, key: EncodableT) -> int:
        """Number of members."""
        return await self.execute("ZCARD", key)

    async def zrangebyscore(
        self,
        key: EncodableT,
        minimum: float | str,
        maximum: float | str,
        *,
        offset: int | None = None,
        count: int | None = None,
    ) -> list[bytes]:
        """Members with scores in a range."""
        args: list[EncodableT] = ["ZRANGEBYSCORE", key, minimum, maximum]
        if offset is not None and count is not None:
            args += ["LIMIT", offset, count]
        return await self.execute(*args)

    # -- streams -------------------------------------------------------------

    async def xadd(
        self,
        key: EncodableT,
        fields: dict[Any, Any],
        *,
        maxlen: int | None = None,
        approximate: bool = True,
        entry_id: EncodableT = "*",
    ) -> bytes:
        """
        Append an entry to a stream.

        :param key: stream key.
        :param fields: entry fields.
        :param maxlen: trim the stream to about this many entries.
        :param approximate: allow Redis to trim lazily (much cheaper).
        :param entry_id: entry id, ``*`` to let Redis pick.
        :return: id of the new entry.
        """
        args: list[EncodableT] = ["XADD", key]
        if maxlen is not None:
            args += ["MAXLEN", "~" if approximate else "=", maxlen]
        args.append(entry_id)
        for field, value in fields.items():
            args += [field, value]
        return await self.execute(*args)

    async def xlen(self, key: EncodableT) -> int:
        """Number of stream entries."""
        return await self.execute("XLEN", key)

    async def xgroup_create(
        self,
        key: EncodableT,
        group: EncodableT,
        entry_id: EncodableT = "$",
        *,
        mkstream: bool = True,
        exist_ok: bool = True,
    ) -> bool:
        """
        Create a consumer group.

        :param key: stream key.
        :param group: group name.
        :param entry_id: where the group starts reading; ``$`` = new entries.
        :param mkstream: create the stream if it doesn't exist.
        :param exist_ok: don't fail if the group already exists.
        :return: True if the group was created.
        """
        args: list[EncodableT] = ["XGROUP", "CREATE", key, group, entry_id]
        if mkstream:
            args.append("MKSTREAM")
        try:
            await self.execute(*args)
        except ResponseError as exc:
            if exist_ok and exc.code == "BUSYGROUP":
                return False
            raise
        return True

    async def xreadgroup(
        self,
        group: EncodableT,
        consumer: EncodableT,
        streams: dict[Any, Any],
        *,
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]]]]:
        """
        Read entries as a member of a consumer group.

        :param group: group name.
        :param consumer: consumer name.
        :param streams: stream key -> id (``>`` for new entries).
        :param count: max entries per stream.
        :param block: milliseconds to wait; None doesn't block.
        :param noack: don't add entries to the pending list.
        :return: ``[(stream, [(entry_id, fields), ...]), ...]``.
        """
        args: list[EncodableT] = ["XREADGROUP", "GROUP", group, consumer]
        if count is not None:
            args += ["COUNT", count]
        if block is not None:
            args += ["BLOCK", block]
        if noack:
            args.append("NOACK")
        args.append("STREAMS")
        args += list(streams.keys())
        args += list(streams.values())
        timeout: float | None = -1
        if block is not None:
            timeout = None if block == 0 else block / 1000 + _BLOCKING_GRACE
        reply = await self.execute(*args, timeout=timeout)
        if reply is None:
            return []
        return [
            (
                stream,
                [
                    (entry_id, _pairs_to_dict(fields) if fields else {})
                    for entry_id, fields in entries
                ],
            )
            for stream, entries in reply
        ]

    async def xack(self, key: EncodableT, group: EncodableT, *ids: EncodableT) -> int:
        """Acknowledge entries of a consumer group."""
        return await self.execute("XACK", key, group, *ids)

    async def xautoclaim(
        self,
        key: EncodableT,
        group: EncodableT,
        consumer: EncodableT,
        min_idle_time: int,
        start: EncodableT = "0-0",
        *,
        count: int | None = None,
    ) -> tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]]]:
        """
        Take over entries that other consumers left unacknowledged (6.2+).

        :param key: stream key.
        :param group: group name.
        :param consumer: consumer that takes the entries.
        :param min_idle_time: only entries idle for this many milliseconds.
        :param start: id to start scanning from.
        :param count: max entries to claim.
        :return: ``(next_start, [(entry_id, fields), ...])``; deleted
            entries are skipped.
        """
        args: list[EncodableT] = [
            "XAUTOCLAIM",
            key,
            group,
            consumer,
            min_idle_time,
            start,
        ]
        if count is not None:
            args += ["COUNT", count]
        reply = await self.execute(*args)
        entries = [
            (entry_id, _pairs_to_dict(fields))
            for entry_id, fields in reply[1]
            if fields is not None
        ]
        return reply[0], entries

    async def xpending_count(self, key: EncodableT, group: EncodableT) -> int:
        """Number of entries delivered to the group but not acknowledged."""
        reply = await self.execute("XPENDING", key, group)
        return int(reply[0])

    # -- pub/sub and scripting -----------------------------------------------

    async def publish(self, channel: EncodableT, message: EncodableT) -> int:
        """Publish a message; returns the number of receivers."""
        return await self.execute("PUBLISH", channel, message)

    async def eval(
        self,
        script: str,
        keys: Sequence[EncodableT] = (),
        args: Sequence[EncodableT] = (),
    ) -> Any:
        """Run a Lua script."""
        return await self.execute("EVAL", script, len(keys), *keys, *args)

    async def evalsha(
        self,
        sha: str,
        keys: Sequence[EncodableT] = (),
        args: Sequence[EncodableT] = (),
    ) -> Any:
        """Run a Lua script loaded with :meth:`script_load`."""
        return await self.execute("EVALSHA", sha, len(keys), *keys, *args)

    async def script_load(self, script: str) -> str:
        """Load a Lua script into the script cache; returns its SHA1."""
        sha = await self.execute("SCRIPT", "LOAD", script)
        return sha.decode() if isinstance(sha, bytes) else str(sha)

    async def time(self) -> float:
        """Server time as Unix seconds (with microseconds)."""
        seconds, micros = await self.execute("TIME")
        return int(seconds) + int(micros) / 1_000_000

    # -- generic helpers used by the cluster layer ----------------------------

    async def blpop(
        self,
        keys: EncodableT | Sequence[EncodableT],
        timeout: float = 0,
    ) -> tuple[bytes, bytes] | None:
        """
        Pop from the head of the first non-empty list, waiting if needed.

        :param keys: one key or several keys.
        :param timeout: seconds to wait; 0 waits forever.
        :return: ``(key, value)`` or None on timeout.
        """
        key_list: list[EncodableT] = (
            list(keys)
            if isinstance(keys, Sequence) and not isinstance(keys, (str, bytes))
            else [keys]
        )
        reply = await self.execute(
            "BLPOP",
            *key_list,
            timeout,
            timeout=None if timeout == 0 else timeout + _BLOCKING_GRACE,
        )
        return None if reply is None else (reply[0], reply[1])

    async def xread(
        self,
        streams: dict[Any, Any],
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]]]]:
        """
        Read entries from one or more streams (no consumer group).

        :param streams: stream key -> last seen id (``$`` for new only).
        :param count: max entries per stream.
        :param block: milliseconds to wait; None doesn't block.
        :return: ``[(stream, [(entry_id, fields), ...]), ...]``.
        """
        args: list[EncodableT] = ["XREAD"]
        if count is not None:
            args += ["COUNT", count]
        if block is not None:
            args += ["BLOCK", block]
        args.append("STREAMS")
        args += list(streams.keys())
        args += list(streams.values())
        timeout: float | None = -1
        if block is not None:
            timeout = None if block == 0 else block / 1000 + _BLOCKING_GRACE
        reply = await self.execute(*args, timeout=timeout)
        if reply is None:
            return []
        return [
            (
                stream,
                [
                    (entry_id, _pairs_to_dict(fields) if fields else {})
                    for entry_id, fields in entries
                ],
            )
            for stream, entries in reply
        ]

    async def xrange(
        self,
        key: EncodableT,
        start: EncodableT = "-",
        end: EncodableT = "+",
        count: int | None = None,
    ) -> list[tuple[bytes, dict[bytes, bytes]]]:
        """Entries of a stream between two ids, oldest first."""
        args: list[EncodableT] = ["XRANGE", key, start, end]
        if count is not None:
            args += ["COUNT", count]
        reply = await self.execute(*args)
        return [(entry_id, _pairs_to_dict(fields)) for entry_id, fields in reply]

    async def xrevrange(
        self,
        key: EncodableT,
        end: EncodableT = "+",
        start: EncodableT = "-",
        count: int | None = None,
    ) -> list[tuple[bytes, dict[bytes, bytes]]]:
        """Entries of a stream between two ids, newest first."""
        args: list[EncodableT] = ["XREVRANGE", key, end, start]
        if count is not None:
            args += ["COUNT", count]
        reply = await self.execute(*args)
        return [(entry_id, _pairs_to_dict(fields)) for entry_id, fields in reply]

    async def xdel(self, key: EncodableT, *ids: EncodableT) -> int:
        """Delete stream entries."""
        return await self.execute("XDEL", key, *ids)

    async def zrange_withscores(
        self,
        key: EncodableT,
        start: int,
        stop: int,
    ) -> list[tuple[bytes, float]]:
        """Members with scores, by rank."""
        reply = await self.execute("ZRANGE", key, start, stop, "WITHSCORES")
        return [(reply[i], float(reply[i + 1])) for i in range(0, len(reply), 2)]

    async def zscore(self, key: EncodableT, member: EncodableT) -> float | None:
        """Score of a member, or None."""
        reply = await self.execute("ZSCORE", key, member)
        return None if reply is None else float(reply)

    async def smembers(self, key: EncodableT) -> builtins.set[bytes]:
        """All members of a set."""
        return builtins.set(await self.execute("SMEMBERS", key))


class Pipeline:
    """
    Commands collected to run in one round trip.

    Example::

        pipe = redis.pipeline(transaction=True)
        pipe.add("INCR", "counter")
        pipe.add("EXPIRE", "counter", 60)
        count, _ = await pipe.execute()
    """

    def __init__(self, client: Redis, *, transaction: bool = False) -> None:
        self.client = client
        self.transaction = transaction
        self.commands: list[tuple[EncodableT, ...]] = []

    def add(self, *args: EncodableT) -> Self:
        """
        Queue a command.

        :param args: command and arguments.
        :return: the pipeline, for chaining.
        """
        self.commands.append(args)
        return self

    def __len__(self) -> int:
        return len(self.commands)

    async def execute(self, raise_on_error: bool = True) -> list[Any]:
        """
        Run the queued commands.

        :param raise_on_error: raise the first error reply instead of
            returning it in place.
        :raises ResponseError: on error replies, if ``raise_on_error``.
        :return: one reply per queued command.
        """
        commands, self.commands = self.commands, []
        if not commands:
            return []
        async with self.client.pool.connection() as connection:
            if self.transaction:
                replies = await connection.execute_many(
                    [("MULTI",), *commands, ("EXEC",)],
                )
                for reply in replies[:-1]:
                    if isinstance(reply, ResponseError):
                        # The transaction was discarded (e.g. syntax error).
                        raise reply
                exec_reply = replies[-1]
                if isinstance(exec_reply, ResponseError):
                    raise exec_reply
                results = list(exec_reply or [])
            else:
                results = await connection.execute_many(commands)
        if raise_on_error:
            for reply in results:
                if isinstance(reply, ResponseError):
                    raise reply
        return results


@dataclass(frozen=True, slots=True)
class PubSubMessage:
    """A message received on a pub/sub channel."""

    channel: bytes
    data: bytes
    pattern: bytes | None = None


class PubSub:
    """
    A pub/sub session on a dedicated connection.

    Example::

        async with redis.pubsub() as pubsub:
            await pubsub.subscribe("news")
            async for message in pubsub.listen():
                print(message.channel, message.data)
    """

    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool
        self._connection: Connection | None = None
        # A read that outlives a get_message() timeout, so no reply is
        # ever cut in half.
        self._pending_read: asyncio.Task[Any] | None = None
        self.channels: set[bytes] = set()
        self.patterns: set[bytes] = set()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def _get_connection(self) -> Connection:
        if self._connection is None or not self._connection.is_connected:
            # Pub/sub connections stay outside the pool: they are
            # dedicated to this session for its whole life.
            self._cancel_pending_read()
            self._connection = await self.pool._new_connection()  # noqa: SLF001
            if self.channels:
                await self._connection.send("SUBSCRIBE", *self.channels)
            if self.patterns:
                await self._connection.send("PSUBSCRIBE", *self.patterns)
        return self._connection

    @staticmethod
    def _as_bytes(names: Iterable[EncodableT]) -> list[bytes]:
        return [n.encode() if isinstance(n, str) else bytes(n) for n in names]  # type: ignore[arg-type]

    async def subscribe(self, *channels: EncodableT) -> None:
        """Subscribe to channels."""
        names = self._as_bytes(channels)
        connection = await self._get_connection()
        new = [n for n in names if n not in self.channels]
        self.channels.update(names)
        if new:
            await connection.send("SUBSCRIBE", *new)

    async def psubscribe(self, *patterns: EncodableT) -> None:
        """Subscribe to glob patterns."""
        names = self._as_bytes(patterns)
        connection = await self._get_connection()
        new = [n for n in names if n not in self.patterns]
        self.patterns.update(names)
        if new:
            await connection.send("PSUBSCRIBE", *new)

    async def unsubscribe(self, *channels: EncodableT) -> None:
        """Unsubscribe from channels (all if none are given)."""
        names = self._as_bytes(channels)
        if names:
            self.channels.difference_update(names)
        else:
            self.channels.clear()
        if self._connection is not None and self._connection.is_connected:
            await self._connection.send("UNSUBSCRIBE", *names)

    async def get_message(self, timeout: float | None = None) -> PubSubMessage | None:
        """
        Wait for the next message.

        Subscription confirmations are skipped.

        :param timeout: seconds to wait; None waits forever.
        :return: message, or None on timeout.
        """
        connection = await self._get_connection()
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            remaining = None if deadline is None else max(deadline - loop.time(), 0)
            if self._pending_read is None:
                self._pending_read = asyncio.ensure_future(connection.read())
            done, _ = await asyncio.wait({self._pending_read}, timeout=remaining)
            if not done:
                return None
            read_task, self._pending_read = self._pending_read, None
            reply = read_task.result()
            if not isinstance(reply, list) or not reply:
                continue
            kind = reply[0]
            if kind == _MESSAGE:
                return PubSubMessage(channel=reply[1], data=reply[2])
            if kind == _PATTERN_MESSAGE:
                return PubSubMessage(channel=reply[2], data=reply[3], pattern=reply[1])

    async def listen(self) -> AsyncIterator[PubSubMessage]:
        """
        Yield messages forever.

        :yield: received messages.
        """
        while True:
            message = await self.get_message()
            if message is not None:
                yield message

    def _cancel_pending_read(self) -> None:
        if self._pending_read is not None:
            self._pending_read.cancel()
            self._pending_read = None

    async def close(self) -> None:
        """Close the session's connection."""
        self._cancel_pending_read()
        connection, self._connection = self._connection, None
        if connection is not None:
            with contextlib.suppress(Exception):
                await connection.close()
