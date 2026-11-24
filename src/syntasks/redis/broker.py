"""Redis brokers."""

import asyncio
import logging
import time
import uuid
from collections.abc import AsyncGenerator
from functools import partial
from typing import Any

from syntasks.abc.broker import AsyncBroker
from syntasks.acks import AckableMessage
from syntasks.message import BrokerMessage
from syntasks.redis.client import Redis
from syntasks.redis.errors import RedisConnectionError, RedisError, RedisTimeoutError
from syntasks.redis.pool import DEFAULT_MAX_CONNECTIONS

logger = logging.getLogger(__name__)

DEFAULT_URL = "redis://localhost:6379"
_RETRYABLE = (RedisConnectionError, RedisTimeoutError, OSError)
_MAX_BACKOFF = 5.0

# Moves due delayed messages to the queue, atomically.
# Members are a 32-char unique prefix followed by the payload.
_MOVE_DUE_TO_LIST = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, member in ipairs(due) do
  redis.call('ZREM', KEYS[1], member)
  redis.call('LPUSH', KEYS[2], string.sub(member, 33))
end
return #due
"""
_MOVE_DUE_TO_STREAM = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, member in ipairs(due) do
  redis.call('ZREM', KEYS[1], member)
  if ARGV[3] == '' then
    redis.call('XADD', KEYS[2], '*', 'data', string.sub(member, 33))
  else
    redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[3], '*',
               'data', string.sub(member, 33))
  end
end
return #due
"""


def make_client(
    url: str,
    client: Redis | None,
    max_connection_pool_size: int | None,
    connection_kwargs: dict[str, Any],
) -> tuple[Redis, bool]:
    """
    Build the Redis client a component uses.

    :param url: Redis URL, used when no client is given.
    :param client: an existing client to share.
    :param max_connection_pool_size: pool size for a new client.
    :param connection_kwargs: connection options for a new client.
    :return: the client and whether the component owns it.
    """
    if client is not None:
        return client, False
    return (
        Redis.from_url(
            url,
            max_connections=max_connection_pool_size or DEFAULT_MAX_CONNECTIONS,
            **connection_kwargs,
        ),
        True,
    )


def _delay_of(message: BrokerMessage) -> float:
    raw = message.labels.get("delay")
    if raw is None:
        return 0.0
    try:
        return max(float(raw), 0.0)
    except (TypeError, ValueError):
        logger.warning("Ignoring invalid delay label %r.", raw)
        return 0.0


class BaseRedisBroker(AsyncBroker):
    """Shared plumbing of the Redis brokers."""

    def __init__(
        self,
        url: str = DEFAULT_URL,
        *,
        queue_name: str = "syntasks",
        client: Redis | None = None,
        max_connection_pool_size: int | None = None,
        **connection_kwargs: Any,
    ) -> None:
        """
        Create the broker.

        :param url: Redis URL (``redis://``, ``rediss://``, ``unix://``).
        :param queue_name: name of the queue (Redis key or channel).
        :param client: share an existing :class:`Redis` client instead of
            creating one; it won't be closed by the broker.
        :param max_connection_pool_size: connections in the broker's pool.
        :param connection_kwargs: extra connection options, e.g.
            ``password``, ``socket_timeout``, ``client_name``.
        """
        super().__init__()
        self.queue_name = queue_name
        self.redis, self._owns_client = make_client(
            url,
            client,
            max_connection_pool_size,
            connection_kwargs,
        )

    async def shutdown(self) -> None:
        """Close the broker and its connections."""
        await super().shutdown()
        if self._owns_client:
            await self.redis.close()

    async def _backoff(self, attempt: int, error: BaseException) -> None:
        delay = min(0.1 * 2**attempt, _MAX_BACKOFF)
        logger.warning(
            "Redis is unavailable (%s). Retrying in %.1f seconds.",
            error,
            delay,
        )
        await asyncio.sleep(delay)


class _DelayedMixin(BaseRedisBroker):
    """Delayed delivery through a sorted set, for queue-based brokers."""

    delayed_batch_size = 100
    _move_script: str = ""

    @property
    def delayed_key(self) -> str:
        """Sorted set holding delayed messages, scored by due time."""
        return f"{self.queue_name}:delayed"

    async def _kick_delayed(self, message: BrokerMessage, delay: float) -> None:
        member = uuid.uuid4().hex.encode() + message.message
        await self.redis.zadd(self.delayed_key, {member: time.time() + delay})

    def _move_script_args(self) -> list[Any]:
        return [time.time(), self.delayed_batch_size]

    async def move_due_messages(self) -> int:
        """
        Move delayed messages that are due into the queue.

        Workers call this while listening; it is safe to run from many
        workers at once.

        :return: number of messages moved.
        """
        moved = 0
        while True:
            count = await self.redis.eval(
                self._move_script,
                keys=[self.delayed_key, self.queue_name],
                args=self._move_script_args(),
            )
            moved += count
            if count < self.delayed_batch_size:
                return moved


class RedisListBroker(_DelayedMixin):
    """
    Broker on a Redis list.

    Simple and fast: ``LPUSH`` to send, ``BRPOP`` to receive. Each message
    goes to exactly one worker, but there are no acknowledgements: a
    message a worker took is lost if that worker crashes. Use
    :class:`RedisStreamBroker` when that matters.

    Supports the ``delay`` label (seconds).
    """

    _move_script = _MOVE_DUE_TO_LIST

    def __init__(
        self,
        url: str = DEFAULT_URL,
        *,
        queue_name: str = "syntasks",
        poll_timeout: float = 1.0,
        client: Redis | None = None,
        max_connection_pool_size: int | None = None,
        **connection_kwargs: Any,
    ) -> None:
        """
        Create the broker.

        :param url: Redis URL.
        :param queue_name: list key.
        :param poll_timeout: seconds each ``BRPOP`` waits; also how often
            delayed messages are checked.
        :param client: share an existing client.
        :param max_connection_pool_size: connections in the pool.
        :param connection_kwargs: extra connection options.
        """
        super().__init__(
            url,
            queue_name=queue_name,
            client=client,
            max_connection_pool_size=max_connection_pool_size,
            **connection_kwargs,
        )
        self.poll_timeout = poll_timeout

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send a message.

        :param message: message to send.
        """
        delay = _delay_of(message)
        if delay > 0:
            await self._kick_delayed(message, delay)
        else:
            await self.redis.lpush(self.queue_name, message.message)

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Receive messages.

        :yield: message payloads.
        """
        attempt = 0
        while True:
            try:
                await self.move_due_messages()
                reply = await self.redis.brpop(self.queue_name, self.poll_timeout)
            except _RETRYABLE as exc:
                await self._backoff(attempt, exc)
                attempt += 1
                continue
            attempt = 0
            if reply is not None:
                yield reply[1]


class RedisStreamBroker(_DelayedMixin):
    """
    Reliable broker on a Redis stream with a consumer group.

    Messages are acknowledged (``XACK``) after processing, according to
    the worker's ``ack_type``. Messages a crashed worker never
    acknowledged are taken over by another worker once they have been
    idle for ``idle_timeout`` milliseconds.

    Supports the ``delay`` label (seconds). Requires Redis 6.2+.
    """

    _move_script = _MOVE_DUE_TO_STREAM

    def __init__(
        self,
        url: str = DEFAULT_URL,
        *,
        queue_name: str = "syntasks",
        consumer_group_name: str = "syntasks",
        consumer_name: str | None = None,
        consumer_id: str = "$",
        mkstream: bool = True,
        xread_block: int = 2000,
        xread_count: int = 10,
        maxlen: int | None = None,
        approximate: bool = True,
        idle_timeout: int = 600_000,
        unacknowledged_batch_size: int = 100,
        client: Redis | None = None,
        max_connection_pool_size: int | None = None,
        **connection_kwargs: Any,
    ) -> None:
        """
        Create the broker.

        :param url: Redis URL.
        :param queue_name: stream key.
        :param consumer_group_name: consumer group shared by all workers.
        :param consumer_name: this worker's name in the group; a random
            one is generated if not set.
        :param consumer_id: where a new group starts: ``$`` only new
            messages, ``0`` everything already in the stream.
        :param mkstream: create the stream if it doesn't exist.
        :param xread_block: milliseconds each read waits for messages.
        :param xread_count: messages fetched per read.
        :param maxlen: keep the stream at about this many entries.
        :param approximate: trim lazily (much cheaper than exact).
        :param idle_timeout: milliseconds after which an unacknowledged
            message is taken over by another worker.
        :param unacknowledged_batch_size: messages claimed per round.
        :param client: share an existing client.
        :param max_connection_pool_size: connections in the pool.
        :param connection_kwargs: extra connection options.
        """
        super().__init__(
            url,
            queue_name=queue_name,
            client=client,
            max_connection_pool_size=max_connection_pool_size,
            **connection_kwargs,
        )
        self.consumer_group_name = consumer_group_name
        self.consumer_name = consumer_name or uuid.uuid4().hex
        self.consumer_id = consumer_id
        self.mkstream = mkstream
        self.xread_block = xread_block
        self.xread_count = xread_count
        self.maxlen = maxlen
        self.approximate = approximate
        self.idle_timeout = idle_timeout
        self.unacknowledged_batch_size = unacknowledged_batch_size
        self._group_ready = False

    def _move_script_args(self) -> list[Any]:
        return [*super()._move_script_args(), self.maxlen or ""]

    async def _ensure_group(self, recovering: bool = False) -> None:
        if self._group_ready:
            return
        await self.redis.xgroup_create(
            self.queue_name,
            self.consumer_group_name,
            # After the stream or group vanished, everything now in the
            # stream arrived since then, so read it all.
            "0" if recovering else self.consumer_id,
            mkstream=self.mkstream,
        )
        self._group_ready = True

    async def startup(self) -> None:
        """
        Start the broker and make sure the consumer group exists.

        Senders create the group too, so messages sent before any worker
        has started are kept for the workers instead of being skipped.
        """
        await super().startup()
        await self._ensure_group()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send a message.

        :param message: message to send.
        """
        await self._ensure_group()
        delay = _delay_of(message)
        if delay > 0:
            await self._kick_delayed(message, delay)
            return
        await self.redis.xadd(
            self.queue_name,
            {"data": message.message},
            maxlen=self.maxlen,
            approximate=self.approximate,
        )

    async def _ack(self, entry_id: bytes) -> None:
        await self.redis.xack(self.queue_name, self.consumer_group_name, entry_id)

    def _wrap(
        self,
        entry_id: bytes,
        fields: dict[bytes, bytes],
    ) -> AckableMessage | None:
        data = fields.get(b"data")
        if data is None:
            logger.warning("Stream entry %s has no data field; skipping.", entry_id)
            return None
        return AckableMessage(data=data, ack=partial(self._ack, entry_id))

    async def claim_abandoned(self) -> list[AckableMessage]:
        """
        Take over messages other consumers left unacknowledged too long.

        :return: claimed messages.
        """
        claimed: list[AckableMessage] = []
        start: bytes | str = "0-0"
        while True:
            start, entries = await self.redis.xautoclaim(
                self.queue_name,
                self.consumer_group_name,
                self.consumer_name,
                self.idle_timeout,
                start,
                count=self.unacknowledged_batch_size,
            )
            for entry_id, fields in entries:
                message = self._wrap(entry_id, fields)
                if message is None:
                    await self._ack(entry_id)
                else:
                    claimed.append(message)
            if (
                start in {b"0-0", "0-0"}
                or len(claimed) >= self.unacknowledged_batch_size
            ):
                return claimed

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:  # noqa: C901
        """
        Receive messages.

        :yield: ackable messages.
        """
        attempt = 0
        # Check abandoned messages about twice per idle period.
        claim_every = max(self.idle_timeout / 2000, 1.0)
        next_claim = 0.0
        recovering = False
        while True:
            try:
                await self._ensure_group(recovering)
                recovering = False
                await self.move_due_messages()
                claimed: list[AckableMessage] = []
                if time.monotonic() >= next_claim:
                    claimed = await self.claim_abandoned()
                    next_claim = time.monotonic() + claim_every
                if claimed:
                    batches: Any = []
                else:
                    batches = await self.redis.xreadgroup(
                        self.consumer_group_name,
                        self.consumer_name,
                        {self.queue_name: ">"},
                        count=self.xread_count,
                        block=self.xread_block,
                    )
            except _RETRYABLE as exc:
                self._group_ready = False
                await self._backoff(attempt, exc)
                attempt += 1
                continue
            except RedisError as exc:
                if "NOGROUP" in str(exc):
                    # The stream or group was deleted; recreate it.
                    self._group_ready = False
                    recovering = True
                    continue
                raise
            attempt = 0
            for message in claimed:
                yield message
            for _, entries in batches:
                for entry_id, fields in entries:
                    wrapped = self._wrap(entry_id, fields)
                    if wrapped is None:
                        await self._ack(entry_id)
                    else:
                        yield wrapped


class RedisPubSubBroker(BaseRedisBroker):
    """
    Broker on Redis pub/sub: every listening worker gets every message.

    Useful for broadcasting (e.g. "reload config" to all workers).
    Messages sent while no worker listens are lost, and the ``delay``
    label is not supported.
    """

    async def kick(self, message: BrokerMessage) -> None:
        """
        Publish a message to every listening worker.

        :param message: message to send.
        """
        if _delay_of(message) > 0:
            logger.warning(
                "RedisPubSubBroker doesn't support delayed messages; "
                "sending task %s now.",
                message.task_name,
            )
        await self.redis.publish(self.queue_name, message.message)

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Receive messages.

        :yield: message payloads.
        """
        attempt = 0
        while True:
            pubsub = self.redis.pubsub()
            try:
                await pubsub.subscribe(self.queue_name)
                attempt = 0
                async for message in pubsub.listen():
                    yield message.data
            except _RETRYABLE as exc:
                await self._backoff(attempt, exc)
                attempt += 1
            finally:
                await pubsub.close()
