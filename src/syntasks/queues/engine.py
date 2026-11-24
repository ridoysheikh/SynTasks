"""
Queue engine: named queues, priorities and limits on Redis streams.

Key layout (under ``syntasks:{<app>}:``):

==================================  ==============================================
``q:index``                           SET of queue names
``q:<queue>:p<N>``                    STREAM per priority level (0 = highest)
``q:<queue>:t:<task>``                STREAM of one task that is gated (cluster
                                      concurrency, per-machine concurrency, rate limit)
                                      or routed by tags
``q:<queue>:streams``                 SET of the queue's stream keys
``q:<queue>:delayed``                 ZSET of delayed messages, scored by due time
``q:<queue>:dlq``                     STREAM of dead letters
``q:<queue>:meta``                    HASH (``paused``)
``q:uniq:<hash>``                     dedupe key for ``unique_for``
``q:park:<task>:<key>``               LIST of messages waiting for a per-key slot
``lim:sem:task:<task>``               cluster concurrency slots of a task
``lim:wake:task:<task>``              wake list of a gated task (kick / slot release)
==================================  ==============================================

All streams use one consumer group, ``workers``; each worker process is a
consumer named by its node id. Processed entries are acknowledged and
deleted, so a stream's length is its backlog.
"""

import asyncio
import contextlib
import hashlib
import json
import logging
import math
import time
import uuid
from collections import Counter, deque
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any

from syntasks.acks import AckableMessage
from syntasks.cluster.semaphore import RELEASE as SEM_RELEASE
from syntasks.cluster.semaphore import RENEW as SEM_RENEW
from syntasks.exceptions import DuplicateTaskError, QueueFullError
from syntasks.limits import check_size
from syntasks.message import BrokerMessage
from syntasks.options import TaskOptions, extract_argument, key_text
from syntasks.queues import scripts
from syntasks.redis.errors import RedisConnectionError, RedisError, RedisTimeoutError

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks

__all__ = [
    "Delivery",
    "DuplicateTaskError",
    "QueueEngine",
    "QueueFullError",
]

logger = logging.getLogger("syntasks.queues")

_RETRYABLE = (RedisConnectionError, RedisTimeoutError, OSError)
_PAUSE_REFRESH = 2.0
_GATED_POLL = 1.0
_PARK_TRANSFER_TTL_MS = 86_400_000


@dataclass(kw_only=True)
class Delivery(AckableMessage):
    """A message taken from a queue stream by this worker."""

    stream: str
    entry_id: bytes
    task_name: str
    queue: str
    gated: bool = False
    slot_field: str | None = None
    cluster_holder: str | None = None
    local_task: str | None = None
    key_slots: list[tuple[str, str, str]] = field(default_factory=list)
    finished: bool = False
    slots_released: bool = False
    taken_at: float = field(default_factory=time.monotonic)

    @property
    def ref(self) -> tuple[str, bytes]:
        """Identity of the stream entry."""
        return self.stream, self.entry_id


def _noop_ack() -> None:  # pragma: no cover
    """Placeholder; replaced right after the delivery is built."""


class QueueEngine:
    """Sending to and fetching from the app's queues."""

    def __init__(self, app: "SynTasks") -> None:
        self.app = app
        self.redis = app.redis
        self.keys = app.cluster.keys
        self.group = scripts.GROUP
        self.consumer = app.cluster.node_id
        # Worker-side state (set by serve()).
        self.serving = False
        self.queues: list[str] = []
        self.tags: set[str] = set()
        self.processes = 1
        self.fetch_count = 1
        self.block_ms = 1000
        self.claim_idle_ms = 60_000
        self.slot_ttl_ms = 60_000
        self._buffer: deque[Delivery] = deque()
        self._plain_task: asyncio.Task[Any] | None = None
        self._wake_task: asyncio.Task[Any] | None = None
        self._inflight: dict[tuple[str, bytes], Delivery] = {}
        self._local_running: Counter[str] = Counter()
        self._rate_wait_until: dict[str, float] = {}
        self._gated_cursor = 0
        self._paused: set[str] = set()
        self._paused_checked = 0.0
        self._next_claim = 0.0
        self._next_gated_claim: dict[str, float] = {}
        self._groups_ready: set[str] = set()
        self._attempt = 0

    # -- keys ------------------------------------------------------------

    def stream_key(self, queue: str, priority: int = 0) -> str:
        """Stream of one priority level of a queue."""
        return self.keys("q", queue, f"p{priority}")

    def task_stream_key(self, queue: str, task_name: str) -> str:
        """Dedicated stream of one task."""
        return self.keys("q", queue, "t", task_name)

    def streams_key(self, queue: str) -> str:
        """Set of a queue's streams."""
        return self.keys("q", queue, "streams")

    def delayed_key(self, queue: str) -> str:
        """Delayed messages of a queue."""
        return self.keys("q", queue, "delayed")

    def dlq_key(self, queue: str) -> str:
        """Dead letters of a queue."""
        return self.keys("q", queue, "dlq")

    def meta_key(self, queue: str) -> str:
        """Settings and state of a queue."""
        return self.keys("q", queue, "meta")

    @property
    def index_key(self) -> str:
        """Set of all queue names."""
        return self.keys("q", "index")

    def task_semaphore(self, task_name: str) -> str:
        """Name of a task's cluster concurrency semaphore."""
        return f"task:{task_name}"

    def task_wake_key(self, task_name: str) -> str:
        """Wake list of a gated task."""
        return self.keys("lim", "wake", self.task_semaphore(task_name))

    # -- routing ---------------------------------------------------------

    def dedicated(self, options: TaskOptions) -> bool:
        """Whether a task gets its own stream."""
        return options.gated or bool(options.tags)

    def route(
        self,
        task_name: str,
        labels: dict[str, Any],
    ) -> tuple[str, str, TaskOptions]:
        """
        Decide where a message goes.

        :param task_name: task name.
        :param labels: message labels (``queue`` / ``priority`` override).
        :return: ``(queue, stream key, options)``.
        """
        options = self.app.options_for(task_name)
        queue = str(labels.get("queue") or options.queue)
        if self.dedicated(options):
            return queue, self.task_stream_key(queue, task_name), options
        levels = self.app.queue_options(queue).priorities
        try:
            priority = int(labels.get("priority", options.priority))
        except (TypeError, ValueError):
            priority = options.priority
        priority = min(max(priority, 0), levels - 1)
        return queue, self.stream_key(queue, priority), options

    # -- sending -----------------------------------------------------------

    def _unique_key(self, message: BrokerMessage, options: TaskOptions) -> str:
        try:
            payload = json.loads(message.message)
            if options.unique_key:
                basis: Any = extract_argument(
                    options.unique_key,
                    None,
                    payload.get("args", []),
                    payload.get("kwargs", {}),
                )
            else:
                basis = [payload.get("args", []), payload.get("kwargs", {})]
            text = key_text(basis)
        except (ValueError, KeyError, IndexError, TypeError):
            text = message.message.decode(errors="replace")
        digest = hashlib.sha1(  # noqa: S324
            f"{message.task_name}\x00{text}".encode(),
        ).hexdigest()
        return self.keys("q", "uniq", digest)

    async def kick(self, message: BrokerMessage) -> str:
        """
        Put a message into its queue.

        :param message: formatted message.
        :raises QueueFullError: if the queue is full and rejects.
        :raises DuplicateTaskError: if ``unique_for`` dropped it.
        :raises PayloadTooLargeError: if the message is too big.
        :return: stream entry id, or ``""`` if delayed.
        """
        queue, stream, options = self.route(message.task_name, message.labels)
        check_size(
            message.message,
            max_bytes=options.max_payload,
            what=f"Message of task {message.task_name}",
        )
        queue_config = self.app.queue_options(queue)
        delay_ms = ""
        raw_delay = message.labels.get("delay")
        if raw_delay not in (None, "", 0, "0"):
            try:
                delay = float(str(raw_delay))
            except (TypeError, ValueError):
                logger.warning("Ignoring invalid delay label %r.", raw_delay)
            else:
                if delay > 0:
                    delay_ms = str(math.ceil(delay * 1000))
        unique_ttl = ""
        unique_key = self.keys("q", "uniq", "-")
        if options.unique_for and not message.labels.get("_retry"):
            unique_ttl = str(math.ceil(options.unique_for * 1000))
            unique_key = self._unique_key(message, options)
        reply = await scripts.KICK(
            self.redis,
            [
                stream,
                self.streams_key(queue),
                self.index_key,
                self.task_wake_key(message.task_name),
                unique_key,
                self.delayed_key(queue),
            ],
            [
                message.message,
                message.task_name,
                queue,
                "" if queue_config.max_length is None else queue_config.max_length,
                queue_config.on_full,
                unique_ttl,
                message.task_id,
                delay_ms,
                self.group,
                "1" if self.dedicated(options) else "",
                uuid.uuid4().hex,
            ],
        )
        status = reply[0].decode()
        if status == "dup":
            raise DuplicateTaskError(task_id=reply[1].decode())
        if status == "full":
            raise QueueFullError(queue=queue, length=int(reply[1]))
        if delay_ms and float(delay_ms) < self.app.config.scheduler.max_sleep * 1000:
            with contextlib.suppress(RedisError, OSError):
                await self.app.cluster.control.publish("delayed", queue=queue)
        return "" if status == "delayed" else reply[1].decode()

    # -- worker setup ------------------------------------------------------

    def serve(
        self,
        queues: list[str],
        *,
        tags: list[str] | None = None,
        processes: int = 1,
        fetch_count: int = 1,
        block: float = 1.0,
        claim_idle: float = 60.0,
    ) -> None:
        """
        Configure which queues this process reads.

        :param queues: queue names, in priority order.
        :param tags: this machine's tags.
        :param processes: worker processes on this machine (splits
            per-machine limits).
        :param fetch_count: messages fetched per read.
        :param block: seconds one blocking read waits.
        :param claim_idle: seconds before unacknowledged messages are
            taken over.
        """
        if not queues:
            raise ValueError("A worker must serve at least one queue.")
        self.queues = list(dict.fromkeys(queues))
        self.tags = set(tags or ())
        self.processes = max(processes, 1)
        self.fetch_count = max(fetch_count, 1)
        self.block_ms = max(int(block * 1000), 10)
        self.claim_idle_ms = max(int(claim_idle * 1000), 1000)
        self.slot_ttl_ms = max(30_000, self.claim_idle_ms)
        self.serving = True

    def plain_streams(self) -> list[tuple[str, str]]:
        """``(queue, stream)`` pairs read with plain blocking reads."""
        streams = []
        for queue in self.queues:
            if queue in self._paused:
                continue
            for level in range(self.app.queue_options(queue).priorities):
                streams.append((queue, self.stream_key(queue, level)))
        return streams

    def gated_tasks(self) -> list[tuple[str, str, TaskOptions]]:
        """``(task, stream, options)`` of dedicated streams this process serves."""
        result = []
        for name in sorted(self.app.get_all_tasks()):
            options = self.app.options_for(name)
            if not self.dedicated(options):
                continue
            if options.queue not in self.queues or options.queue in self._paused:
                continue
            if not set(options.tags) <= self.tags:
                continue
            result.append((name, self.task_stream_key(options.queue, name), options))
        return result

    def node_limit(self, options: TaskOptions) -> int | None:
        """A task's limit per worker process (a node)."""
        return options.max_tasks_per_node or None

    async def ensure_groups(self) -> None:
        """Create the consumer group on every stream this process reads."""
        pipe = self.redis.pipeline()
        streams = [
            *self.plain_streams(),
            *((o.queue, s) for _, s, o in self.gated_tasks()),
        ]
        for queue, stream in streams:
            pipe.add("XGROUP", "CREATE", stream, self.group, "0", "MKSTREAM")
            pipe.add("SADD", self.streams_key(queue), stream)
            pipe.add("SADD", self.index_key, queue)
        await pipe.execute(raise_on_error=False)
        self._groups_ready = {s for _, s in streams}

    async def refresh_paused(self, force: bool = False) -> None:
        """Re-read which served queues are paused."""
        now = time.monotonic()
        if not force and now - self._paused_checked < _PAUSE_REFRESH:
            return
        self._paused_checked = now
        pipe = self.redis.pipeline()
        for queue in self.queues:
            pipe.add("HGET", self.meta_key(queue), "paused")
        replies = await pipe.execute(raise_on_error=False)
        paused = {
            queue
            for queue, reply in zip(self.queues, replies, strict=True)
            if reply == b"1"
        }
        if paused != self._paused:
            logger.info("Paused queues: %s", ", ".join(sorted(paused)) or "none")
        self._paused = paused

    # -- fetching ------------------------------------------------------------

    def _make_delivery(
        self,
        queue: str,
        stream: str,
        entry_id: bytes,
        fields: dict[bytes, bytes],
        *,
        gated: bool = False,
    ) -> Delivery | None:
        data = fields.get(b"data")
        if data is None:
            return None
        slot = fields.get(b"slot")
        delivery = Delivery(
            data=data,
            ack=_noop_ack,
            stream=stream,
            entry_id=entry_id,
            task_name=(fields.get(b"task") or b"").decode(),
            queue=queue,
            gated=gated,
            slot_field=slot.decode() if slot else None,
        )
        delivery.ack = partial(self.ack, delivery)
        self._inflight[delivery.ref] = delivery
        return delivery

    async def _try_gated(self) -> Delivery | None:
        gated = self.gated_tasks()
        if not gated:
            return None
        now = time.monotonic()
        count = len(gated)
        for offset in range(count):
            name, stream, options = gated[(self._gated_cursor + offset) % count]
            limit = self.node_limit(options)
            if limit is not None and self._local_running[name] >= limit:
                continue
            if self._rate_wait_until.get(name, 0.0) > now:
                continue
            delivery = await self._take_gated(name, stream, options, now)
            if delivery is not None:
                self._gated_cursor = (self._gated_cursor + offset + 1) % count
                return delivery
        return None

    async def _take_gated(
        self,
        name: str,
        stream: str,
        options: TaskOptions,
        now: float,
    ) -> Delivery | None:
        queue_config = self.app.queue_options(options.queue)
        holder = self.app.cluster.holder(uuid.uuid4().hex)
        claim = ""
        if now >= self._next_gated_claim.get(name, 0.0):
            claim = str(self.claim_idle_ms)
            self._next_gated_claim[name] = now + self.claim_idle_ms / 2000
        semaphore = self.task_semaphore(name)
        reply = await scripts.GATED_TAKE(
            self.redis,
            [
                stream,
                self.keys("lim", "sem", semaphore),
                self.keys("lim", "sems"),
                self.keys("lim", "rate", f"task:{name}"),
                self.dlq_key(options.queue),
            ],
            [
                self.group,
                self.consumer,
                holder,
                "" if options.max_tasks is None else options.max_tasks,
                self.slot_ttl_ms,
                "" if options.rate is None else repr(options.rate.emission_ms),
                options.rate.burst if options.rate else 1,
                claim,
                queue_config.max_deliveries,
                semaphore,
                options.queue,
                queue_config.dead_letter_max,
            ],
        )
        status = reply[0].decode()
        if status == "rate":
            self._rate_wait_until[name] = time.monotonic() + int(reply[1]) / 1000
            return None
        if status != "ok":
            return None
        raw_fields = reply[2]
        fields = dict(zip(raw_fields[::2], raw_fields[1::2], strict=True))
        delivery = self._make_delivery(
            options.queue,
            stream,
            reply[1],
            fields,
            gated=True,
        )
        if delivery is None:
            await scripts.ACK(self.redis, [stream], [self.group, reply[1]])
            if options.max_tasks is not None:
                await SEM_RELEASE(
                    self.redis,
                    [self.keys("lim", "sem", semaphore), self.task_wake_key(name)],
                    [holder],
                )
            return None
        if options.max_tasks is not None:
            delivery.cluster_holder = holder
        delivery.local_task = name
        self._local_running[name] += 1
        return delivery

    def _buffer_batches(
        self,
        batches: list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]]]],
        streams: list[tuple[str, str]],
    ) -> None:
        order = {stream: index for index, (_, stream) in enumerate(streams)}
        queues = {stream: queue for queue, stream in streams}
        new: list[Delivery] = []
        for raw_stream, entries in batches:
            stream = (
                raw_stream.decode() if isinstance(raw_stream, bytes) else raw_stream
            )
            queue = queues.get(stream, "")
            for entry_id, fields in entries:
                delivery = self._make_delivery(queue, stream, entry_id, fields)
                if delivery is None:
                    self._inflight.pop((stream, entry_id), None)
                    asyncio.create_task(  # noqa: RUF006
                        scripts.ACK(self.redis, [stream], [self.group, entry_id]),
                    )
                    continue
                new.append(delivery)
        new.sort(key=lambda d: order.get(d.stream, len(order)))
        merged = sorted(
            [*self._buffer, *new],
            key=lambda d: order.get(d.stream, len(order)),
        )
        self._buffer = deque(merged)

    async def _read_plain(
        self,
        streams: list[tuple[str, str]],
        block: int | None,
    ) -> Any:
        return await self.redis.xreadgroup(
            self.group,
            self.consumer,
            {stream: ">" for _, stream in streams},
            count=self.fetch_count,
            block=block,
        )

    async def _claim_plain(self, streams: list[tuple[str, str]]) -> None:
        for queue, stream in streams:
            queue_config = self.app.queue_options(queue)
            reply = await scripts.CLAIM_ABANDONED(
                self.redis,
                [stream, self.dlq_key(queue)],
                [
                    self.group,
                    self.consumer,
                    self.claim_idle_ms,
                    self.fetch_count,
                    queue_config.max_deliveries,
                    queue,
                    queue_config.dead_letter_max,
                ],
            )
            if reply:
                batch = [
                    (
                        reply[i],
                        dict(zip(reply[i + 1][::2], reply[i + 1][1::2], strict=True)),
                    )
                    for i in range(0, len(reply), 2)
                ]
                logger.info(
                    "Took over %d abandoned message(s) from %s.",
                    len(batch),
                    stream,
                )
                self._buffer_batches([(stream.encode(), batch)], streams)

    async def _wait(self, streams: list[tuple[str, str]], has_gated: bool) -> None:
        waiters: set[asyncio.Task[Any]] = set()
        if streams:
            if self._plain_task is None:
                self._plain_task = asyncio.create_task(
                    self._read_plain(streams, self.block_ms),
                )
            waiters.add(self._plain_task)
        timeout: float | None = None
        if has_gated:
            if self._wake_task is None:
                wake_keys = [
                    self.task_wake_key(name) for name, _, _ in self.gated_tasks()
                ]
                self._wake_task = asyncio.create_task(
                    self.redis.blpop(wake_keys, _GATED_POLL),
                )
            waiters.add(self._wake_task)
            timeout = _GATED_POLL
            if self._rate_wait_until:
                soonest = min(self._rate_wait_until.values()) - time.monotonic()
                timeout = max(min(timeout, soonest), 0.001)
        if not waiters:
            await asyncio.sleep(self.block_ms / 1000)
            return
        await asyncio.wait(
            waiters,
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if self._wake_task is not None and self._wake_task.done():
            task, self._wake_task = self._wake_task, None
            with contextlib.suppress(Exception):
                task.result()
        if self._plain_task is not None and self._plain_task.done():
            task, self._plain_task = self._plain_task, None
            batches = task.result()
            if batches:
                self._buffer_batches(batches, streams)

    async def listen(self) -> AsyncGenerator[Delivery, None]:  # noqa: C901
        """
        Yield messages for this process, honoring limits and priorities.

        :raises RuntimeError: if :meth:`serve` was not called.
        :yield: deliveries.
        """
        if not self.serving:
            raise RuntimeError("Call serve() before listening.")
        try:
            while True:
                try:
                    if self._buffer:
                        yield self._buffer.popleft()
                        continue
                    await self.refresh_paused()
                    delivery = await self._try_gated()
                    if delivery is not None:
                        yield delivery
                        continue
                    streams = self.plain_streams()
                    if streams and time.monotonic() >= self._next_claim:
                        self._next_claim = time.monotonic() + max(
                            self.claim_idle_ms / 2000,
                            1.0,
                        )
                        await self._claim_plain(streams)
                        if self._buffer:
                            continue
                    if streams and self._plain_task is None:
                        batches = await self._read_plain(streams, None)
                        if batches:
                            self._buffer_batches(batches, streams)
                            continue
                    await self._wait(streams, bool(self.gated_tasks()))
                    self._attempt = 0
                except _RETRYABLE as exc:
                    await self._reset_waiters()
                    delay = min(0.1 * 2**self._attempt, 5.0)
                    self._attempt += 1
                    logger.warning(
                        "Redis is unavailable (%s); retrying in %.1fs.",
                        exc,
                        delay,
                    )
                    await asyncio.sleep(delay)
                except RedisError as exc:
                    await self._reset_waiters()
                    if "NOGROUP" in str(exc):
                        await self.ensure_groups()
                        continue
                    raise
        finally:
            await self._reset_waiters()
            await self.requeue_buffered()

    async def _reset_waiters(self) -> None:
        for name in ("_plain_task", "_wake_task"):
            task = getattr(self, name)
            setattr(self, name, None)
            if task is None:
                continue
            if task.done():
                if (
                    name == "_plain_task"
                    and not task.cancelled()
                    and task.exception() is None
                ):
                    batches = task.result()
                    if batches:
                        self._buffer_batches(batches, self.plain_streams())
                elif not task.cancelled():
                    task.exception()
                continue
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def requeue_buffered(self) -> None:
        """Give back messages taken but not started (on shutdown)."""
        buffered, self._buffer = list(self._buffer), deque()
        for delivery in buffered:
            try:
                await scripts.REQUEUE(
                    self.redis,
                    [delivery.stream, self.task_wake_key(delivery.task_name)],
                    [
                        self.group,
                        delivery.entry_id,
                        delivery.data,
                        delivery.task_name,
                        "1" if delivery.gated else "",
                    ],
                )
            except (RedisError, OSError):
                logger.warning("Could not requeue message %s.", delivery.entry_id)
            await self.finish(delivery)
        if buffered:
            logger.info("Requeued %d prefetched message(s).", len(buffered))

    # -- per-key concurrency --------------------------------------------

    async def acquire_key(
        self,
        delivery: Delivery,
        options: TaskOptions,
        key: str,
    ) -> bool:
        """
        Take a per-key slot for a delivery, or park the delivery.

        :param delivery: the delivery.
        :param options: task options.
        :param key: key value as text.
        :return: True to run it now; False if it was parked (and acked).
        """
        return await self.acquire_slot(
            delivery,
            f"key:{delivery.task_name}:{key}",
            self.keys("q", "park", delivery.task_name, key),
            options.max_tasks_per_key or 1,
            "park",
        )

    async def acquire_slot(
        self,
        delivery: Delivery,
        name: str,
        park_key: str,
        limit: int,
        mode: str,
    ) -> bool:
        """
        Take a slot of a named semaphore for a delivery.

        If no slot is free the delivery is parked (``mode="park"``, it runs
        when a slot frees up, in order) or dropped (``mode="drop"``).

        :param delivery: the delivery.
        :param name: semaphore name.
        :param park_key: list where parked deliveries wait.
        :param limit: slots.
        :param mode: ``park`` or ``drop``.
        :return: True to run it now.
        """
        sem_key = self.keys("lim", "sem", name)
        holder = self.app.cluster.holder(uuid.uuid4().hex)
        acquired = await scripts.KEY_ACQUIRE(
            self.redis,
            [
                sem_key,
                park_key,
                delivery.stream,
                self.keys("lim", "sems"),
                self.keys("q", "parks"),
            ],
            [
                holder,
                limit,
                self.slot_ttl_ms,
                self.group,
                delivery.entry_id,
                delivery.data,
                delivery.task_name,
                delivery.slot_field or "",
                name,
                mode,
            ],
        )
        if acquired:
            delivery.key_slots.append((sem_key, park_key, holder))
            return True
        delivery.ack = _noop_ack
        await self.finish(delivery)
        return False

    async def unpark_free(self) -> int:
        """Janitor: move parked messages whose key has a free slot back."""
        return int(
            await scripts.UNPARK_FREE(
                self.redis,
                [self.keys("q", "parks")],
                [_PARK_TRANSFER_TTL_MS, self.group, self.keys.base + ":"],
            ),
        )

    # -- completion --------------------------------------------------------

    async def ack(self, delivery: Delivery) -> None:
        """
        Acknowledge and delete a delivery's stream entry.

        :param delivery: the delivery.
        """
        await scripts.ACK(
            self.redis,
            [delivery.stream],
            [self.group, delivery.entry_id],
        )

    async def finish(self, delivery: Delivery) -> None:
        """
        Release everything a delivery held (slots, local counters).

        Safe to call more than once.

        :param delivery: the delivery.
        """
        if delivery.finished:
            return
        delivery.finished = True
        self._inflight.pop(delivery.ref, None)
        await self.release_slots(delivery)

    async def release_slots(self, delivery: Delivery) -> None:
        """
        Free the concurrency slots of a delivery whose run has ended.

        Called as soon as the task returns (before its result is stored),
        and again by :meth:`finish`; only the first call does anything.

        :param delivery: the delivery.
        """
        if delivery.slots_released:
            return
        delivery.slots_released = True
        if delivery.local_task is not None:
            self._local_running[delivery.local_task] -= 1
            if self._local_running[delivery.local_task] <= 0:
                del self._local_running[delivery.local_task]
        try:
            if delivery.cluster_holder is not None:
                semaphore = self.task_semaphore(delivery.task_name)
                await SEM_RELEASE(
                    self.redis,
                    [
                        self.keys("lim", "sem", semaphore),
                        self.task_wake_key(delivery.task_name),
                    ],
                    [delivery.cluster_holder],
                )
            for sem_key, park_key, holder in delivery.key_slots:
                await scripts.KEY_RELEASE(
                    self.redis,
                    [sem_key, park_key],
                    [holder, _PARK_TRANSFER_TTL_MS, self.group, self.keys.base + ":"],
                )
        except (RedisError, OSError) as exc:
            logger.warning(
                "Could not release slots of %s (%s); they expire on their own.",
                delivery.task_name,
                exc,
            )

    async def dead_letter(
        self,
        delivery: Delivery | None,
        *,
        queue: str,
        data: bytes,
        task_name: str,
        reason: str,
        error: str,
    ) -> None:
        """
        Store a message in the queue's dead letters (and ack it).

        :param delivery: the delivery, if the message came from a stream.
        :param queue: queue name.
        :param data: message payload.
        :param task_name: task name.
        :param reason: short reason, e.g. ``failed`` or ``unknown_task``.
        :param error: error text.
        """
        await scripts.DEAD_LETTER(
            self.redis,
            [
                delivery.stream if delivery else self.stream_key(queue),
                self.dlq_key(queue),
            ],
            [
                self.group,
                delivery.entry_id if delivery else "",
                data,
                task_name,
                reason,
                error[:4000],
                queue,
                self.app.queue_options(queue).dead_letter_max,
            ],
        )

    async def refresh_inflight(self) -> None:
        """
        Keep running messages from being taken over, and keep their slots.

        Resets the idle time of this process's in-flight entries and
        renews their semaphore leases.
        """
        if not self._inflight:
            return
        by_stream: dict[str, list[bytes]] = {}
        for delivery in list(self._inflight.values()):
            by_stream.setdefault(delivery.stream, []).append(delivery.entry_id)
        pipe = self.redis.pipeline()
        for stream, ids in by_stream.items():
            pipe.add("XCLAIM", stream, self.group, self.consumer, 0, *ids, "JUSTID")
        await pipe.execute(raise_on_error=False)
        for delivery in list(self._inflight.values()):
            if delivery.cluster_holder is not None:
                await SEM_RENEW(
                    self.redis,
                    [self.keys("lim", "sem", self.task_semaphore(delivery.task_name))],
                    [delivery.cluster_holder, self.slot_ttl_ms],
                )
            for sem_key, _, holder in delivery.key_slots:
                await SEM_RENEW(self.redis, [sem_key], [holder, self.slot_ttl_ms])

    def running_count(self) -> int:
        """Messages taken and not finished by this process."""
        return len(self._inflight)

    # -- delayed messages (run by the dispatcher) -------------------------------

    async def move_delayed(self) -> float | None:
        """
        Move due delayed messages of every queue into their streams.

        :return: next due time (Unix ms) among all queues, or None.
        """
        next_due: float | None = None
        for raw in await self.redis.smembers(self.index_key):
            queue = raw.decode()
            while True:
                moved, due = await scripts.MOVE_DELAYED(
                    self.redis,
                    [self.delayed_key(queue)],
                    [100, self.group, self.keys("lim", "wake", "task:")],
                )
                if moved < 100:
                    break
            if due:
                due_ms = float(due)
                next_due = due_ms if next_due is None else min(next_due, due_ms)
        return next_due
