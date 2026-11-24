"""Queue operations: inspect, pause, purge, dead letters (``app.queues``)."""

from __future__ import annotations

import builtins
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from syntasks.queues.scripts import GROUP

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks

__all__ = ["DeadLetter", "QueueInfo", "QueueManager", "QueuedMessage"]


def _entry_ms(entry_id: bytes | str) -> int:
    text = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
    return int(text.split("-", 1)[0])


@dataclass(kw_only=True)
class QueueInfo:
    """State of one queue."""

    name: str
    waiting: int = 0
    running: int = 0
    delayed: int = 0
    dead: int = 0
    paused: bool = False
    oldest_ms: int | None = None
    streams: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Plain data."""
        return {
            "name": self.name,
            "waiting": self.waiting,
            "running": self.running,
            "delayed": self.delayed,
            "dead": self.dead,
            "paused": self.paused,
            "oldest_ms": self.oldest_ms,
            "streams": self.streams,
        }


@dataclass(kw_only=True)
class QueuedMessage:
    """A message waiting in a queue."""

    id: str
    stream: str
    task_name: str
    task_id: str | None
    args: builtins.list[Any]
    kwargs: dict[str, Any]
    labels: dict[str, Any]
    enqueued_ms: int


@dataclass(kw_only=True)
class DeadLetter:
    """A message that was given up on."""

    id: str
    queue: str
    task_name: str
    task_id: str | None
    reason: str
    error: str
    at_ms: int
    stream: str
    data: bytes = field(repr=False)


_RUN_STATE_LABELS = ("_attempt", "_retry", "delay", "_unknown")


def _fresh_payload(data: bytes) -> bytes:
    """A replayed message starts over: drop retry state from its labels."""
    payload = _decode_message(data)
    labels = payload.get("labels")
    if not isinstance(labels, dict):
        return data
    for name in _RUN_STATE_LABELS:
        labels.pop(name, None)
    types = payload.get("labels_types")
    if isinstance(types, dict):
        for name in _RUN_STATE_LABELS:
            types.pop(name, None)
    return json.dumps(payload, separators=(",", ":")).encode()


def _decode_message(data: bytes) -> dict[str, Any]:
    try:
        value = json.loads(data)
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}


class QueueManager:
    """Inspect and operate the app's queues."""

    def __init__(self, app: SynTasks) -> None:
        self.app = app

    @property
    def _engine(self) -> Any:
        return self.app.engine

    @property
    def _redis(self) -> Any:
        return self.app.redis

    async def names(self) -> builtins.list[str]:
        """All queues that ever received a message or a worker."""
        names = {
            raw.decode() for raw in await self._redis.smembers(self._engine.index_key)
        }
        return sorted(names)

    async def _streams(self, queue: str) -> builtins.list[str]:
        return sorted(
            raw.decode()
            for raw in await self._redis.smembers(self._engine.streams_key(queue))
        )

    async def info(self, queue: str) -> QueueInfo:
        """
        Lengths and state of a queue.

        :param queue: queue name.
        :return: queue info.
        """
        engine = self._engine
        info = QueueInfo(name=queue)
        for stream in await self._streams(queue):
            length = await self._redis.xlen(stream)
            info.streams[stream] = length
            if not length:
                continue
            try:
                pending = await self._redis.xpending_count(stream, GROUP)
            except Exception:
                pending = 0
            info.running += pending
            info.waiting += length - pending
            first = await self._redis.xrange(stream, count=1)
            if first:
                ms = _entry_ms(first[0][0])
                info.oldest_ms = (
                    ms if info.oldest_ms is None else min(info.oldest_ms, ms)
                )
        info.delayed = await self._redis.zcard(engine.delayed_key(queue))
        info.dead = await self._redis.xlen(engine.dlq_key(queue))
        info.paused = await self._redis.hget(engine.meta_key(queue), "paused") == b"1"
        return info

    async def list(self) -> builtins.list[QueueInfo]:
        """Info about every queue."""
        return [await self.info(name) for name in await self.names()]

    async def peek(self, queue: str, limit: int = 10) -> builtins.list[QueuedMessage]:
        """
        Oldest messages of a queue, without taking them.

        :param queue: queue name.
        :param limit: max messages per stream.
        :return: messages, oldest first.
        """
        messages: builtins.list[QueuedMessage] = []
        for stream in await self._streams(queue):
            for entry_id, fields in await self._redis.xrange(stream, count=limit):
                payload = _decode_message(fields.get(b"data", b""))
                messages.append(
                    QueuedMessage(
                        id=entry_id.decode(),
                        stream=stream,
                        task_name=(fields.get(b"task") or b"").decode(),
                        task_id=payload.get("task_id"),
                        args=payload.get("args", []),
                        kwargs=payload.get("kwargs", {}),
                        labels=payload.get("labels", {}),
                        enqueued_ms=_entry_ms(entry_id),
                    ),
                )
        messages.sort(key=lambda m: (m.enqueued_ms, m.id))
        return messages[:limit]

    async def pause(self, queue: str) -> None:
        """
        Stop workers from taking messages of a queue (kicks still work).

        :param queue: queue name.
        """
        await self._redis.hset(self._engine.meta_key(queue), {"paused": "1"})
        await self._redis.execute("SADD", self._engine.index_key, queue)
        await self.app.cluster.control.publish("queue.paused", queue=queue, paused=True)

    async def resume(self, queue: str) -> None:
        """
        Let workers take messages of a queue again.

        :param queue: queue name.
        """
        await self._redis.hdel(self._engine.meta_key(queue), "paused")
        await self.app.cluster.control.publish(
            "queue.paused",
            queue=queue,
            paused=False,
        )

    async def purge(self, queue: str) -> int:
        """
        Delete every waiting and delayed message of a queue.

        Messages being processed right now finish normally.

        :param queue: queue name.
        :return: number of deleted messages.
        """
        removed = 0
        for stream in await self._streams(queue):
            pending: set[bytes] = set()
            try:
                reply = await self._redis.execute(
                    "XPENDING",
                    stream,
                    GROUP,
                    "-",
                    "+",
                    100_000,
                )
                pending = {item[0] for item in reply}
            except Exception:
                pending = set()
            start = "-"
            while True:
                entries = await self._redis.xrange(stream, start, "+", count=1000)
                ids = [entry_id for entry_id, _ in entries if entry_id not in pending]
                if ids:
                    removed += await self._redis.xdel(stream, *ids)
                if len(entries) < 1000:
                    break
                last = entries[-1][0].decode()
                ms, seq = last.split("-")
                start = f"{ms}-{int(seq) + 1}"
        delayed_key = self._engine.delayed_key(queue)
        removed += await self._redis.zcard(delayed_key)
        await self._redis.delete(delayed_key)
        return removed

    async def delete_message(self, queue: str, message_id: str) -> bool:
        """
        Delete one waiting message.

        :param queue: queue name.
        :param message_id: stream entry id (from :meth:`peek`).
        :return: True if it was deleted.
        """
        for stream in await self._streams(queue):
            if await self._redis.xdel(stream, message_id):
                return True
        return False

    async def dead_letters(
        self,
        queue: str,
        limit: int = 50,
    ) -> builtins.list[DeadLetter]:
        """
        Dead letters of a queue, newest first.

        :param queue: queue name.
        :param limit: max entries.
        :return: dead letters.
        """
        entries = await self._redis.xrevrange(self._engine.dlq_key(queue), count=limit)
        letters = []
        for entry_id, fields in entries:
            data = fields.get(b"data", b"")
            letters.append(
                DeadLetter(
                    id=entry_id.decode(),
                    queue=queue,
                    task_name=fields.get(b"task", b"").decode(),
                    task_id=_decode_message(data).get("task_id"),
                    reason=fields.get(b"reason", b"").decode(),
                    error=fields.get(b"error", b"").decode(),
                    at_ms=int(fields.get(b"at", b"0") or 0),
                    stream=fields.get(b"stream", b"").decode(),
                    data=data,
                ),
            )
        return letters

    async def replay_dead(
        self,
        queue: str,
        ids: builtins.list[str] | None = None,
    ) -> int:
        """
        Send dead letters back to their queue.

        :param queue: queue name.
        :param ids: dead-letter ids; None replays all.
        :return: number of replayed messages.
        """
        dlq = self._engine.dlq_key(queue)
        replayed = 0
        while True:
            entries = await self._redis.xrange(dlq, count=500)
            if ids is not None:
                wanted = set(ids)
                entries = [e for e in entries if e[0].decode() in wanted]
            if not entries:
                return replayed
            for entry_id, fields in entries:
                task = fields.get(b"task", b"").decode()
                data = _fresh_payload(fields.get(b"data", b""))
                _, stream, _ = self._engine.route(task, {"queue": queue})
                await self._ensure_group(stream)
                pipe = self._redis.pipeline(transaction=True)
                pipe.add("XADD", stream, "*", "data", data, "task", task)
                pipe.add("XDEL", dlq, entry_id)
                pipe.add("SADD", self._engine.streams_key(queue), stream)
                await pipe.execute()
                replayed += 1
            if ids is not None:
                return replayed

    async def _ensure_group(self, stream: str) -> None:
        try:
            await self._redis.xgroup_create(stream, GROUP, "0", mkstream=True)
        except Exception:
            return

    async def purge_dead(self, queue: str) -> int:
        """
        Delete all dead letters of a queue.

        :param queue: queue name.
        :return: number deleted.
        """
        dlq = self._engine.dlq_key(queue)
        count = await self._redis.xlen(dlq)
        await self._redis.delete(dlq)
        return count

    async def move(self, source: str, target: str) -> int:
        """
        Move every waiting message of one queue to another.

        :param source: queue to empty.
        :param target: queue to fill.
        :return: number of moved messages.
        """
        moved = 0
        for message in await self.peek(source, limit=1_000_000):
            _, stream, _ = self._engine.route(message.task_name, {"queue": target})
            raw = await self._redis.xrange(message.stream, message.id, message.id)
            if not raw:
                continue
            fields = raw[0][1]
            await self._ensure_group(stream)
            pipe = self._redis.pipeline(transaction=True)
            pipe.add(
                "XADD",
                stream,
                "*",
                "data",
                fields[b"data"],
                "task",
                message.task_name,
            )
            pipe.add("XDEL", message.stream, message.id)
            pipe.add("SADD", self._engine.streams_key(target), stream)
            pipe.add("SADD", self._engine.index_key, target)
            await pipe.execute()
            moved += 1
        return moved
