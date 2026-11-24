"""
Opt-in log capture per task.

Tasks with ``capture_logs = true`` have every log record emitted while
they run (also from sub-coroutines and sync code in threads) stored in a
Redis stream ``syntasks:{<app>}:log:<task_id>``, bounded in lines and
record size and kept for ``logging.capture_retention``. Other tasks pay
one context-variable lookup per log record.

Read or follow them with ``app.logs.read(task_id, follow=True)`` or
``syntasks logs <task_id> -f``.
"""

import asyncio
import contextlib
import contextvars
import logging
import threading
import time
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from syntasks.redis.errors import RedisError

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks
    from syntasks.message import SynTasksMessage
    from syntasks.options import TaskOptions

__all__ = ["LogManager", "LogRecord"]

_current: contextvars.ContextVar["_Sink | None"] = contextvars.ContextVar(
    "syntasks_log_sink",
    default=None,
)
_FLUSH_EVERY = 0.2
_FLUSH_SIZE = 100
_handler_lock = threading.Lock()
_handler_installed = False


@dataclass(frozen=True, kw_only=True)
class LogRecord:
    """One captured log record."""

    id: str
    time: float
    level: str
    logger: str
    message: str
    exception: str | None = None
    attempt: int = 0


class _CaptureHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        sink = _current.get()
        if sink is None or record.levelno < sink.level:
            return
        try:
            message = record.getMessage()
        except Exception:
            message = str(record.msg)
        exception = None
        if record.exc_info:
            exception = logging.Formatter().formatException(record.exc_info)
        sink.add(record.created, record.levelname, record.name, message, exception)


def _install_handler() -> None:
    global _handler_installed  # noqa: PLW0603
    with _handler_lock:
        if _handler_installed:
            return
        logging.getLogger().addHandler(_CaptureHandler(level=logging.NOTSET))
        _handler_installed = True


class _Sink:
    def __init__(
        self,
        app: "SynTasks",
        key: str,
        attempt: int,
        level: int,
        max_record: int,
        max_lines: int,
    ) -> None:
        self.app = app
        self.key = key
        self.attempt = attempt
        self.level = level
        self.max_record = max_record
        self.max_lines = max_lines
        self.pending: deque[dict[str, str]] = deque()
        self.lock = threading.Lock()
        self.wakeup = asyncio.Event()
        self.loop = asyncio.get_running_loop()

    def add(
        self,
        created: float,
        level: str,
        name: str,
        message: str,
        exception: str | None,
    ) -> None:
        if len(message) > self.max_record:
            message = message[: self.max_record] + " …[truncated]"
        entry = {
            "t": repr(created),
            "level": level,
            "logger": name,
            "msg": message,
            "attempt": str(self.attempt),
        }
        if exception:
            entry["exc"] = exception[: self.max_record]
        with self.lock:
            self.pending.append(entry)
            size = len(self.pending)
        if size >= _FLUSH_SIZE:
            with contextlib.suppress(RuntimeError):
                self.loop.call_soon_threadsafe(self.wakeup.set)

    async def flush(
        self,
        *,
        final: bool = False,
        retention_ms: int | None = None,
    ) -> None:
        with self.lock:
            batch = list(self.pending)
            self.pending.clear()
        if not batch and not final:
            return
        pipe = self.app.redis.pipeline()
        for entry in batch:
            args: list[Any] = ["XADD", self.key, "MAXLEN", "~", self.max_lines, "*"]
            for name, value in entry.items():
                args += [name, value]
            pipe.add(*args)
        if final:
            pipe.add("XADD", self.key, "*", "end", "1", "attempt", str(self.attempt))
            if retention_ms:
                pipe.add("PEXPIRE", self.key, retention_ms)
        try:
            await pipe.execute(raise_on_error=False)
        except (RedisError, OSError) as exc:
            logging.getLogger("syntasks.logs").debug("Log flush failed: %s", exc)

    async def run(self) -> None:
        while True:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.wakeup.wait(), _FLUSH_EVERY)
            self.wakeup.clear()
            await self.flush()


class _Capture:
    """Async context manager that captures the current run's logs."""

    def __init__(self, manager: "LogManager", message: "SynTasksMessage") -> None:
        self.manager = manager
        self.message = message
        self.sink: _Sink | None = None
        self.token: contextvars.Token[Any] | None = None
        self.flusher: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Self:
        _install_handler()
        config = self.manager.app.config.logging
        self.sink = _Sink(
            self.manager.app,
            self.manager.key(self.message.task_id),
            int(self.message.labels.get("_attempt", 0) or 0),
            (
                logging.getLevelName(config.capture_level.upper())
                if isinstance(logging.getLevelName(config.capture_level.upper()), int)
                else logging.INFO
            ),
            config.capture_max_record,
            config.capture_max_lines,
        )
        self.token = _current.set(self.sink)
        self.flusher = asyncio.create_task(self.sink.run())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self.token is not None:
            with contextlib.suppress(ValueError):
                _current.reset(self.token)
            self.token = None
        if self.flusher is not None:
            self.flusher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.flusher
        if self.sink is not None:
            retention = self.manager.app.config.logging.capture_retention
            await self.sink.flush(final=True, retention_ms=int(retention * 1000))


class _NoCapture:
    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None


class LogManager:
    """Captured task logs (``app.logs``)."""

    def __init__(self, app: "SynTasks") -> None:
        self.app = app

    def key(self, task_id: str) -> str:
        """Stream holding a task's captured logs."""
        return self.app.keys("log", task_id)

    def capture(self, message: "SynTasksMessage", options: "TaskOptions") -> Any:
        """
        Context manager the worker wraps each run in.

        :param message: the run's message.
        :param options: the task's options.
        :return: async context manager.
        """
        if not options.capture_logs:
            return _NoCapture()
        return _Capture(self, message)

    @staticmethod
    def _record(entry_id: bytes, fields: dict[bytes, bytes]) -> LogRecord:
        def text(name: bytes) -> str:
            return fields.get(name, b"").decode(errors="replace")

        return LogRecord(
            id=entry_id.decode(),
            time=float(text(b"t") or 0),
            level=text(b"level"),
            logger=text(b"logger"),
            message=text(b"msg"),
            exception=text(b"exc") or None,
            attempt=int(text(b"attempt") or 0),
        )

    async def read(
        self,
        task_id: str,
        *,
        follow: bool = False,
        timeout: float | None = None,
    ) -> AsyncIterator[LogRecord]:
        """
        Captured log records of a task.

        :param task_id: task id.
        :param follow: keep waiting for new records until the run ends.
        :param timeout: stop following after this many seconds.
        :yield: log records, oldest first.
        """
        key = self.key(task_id)
        last = "0"
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if follow:
                block = 1000
                if deadline is not None:
                    left = deadline - time.monotonic()
                    if left <= 0:
                        return
                    block = max(min(block, int(left * 1000)), 1)
                batches = await self.app.redis.xread(
                    {key: last},
                    count=500,
                    block=block,
                )
                entries = batches[0][1] if batches else []
            else:
                entries = await self.app.redis.xrange(
                    key,
                    f"({last}" if last != "0" else "-",
                    "+",
                    count=500,
                )
            for entry_id, fields in entries:
                last = entry_id.decode()
                if fields.get(b"end") == b"1":
                    # A retry may append more; stop following at the final end.
                    if follow and not await self._more_after(key, last):
                        return
                    continue
                yield self._record(entry_id, fields)
            if not follow and len(entries) < 500:
                return

    async def _more_after(self, key: str, last: str) -> bool:
        return bool(await self.app.redis.xrange(key, f"({last}", "+", count=1))

    async def delete(self, task_id: str) -> None:
        """
        Delete a task's captured logs.

        :param task_id: task id.
        """
        await self.app.redis.delete(self.key(task_id))
