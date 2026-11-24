"""
One worker process: fetch, run, and take part in cluster roles.

Besides running tasks, every worker process:

* registers in the node registry and sends heartbeats;
* listens to the control bus (pause, cancel, wake-ups);
* keeps its in-flight messages and slots alive;
* competes for the leased roles: the **dispatcher** (delayed messages and
  schedules) and the **janitor** (cleanup), so exactly one process in the
  cluster does each at a time;
* hosts channel handlers;
* watches its memory and its event loop.
"""

import asyncio
import contextlib
import functools
import logging
import math
import os
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from syntasks import __version__
from syntasks.acks import AcknowledgeType
from syntasks.cluster import Janitor, LeasedRole, NodeInfo
from syntasks.cluster.lease import Lease
from syntasks.config.cluster import publish_cluster_config
from syntasks.redis.errors import RedisError
from syntasks.worker.isolation import process_rss, set_memory_limit
from syntasks.worker.receiver import AppReceiver

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks

__all__ = ["EXIT_RECYCLE", "WorkerRuntime"]

logger = logging.getLogger("syntasks.worker")

# Exit code asking the supervisor for a fresh process (memory, tainted).
EXIT_RECYCLE = 75
_CONSUMER_CLEANUP_EVERY = 60.0


class WorkerRuntime:
    """Runs one worker process of a SynTasks app."""

    def __init__(
        self,
        app: "SynTasks",
        *,
        processes: int = 1,
        process_index: int = 0,
        group: str = "",
        own_process: bool = False,
    ) -> None:
        self.app = app
        self.processes = max(processes, 1)
        self.process_index = process_index
        self.group = group or f"{os.getpid()}"
        self.own_process = own_process
        self.exit_code = 0
        self.receiver: AppReceiver | None = None
        self.stop_event: asyncio.Event | None = None
        self.dispatch_wake = asyncio.Event()
        self.started = time.time()
        self._loop_tick = time.monotonic()
        self._background: list[asyncio.Task[Any]] = []
        self._next_consumer_cleanup = 0.0
        self.dispatchers: list[LeasedRole] = []
        self.janitor_role: LeasedRole | None = None

    # -- lifecycle ---------------------------------------------------------------

    def taint(self, reason: str) -> None:
        """
        Stop taking work and ask for a fresh process.

        :param reason: why, for the log.
        """
        logger.error("%s", reason)
        self.exit_code = EXIT_RECYCLE
        if self.stop_event is not None:
            self.stop_event.set()

    def _per_process(self, total: int) -> int:
        return max(math.ceil(total / self.processes), 1)

    def node_info(self) -> NodeInfo:
        """What this process reports in its heartbeat."""
        running = self.receiver.running if self.receiver else {}
        config = self.app.config
        return NodeInfo(
            node_id=self.app.cluster.node_id,
            group=self.group,
            version=__version__,
            revision=config.app.revision,
            started=self.started,
            queues=list(config.worker.queues),
            tags=list(config.worker.tags),
            capacity=self._per_process(config.worker.max_async_tasks),
            running=len(running),
            running_tasks={
                task_id: item.task_name for task_id, item in running.items()
            },
            channels=self.app.channels.hosted_count(),
        )

    async def run(self, stop: asyncio.Event) -> None:
        """
        Run until ``stop`` is set (or the process is tainted).

        :param stop: stop event.
        """
        app = self.app
        config = app.config
        self.stop_event = stop
        app.is_worker_process = True
        if (
            self.own_process
            and config.worker.memory_hard_limit
            and set_memory_limit(config.worker.memory_hard_limit)
        ):
            logger.info(
                "Memory hard limit: %d bytes.",
                config.worker.memory_hard_limit,
            )
        app.engine.serve(
            list(config.worker.queues),
            tags=list(config.worker.tags),
            processes=self.processes,
            fetch_count=max(config.worker.prefetch, 1),
            block=config.worker.xread_block,
            claim_idle=config.worker.claim_idle,
        )
        await app.startup()
        try:
            if await publish_cluster_config(
                app.redis,
                app.keys("cfg", "cluster"),
                config,
                app.cluster.node_id,
            ):
                app.reload_options()
            await app.engine.ensure_groups()
            await app.schedules.sync_static()
            await app.cluster.nodes.beat(self.node_info())
            executor = ThreadPoolExecutor(
                max_workers=config.worker.max_threads,
                thread_name_prefix="syntasks-sync",
            )
            receiver = AppReceiver(
                app,
                self,
                executor=executor,
                max_async_tasks=self._per_process(config.worker.max_async_tasks),
                max_prefetch=config.worker.prefetch,
                ack_type=AcknowledgeType(config.worker.ack),
                run_startup=False,
                max_tasks_to_execute=config.worker.max_tasks_per_process or None,
                wait_tasks_timeout=config.worker.shutdown_grace,
            )
            self.receiver = receiver
            self._start_background()
            logger.info(
                "Worker %s serving %s (%d slots).",
                app.cluster.node_id,
                ", ".join(config.worker.queues),
                self._per_process(config.worker.max_async_tasks),
            )
            try:
                await receiver.listen(stop)
            finally:
                executor.shutdown(wait=False, cancel_futures=True)
        finally:
            await self._stop_background()
            with contextlib.suppress(RedisError, OSError):
                await app.cluster.nodes.remove(app.cluster.node_id)
            await app.shutdown()

    def _start_background(self) -> None:
        app = self.app
        config = app.config
        control = app.cluster.control
        control.on("task.cancel", self._on_cancel)
        control.on("queue.paused", self._on_paused)
        control.on("delayed", self._on_wake)
        control.on("schedule.changed", self._on_wake)
        control.on("node.drain", self._on_drain)
        tasks = [
            control.run(),
            self._heartbeat(),
            self._refresh_inflight(),
            self._watch_memory(),
            self._loop_ticker(),
            app.channels.host(self),
        ]
        janitor = Janitor(app.redis, app.keys, app.cluster.nodes)
        janitor.add_cleanup(app.engine.unpark_free)
        janitor.add_cleanup(self._cleanup_consumers)
        app.schedules.add_janitor_hooks(janitor)
        app.channels.add_janitor_hooks(janitor)
        self.janitor_role = LeasedRole(
            "janitor",
            app.cluster.lease("janitor", int(config.node.dead_after * 1000)),
            janitor.work,
        )
        tasks.append(self.janitor_role.run())
        if config.scheduler.enabled:
            for shard in range(max(config.scheduler.shards, 1)):
                role = LeasedRole(
                    f"dispatcher:{shard}",
                    app.cluster.lease(
                        f"dispatcher:{shard}",
                        int(config.scheduler.lease_ttl * 1000),
                    ),
                    functools.partial(self._dispatch, shard=shard),
                )
                self.dispatchers.append(role)
                tasks.append(role.run())
        self._background = [asyncio.create_task(coro) for coro in tasks]
        if config.worker.loop_block_warn > 0 or config.worker.loop_block_kill > 0:
            threading.Thread(
                target=self._watchdog,
                name="syntasks-loop-watchdog",
                daemon=True,
            ).start()

    async def _stop_background(self) -> None:
        tasks, self._background = self._background, []
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    # -- control events -----------------------------------------------------------

    def _on_cancel(self, event: dict[str, Any]) -> None:
        if self.receiver is not None:
            self.receiver.cancel(str(event.get("task_id")))

    async def _on_paused(self, _event: dict[str, Any]) -> None:
        await self.app.engine.refresh_paused(force=True)

    def _on_wake(self, _event: dict[str, Any]) -> None:
        self.dispatch_wake.set()

    def _on_drain(self, event: dict[str, Any]) -> None:
        target = event.get("node_id")
        if target in (self.app.cluster.node_id, self.group, "*"):
            logger.warning("Drain requested; finishing running tasks and stopping.")
            if self.stop_event is not None:
                self.stop_event.set()

    # -- background loops -------------------------------------------------------------

    async def _heartbeat(self) -> None:
        interval = self.app.config.node.heartbeat
        while True:
            await asyncio.sleep(interval)
            try:
                await self.app.cluster.nodes.beat(self.node_info())
                await self.app.cluster.clock.maybe_sync()
            except (RedisError, OSError) as exc:
                logger.warning("Heartbeat failed: %s", exc)

    async def _refresh_inflight(self) -> None:
        engine = self.app.engine
        interval = min(engine.claim_idle_ms, engine.slot_ttl_ms) / 3000
        while True:
            await asyncio.sleep(interval)
            try:
                await engine.refresh_inflight()
            except (RedisError, OSError) as exc:
                logger.warning("Could not refresh running messages: %s", exc)

    async def _dispatch(self, lease: Lease, shard: int = 0) -> None:
        app = self.app
        max_sleep = app.config.scheduler.max_sleep
        while True:
            self.dispatch_wake.clear()
            next_delayed = await app.engine.move_delayed() if shard == 0 else None
            next_schedule = await app.schedules.dispatch(lease, shard)
            candidates = [t for t in (next_delayed, next_schedule) if t is not None]
            sleep = max_sleep
            if candidates:
                sleep = min(
                    max((min(candidates) - app.cluster.clock.now_ms()) / 1000, 0.0),
                    max_sleep,
                )
            if sleep > 0:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.dispatch_wake.wait(), sleep)

    async def _cleanup_consumers(self) -> None:
        """Delete stream consumers of processes that are gone and own nothing."""
        now = time.monotonic()
        if now < self._next_consumer_cleanup:
            return
        self._next_consumer_cleanup = now + _CONSUMER_CLEANUP_EVERY
        app = self.app
        alive = {node.node_id for node in await app.nodes() if node.alive}
        idle_ms = int(app.config.node.dead_after * 2000)
        for queue in await app.queues.names():
            for raw in await app.redis.smembers(app.engine.streams_key(queue)):
                stream = raw.decode()
                try:
                    consumers = await app.redis.execute(
                        "XINFO",
                        "CONSUMERS",
                        stream,
                        app.engine.group,
                    )
                except RedisError:
                    continue
                for consumer in consumers:
                    info = dict(zip(consumer[::2], consumer[1::2], strict=False))
                    name = info.get(b"name", b"").decode()
                    if (
                        name not in alive
                        and int(info.get(b"pending", 0)) == 0
                        and int(info.get(b"idle", 0)) > idle_ms
                    ):
                        await app.redis.execute(
                            "XGROUP",
                            "DELCONSUMER",
                            stream,
                            app.engine.group,
                            name,
                        )

    async def _watch_memory(self) -> None:
        config = self.app.config.worker
        if not config.memory_soft_limit:
            return
        while True:
            await asyncio.sleep(config.memory_check_interval)
            rss = process_rss()
            if rss is not None and rss > config.memory_soft_limit:
                self.taint(
                    f"Process uses {rss} bytes, above the soft limit of "
                    f"{config.memory_soft_limit}; draining and restarting it.",
                )
                return

    async def _loop_ticker(self) -> None:
        while True:
            self._loop_tick = time.monotonic()
            await asyncio.sleep(0.1)

    def _watchdog(self) -> None:  # pragma: no cover (thread timing)
        config = self.app.config.worker
        main_id = threading.main_thread().ident
        warned_at = 0.0
        while self._background:
            time.sleep(0.25)
            blocked = time.monotonic() - self._loop_tick
            if (
                config.loop_block_warn
                and blocked > config.loop_block_warn
                and warned_at == 0.0
            ):
                warned_at = blocked
                frame = sys._current_frames().get(main_id or 0)  # noqa: SLF001
                stack = "".join(traceback.format_stack(frame)) if frame else "?"
                logger.warning(
                    "Event loop blocked for %.1fs (sync code in an async task?). "
                    "Main thread stack:\n%s",
                    blocked,
                    stack,
                )
            if blocked < 0.5:
                warned_at = 0.0
            if (
                config.loop_block_kill
                and blocked > config.loop_block_kill
                and self.own_process
            ):
                logger.critical(
                    "Event loop blocked for %.1fs; restarting this process.",
                    blocked,
                )
                logging.shutdown()
                os._exit(EXIT_RECYCLE)
