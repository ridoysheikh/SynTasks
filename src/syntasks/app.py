"""
The SynTasks app object.

One object holds the config, Redis, queues, results, schedules and
channels of an application.

Example::

    from syntasks import SynTasks

    app = SynTasks(timeout="5m", retries=0, max_tasks=None)  # + syntasks.toml

    @app.task(timeout="30s", retries=3, max_tasks=20)
    async def charge(customer_id: str, cents: int) -> None: ...

Run workers with ``syntasks worker``.
"""

import asyncio
import os
import weakref
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar

from syntasks.abc.broker import AsyncBroker
from syntasks.cluster import Cluster, NodeInfo
from syntasks.config import Config, ConfigError, load_config
from syntasks.config.schema import QueueConfig, RedisConfig
from syntasks.decor import AsyncSynTasksDecoratedTask
from syntasks.kicker import AsyncKicker
from syntasks.message import BrokerMessage
from syntasks.modules import Declarations
from syntasks.options import (
    REQUIRED,
    OptionLayer,
    TaskOptions,
    parse_layer,
    resolve_chain,
)
from syntasks.queues import QueueEngine, QueueManager
from syntasks.redis.client import Redis
from syntasks.redis.connection import ConnectionOptions
from syntasks.redis.pool import ConnectionPool, SentinelConnectionPool
from syntasks.results import AppResultBackend
from syntasks.results.groups import TaskGroup

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import AsyncGenerator

    from syntasks.acks import AckableMessage

__all__ = ["SynTasks", "create_redis"]


def create_redis(config: RedisConfig) -> Redis:
    """
    Build a Redis client from the ``[redis]`` settings.

    :param config: Redis settings.
    :return: client.
    """
    options = ConnectionOptions.from_url(
        config.url,
        connect_timeout=config.connect_timeout,
        socket_timeout=config.socket_timeout,
    )
    if config.sentinel is not None:
        sentinels = []
        for address in config.sentinel.nodes:
            host, _, port = address.rpartition(":")
            if not host:
                raise ConfigError(
                    detail=f"redis.sentinel.nodes: invalid address {address!r}.",
                )
            sentinels.append((host, int(port)))
        return Redis(
            SentinelConnectionPool(
                config.sentinel.service,
                sentinels,
                options,
                sentinel_options=ConnectionOptions(
                    username=config.sentinel.username,
                    password=config.sentinel.password,
                    connect_timeout=config.connect_timeout,
                    socket_timeout=config.socket_timeout,
                ),
                max_connections=config.pool_size,
            ),
        )
    return Redis(ConnectionPool(options, max_connections=config.pool_size))


class SynTasks(Declarations, AsyncBroker):  # type: ignore[misc]
    """
    A SynTasks application.

    :param config: a :class:`Config`, a path to a TOML file, or None to look
        for ``syntasks.toml`` / ``pyproject.toml`` (defaults if none).
    :param name: override ``[app] name``.
    :param redis_url: override ``[redis] url``.
    :param profile: config profile to apply.
    :param redis: share an existing Redis client.
    :param search_config: look for a config file when ``config`` is None.
    :param options: task options every task, schedule and channel starts
        from (the root of the module chain), e.g. ``timeout="5m",
        retries=0, max_tasks=None``.
    """

    _instances: ClassVar["weakref.WeakSet[SynTasks]"] = weakref.WeakSet()
    result_backend: AppResultBackend  # type: ignore[assignment]

    def __init__(
        self,
        config: Config | str | os.PathLike[str] | None = None,
        *,
        name: str | None = None,
        redis_url: str | None = None,
        profile: str | None = None,
        redis: Redis | None = None,
        search_config: bool = True,
        **options: Any,
    ) -> None:
        super().__init__()
        self._init_declarations("SynTasks()", options)
        if isinstance(config, Config):
            resolved = config
        else:
            resolved = load_config(config, profile=profile, search=search_config)
        if name is not None:
            resolved.app.name = name
        if redis_url is not None:
            resolved.redis.url = redis_url
        self.config = resolved
        self._owns_redis = redis is None
        self.redis = redis or create_redis(resolved.redis)
        self.cluster = Cluster(
            self.redis,
            resolved.app.name,
            dead_after_ms=int(resolved.node.dead_after * 1000),
        )
        self.keys = self.cluster.keys
        # Option chain of every task declared in code, by name.
        self.declared_tasks: dict[str, list[OptionLayer]] = {}
        self._options_cache: dict[str, TaskOptions] = {}
        # Options adopted from a newer deploy's manifest (see config.cluster).
        self.adopted_options: dict[str, TaskOptions] = {}
        self.declared_queues: dict[str, QueueConfig] = {}
        self.engine = QueueEngine(self)
        self.queues = QueueManager(self)
        self.result_backend = AppResultBackend(self)
        self._started = False
        self._runtime: Any = None
        self._setup_features()
        SynTasks._instances.add(self)

    def _setup_features(self) -> None:
        """Attach schedules, channels, logs and task control."""
        from syntasks.channels import ChannelManager  # noqa: PLC0415
        from syntasks.logs import LogManager  # noqa: PLC0415
        from syntasks.results.completion import Completion  # noqa: PLC0415
        from syntasks.results.events import TaskEvents  # noqa: PLC0415
        from syntasks.scheduler import ScheduleManager  # noqa: PLC0415
        from syntasks.worker.control import TaskControl  # noqa: PLC0415

        self.completion = Completion(self)
        self.events = TaskEvents(self)
        self.schedules = ScheduleManager(self)
        self.channels = ChannelManager(self)
        self.logs = LogManager(self)
        self.tasks = TaskControl(self)

    def __repr__(self) -> str:
        return f"<SynTasks {self.config.app.name!r} {len(self.get_all_tasks())} tasks>"

    # -- declarations (see syntasks.modules) ------------------------------

    def _broker_for_tasks(self) -> Any:
        return self

    def _declare_task(
        self,
        task: AsyncSynTasksDecoratedTask[Any, Any],
        layer: OptionLayer,
    ) -> None:
        self.register_declared(task, [self.layer, layer])

    def _extend(self, module: Any) -> None:
        module.attach_to(self, [self.layer])

    def register_declared(
        self,
        task: AsyncSynTasksDecoratedTask[Any, Any],
        chain: list[OptionLayer],
    ) -> None:
        """Register a task declared on this app or an attached module."""
        name = task.task_name
        existing = self.local_task_registry.get(name)
        if existing is not None and existing is not task:
            raise ConfigError(detail=f"Two tasks are named {name!r}.")
        options = resolve_chain(chain, what=name)
        self.declared_tasks[name] = chain
        self._options_cache[name] = options
        task.broker = self
        self._register_task(name, task)
        if task.declared_schedules:
            self.schedules.declare(name, task.declared_schedules)
        if task.declared_runtime is not None:
            self.schedules.declare_template(name, task.declared_runtime)

    def _declare_queue(self, name: str, options: QueueConfig) -> None:
        self.declare_queue(name, options)

    def declare_queue(self, name: str, options: QueueConfig) -> None:
        """Register a queue declared on this app or an attached module."""
        existing = self.declared_queues.get(name)
        if existing is not None and existing != options:
            raise ConfigError(
                detail=f"Queue {name!r} is declared twice with different options.",
            )
        self.declared_queues[name] = options

    def _declare_channel(
        self,
        pattern: str,
        func: Callable[..., Any],
        options: dict[str, Any],
    ) -> None:
        self.register_channel(pattern, func, options, [self.layer])

    def register_channel(
        self,
        pattern: str,
        func: Callable[..., Any],
        options: dict[str, Any],
        chain: list[OptionLayer],
    ) -> None:
        """Register a channel handler declared here or in an attached module."""
        limits = {k: options.pop(k) for k in list(options) if k in REQUIRED}
        layer = parse_layer(f"channel {pattern}", limits)
        resolved = resolve_chain([*chain, layer], what=f"channel {pattern}")
        self.channels.handler(
            pattern,
            max_lifetime=resolved.timeout,
            max_restarts=resolved.retries,
            max_tasks=resolved.max_tasks,
            **options,
        )(func)

    def queue_options(self, name: str) -> QueueConfig:
        """
        Options of a queue (defaults if it wasn't declared).

        :param name: queue name.
        :return: queue options.
        """
        return self.declared_queues.get(name) or QueueConfig()

    def options_for(self, task_name: str) -> TaskOptions:
        """
        Effective options of a task.

        :param task_name: task name.
        :return: options.
        """
        adopted = self.adopted_options.get(task_name)
        if adopted is not None:
            return adopted
        cached = self._options_cache.get(task_name)
        if cached is None:
            chain = self.declared_tasks.get(task_name, [self.layer])
            cached = resolve_chain(chain, what=task_name, required=False)
            self._options_cache[task_name] = cached
        return cached

    def reload_options(self) -> None:
        """Forget cached options of tasks that aren't declared here."""
        self._options_cache = {
            name: options
            for name, options in self._options_cache.items()
            if name in self.declared_tasks
        }

    def send(self, task_name: str, *args: Any, **kwargs: Any) -> Any:
        """
        Kick a task by name, without importing its function.

        :param task_name: task name, e.g. ``"billing.tasks:charge"``.
        :param args: positional arguments.
        :param kwargs: keyword arguments.
        :return: awaitable that returns the task handle.
        """
        return AsyncKicker(task_name, self, {}).kiq(*args, **kwargs)

    def group(self, group_id: str | None = None) -> "TaskGroup":
        """
        A group of tasks to wait for together.

        :param group_id: id (default random).
        :return: task group.
        """
        return TaskGroup(self, group_id)

    async def batch(self, batch_id: str, *, owner: str | None = None) -> Any:
        """
        A named, persistent group of tasks (a campaign); created if new.

        :param batch_id: batch id.
        :param owner: who it belongs to.
        :return: :class:`~syntasks.results.completion.Batch`.
        """
        from syntasks.results.completion import Batch  # noqa: PLC0415

        batch = Batch(self, batch_id)
        await batch._create(owner)  # noqa: SLF001
        return batch

    def kicker(self, task_name: str) -> AsyncKicker[Any, Any]:
        """
        Kicker for a task by name (to set queue, delay, ... before sending).

        :param task_name: task name.
        :return: kicker.
        """
        return AsyncKicker(task_name, self, {})

    # -- broker protocol -----------------------------------------------------

    async def kick(self, message: BrokerMessage) -> None:
        """
        Put a message into its queue.

        :param message: formatted message.
        """
        await self.engine.kick(message)

    def listen(self) -> "AsyncGenerator[AckableMessage, None]":
        """
        Messages for this worker process.

        :return: async generator of deliveries.
        """
        return self.engine.listen()

    async def startup(self) -> None:
        """Start the app (event handlers, middlewares, result backend)."""
        await super().startup()
        await self.cluster.clock.sync()
        self._started = True

    async def shutdown(self) -> None:
        """Stop the app and close its connections."""
        await super().shutdown()
        await self.channels.close()
        if self._owns_redis:
            await self.redis.close()
        self._started = False

    # -- running workers -------------------------------------------------------

    async def run_worker(
        self,
        stop: asyncio.Event | None = None,
        *,
        processes: int = 1,
        process_index: int = 0,
    ) -> None:
        """
        Run a worker in this process until ``stop`` is set.

        ``syntasks worker`` uses this in each worker process; it is also
        handy in tests and to embed a worker in another program.

        :param stop: event that stops the worker.
        :param processes: worker processes on this machine (for limits).
        :param process_index: index of this process.
        """
        from syntasks.worker.runtime import WorkerRuntime  # noqa: PLC0415

        runtime = WorkerRuntime(self, processes=processes, process_index=process_index)
        self._runtime = runtime
        try:
            await runtime.run(stop or asyncio.Event())
        finally:
            self._runtime = None

    def receiver_ready(self) -> bool:
        """Whether a worker started with :meth:`run_worker` is taking work."""
        runtime = self._runtime
        return runtime is not None and runtime.receiver is not None

    async def nodes(self) -> list[NodeInfo]:
        """Worker processes registered in the cluster."""
        return await self.cluster.nodes.list()
