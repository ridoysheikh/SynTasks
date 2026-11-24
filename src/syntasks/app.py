"""
The SynTasks app object.

One object holds the config, Redis, queues, results, schedules and
channels of an application.

Example::

    from syntasks import SynTasks

    app = SynTasks()  # reads syntasks.toml if present

    @app.task(timeout="30s", retries=3)
    async def charge(customer_id: str, cents: int) -> None: ...

Run workers with ``syntasks worker``.
"""

import asyncio
import os
import weakref
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar, ParamSpec, TypeVar, overload

from syntasks.abc.broker import AsyncBroker
from syntasks.cluster import Cluster, NodeInfo
from syntasks.config import Config, ConfigError, TaskConfig, load_config
from syntasks.config.schema import RedisConfig
from syntasks.decor import AsyncSynTasksDecoratedTask
from syntasks.kicker import AsyncKicker
from syntasks.message import BrokerMessage
from syntasks.options import (
    OPTION_NAMES,
    TaskOptions,
    parse_code_options,
    resolve_options,
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

_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")


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


class SynTasks(AsyncBroker):
    """
    A SynTasks application.

    :param config: a :class:`Config`, a path to a TOML file, or None to look
        for ``syntasks.toml`` / ``pyproject.toml`` (defaults if none).
    :param name: override ``[app] name``.
    :param redis_url: override ``[redis] url``.
    :param profile: config profile to apply.
    :param redis: share an existing Redis client.
    :param search_config: look for a config file when ``config`` is None.
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
    ) -> None:
        super().__init__()
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
        self._code_options: dict[str, TaskConfig] = {}
        self._options_cache: dict[str, TaskOptions] = {}
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
        from syntasks.scheduler import ScheduleManager  # noqa: PLC0415
        from syntasks.worker.control import TaskControl  # noqa: PLC0415

        self.schedules = ScheduleManager(self)
        self.channels = ChannelManager(self)
        self.logs = LogManager(self)
        self.tasks = TaskControl(self)

    def __repr__(self) -> str:
        return f"<SynTasks {self.config.app.name!r} {len(self.get_all_tasks())} tasks>"

    # -- tasks -----------------------------------------------------------

    @overload
    def task(
        self,
        task_name: Callable[_FuncParams, _ReturnType],
        **kwargs: Any,
    ) -> AsyncSynTasksDecoratedTask[_FuncParams, _ReturnType]:  # pragma: no cover
        ...

    @overload
    def task(
        self,
        task_name: str | None = None,
        **kwargs: Any,
    ) -> Callable[
        [Callable[_FuncParams, _ReturnType]],
        AsyncSynTasksDecoratedTask[_FuncParams, _ReturnType],
    ]:  # pragma: no cover
        ...

    def task(  # type: ignore[misc]
        self,
        task_name: str | Callable[..., Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Turn a function into a task.

        Keyword arguments that are task options (``queue``, ``timeout``,
        ``retries``, ``concurrency``, ``rate``, ... see
        :class:`syntasks.config.TaskConfig`) configure the task; ``schedule``
        attaches schedules; anything else becomes a label.

        :param task_name: custom name (default ``module:function``).
        :param kwargs: options, ``schedule`` and labels.
        :return: decorated task, or a decorator.
        """
        schedule = kwargs.pop("schedule", None)
        options = {k: kwargs.pop(k) for k in list(kwargs) if k in OPTION_NAMES}
        hint = task_name if isinstance(task_name, str) else "<task>"
        parsed = parse_code_options(hint, options)

        def register(decorated: AsyncSynTasksDecoratedTask[Any, Any]) -> Any:
            self._code_options[decorated.task_name] = parsed
            self._options_cache.pop(decorated.task_name, None)
            self.options_for(decorated.task_name)  # validate the combination
            if schedule is not None:
                self.schedules.declare(decorated.task_name, schedule)
            return decorated

        if callable(task_name):
            return register(super().task(task_name, **kwargs))
        decorator = super().task(task_name, **kwargs)

        def inner(func: Callable[..., Any]) -> Any:
            return register(decorator(func))

        return inner

    def channel(self, pattern: str, **options: Any) -> Any:
        """
        Register a long-running channel handler (see :mod:`syntasks.channels`).

        :param pattern: channel name pattern, e.g. ``"room:{room_id}"``.
        :param options: ``idle_timeout``, ``max_lifetime``, ``max_restarts``.
        :return: decorator.
        """
        return self.channels.handler(pattern, **options)

    def options_for(self, task_name: str) -> TaskOptions:
        """
        Effective options of a task.

        :param task_name: task name.
        :return: options.
        """
        cached = self._options_cache.get(task_name)
        if cached is None:
            cached = resolve_options(
                self.config.defaults,
                self._code_options.get(task_name),
                self.config.tasks.get(task_name),
            )
            self._options_cache[task_name] = cached
        return cached

    def reload_options(self) -> None:
        """Forget cached task options (after the config changed)."""
        self._options_cache.clear()

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
