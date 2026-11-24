import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import AsyncGenerator, Awaitable, Callable
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    ParamSpec,
    Self,
    TypeAlias,
    TypeVar,
    overload,
)
from uuid import uuid4

from syntasks.abc.middleware import SynTasksMiddleware
from syntasks.abc.serializer import SynTasksSerializer
from syntasks.acks import AckableMessage
from syntasks.decor import AsyncSynTasksDecoratedTask, build_decorated_task
from syntasks.events import SynTasksEvents
from syntasks.exceptions import TaskBrokerMismatchError
from syntasks.formatters.proxy_formatter import ProxyFormatter
from syntasks.message import BrokerMessage
from syntasks.result_backends.dummy import DummyResultBackend
from syntasks.serializers.json_serializer import JSONSerializer
from syntasks.state import SynTasksState
from syntasks.utils import maybe_awaitable
from syntasks.warnings import SynTasksDeprecationWarning

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.abc.formatter import SynTasksFormatter
    from syntasks.abc.result_backend import AsyncResultBackend

_T = TypeVar("_T")
_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")

EventHandler: TypeAlias = Callable[[SynTasksState], Awaitable[None] | None]

logger = getLogger("syntasks")


def default_id_generator() -> str:
    """
    Default task_id generator.

    This function is used to generate id's
    for tasks.

    :return: new task_id.
    """
    return uuid4().hex


class AsyncBroker(ABC):
    """
    Async broker.

    This abstract class must be implemented in order
    to get ability to send tasks to brokers
    in async mode.
    """

    global_task_registry: ClassVar[dict[str, AsyncSynTasksDecoratedTask[Any, Any]]] = {}

    def __init__(
        self,
        result_backend: "AsyncResultBackend[_T] | None" = None,
        task_id_generator: Callable[[], str] | None = None,
    ) -> None:
        if result_backend is None:
            result_backend = DummyResultBackend()
        else:
            warnings.warn(
                "Setting result backend with constructor is deprecated. "
                "Please use `with_result_backend` instead.",
                SynTasksDeprecationWarning,
                stacklevel=2,
            )
        if task_id_generator is None:
            task_id_generator = default_id_generator
        else:
            warnings.warn(
                "Setting id generator with constructor is deprecated. "
                "Please use `with_id_generator` instead.",
                SynTasksDeprecationWarning,
                stacklevel=2,
            )
        self.middlewares: list[SynTasksMiddleware] = []
        self.result_backend = result_backend
        self.decorator_class = AsyncSynTasksDecoratedTask
        self.serializer: SynTasksSerializer = JSONSerializer()
        self.formatter: SynTasksFormatter = ProxyFormatter(self)
        self.id_generator = task_id_generator
        self.local_task_registry: dict[str, AsyncSynTasksDecoratedTask[Any, Any]] = {}
        # Every event has a list of handlers.
        # Every handler is a function which takes state as a first argument.
        # And handler can be either sync or async.
        self.event_handlers: defaultdict[
            SynTasksEvents,
            list[Callable[[SynTasksState], Awaitable[None] | None]],
        ] = defaultdict(list)
        self.state = SynTasksState()
        self.custom_dependency_context: dict[Any, Any] = {}
        self.dependency_overrides: dict[Any, Any] = {}
        # True only if broker runs in worker process.
        self.is_worker_process = False
        # True only if broker runs in scheduler process.
        self.is_scheduler_process = False

    def find_task(self, task_name: str) -> AsyncSynTasksDecoratedTask[Any, Any] | None:
        """
        Returns task by name.

        This method should be used to get task by name.
        Instead of accessing `available_tasks` or `local_available_tasks` directly.

        It searches task by name in dict of tasks that
        were registered for this broker directly.
        If it fails, it checks global dict of all available tasks.

        :param task_name: name of a task.
        :returns: found task or None.
        """
        return self.local_task_registry.get(
            task_name,
        ) or self.global_task_registry.get(
            task_name,
        )

    def get_all_tasks(self) -> dict[str, AsyncSynTasksDecoratedTask[Any, Any]]:
        """
        Method to fetch all tasks available in broker.

        This method returns all tasks, globally and locally
        available in broker. With local tasks having higher priority.

        So, if you have two tasks with the same name,
        one registered in global registry and one registered
        in local registry, then local task will be returned.

        :return: dict of all tasks. Keys are task names, values are tasks.
        """
        return {**self.global_task_registry, **self.local_task_registry}

    def add_dependency_context(self, new_ctx: dict[Any, Any]) -> None:
        """
        Add first-level dependencies.

        Provided dict will be used to inject new dependencies
        in all dependency graph contexts.

        :param new_ctx: Additional context values for dependency injection.
        """
        self.custom_dependency_context.update(new_ctx)

    def add_middlewares(self, *middlewares: "SynTasksMiddleware") -> None:
        """
        Add a list of middlewares.

        You should call this method to set middlewares,
        since it saves current broker in all middlewares.

        :param middlewares: list of middlewares.
        """
        for middleware in middlewares:
            if not isinstance(middleware, SynTasksMiddleware):
                logger.warning(
                    "Middleware %s is not an instance of SynTasksMiddleware. "
                    "Skipping...",
                    middleware,
                )
                continue
            middleware.set_broker(self)
            self.middlewares.append(middleware)

    async def startup(self) -> None:
        """Do something when starting broker."""
        event = SynTasksEvents.CLIENT_STARTUP
        if self.is_worker_process:
            event = SynTasksEvents.WORKER_STARTUP

        for handler in self.event_handlers[event]:
            await maybe_awaitable(handler(self.state))

        for middleware in self.middlewares:
            if middleware.__class__.startup != SynTasksMiddleware.startup:
                await maybe_awaitable(middleware.startup())

        await self.result_backend.startup()

    async def shutdown(self) -> None:
        """
        Close the broker.

        This method is called,
        when broker is closing.
        """
        event = SynTasksEvents.CLIENT_SHUTDOWN
        if self.is_worker_process:
            event = SynTasksEvents.WORKER_SHUTDOWN

        # Call all shutdown events.
        for handler in self.event_handlers[event]:
            await maybe_awaitable(handler(self.state))

        for middleware in self.middlewares:
            if middleware.__class__.shutdown != SynTasksMiddleware.shutdown:
                await maybe_awaitable(middleware.shutdown())

        await self.result_backend.shutdown()

    @abstractmethod
    async def kick(
        self,
        message: BrokerMessage,
    ) -> None:
        """
        This method is used to kick tasks out from current program.

        Using this method tasks are sent to
        workers.

        You don't need to send broker message. It's helper for brokers,
        please send only bytes from message.message.

        :param message: name of a task.
        """

    @abstractmethod
    def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
        """
        This function listens to new messages and yields them.

        This it the main point for workers.
        This function is used to get new tasks from the network.

        If your broker support acknowledgement, then you
        should wrap your message in AckableMessage dataclass.

        If your messages was wrapped in AckableMessage dataclass,
        syntasks will call ack when finish processing message.

        :yield: incoming messages.
        :return: nothing.
        """

    @overload
    def task(
        self,
        task_name: Callable[_FuncParams, _ReturnType],
        **labels: Any,
    ) -> AsyncSynTasksDecoratedTask[_FuncParams, _ReturnType]:  # pragma: no cover
        ...

    @overload
    def task(
        self,
        task_name: str | None = None,
        **labels: Any,
    ) -> Callable[
        [Callable[_FuncParams, _ReturnType]],
        AsyncSynTasksDecoratedTask[_FuncParams, _ReturnType],
    ]:  # pragma: no cover
        ...

    def task(  # type: ignore[misc]
        self,
        task_name: str | None = None,
        **labels: Any,
    ) -> Any:
        """
        Decorator that turns function into a task.

        This decorator converts function to
        a `SynTasksDecoratedTask` object.

        This object can be called as a usual function,
        because it uses decorated function in it's __call__
        method.

        !! You have to use it with parentheses in order to
        get autocompletion. Like this:

        >>> @task()
        >>> def my_func():
        >>>     ...

        :param task_name: custom name of a task, defaults to decorated function's name.
        :param labels: some addition labels for task.

        :returns: decorator function or AsyncSynTasksDecoratedTask.
        """

        def make_decorated_task(
            inner_labels: dict[str, str | int],
            inner_task_name: str | None = None,
        ) -> Callable[
            [Callable[_FuncParams, _ReturnType]],
            AsyncSynTasksDecoratedTask[_FuncParams, _ReturnType],
        ]:
            def inner(
                func: Callable[_FuncParams, _ReturnType],
            ) -> AsyncSynTasksDecoratedTask[_FuncParams, _ReturnType]:
                decorated_task = build_decorated_task(
                    self,
                    func,
                    inner_task_name,
                    inner_labels,
                    self.decorator_class,
                )
                self._register_task(decorated_task.task_name, decorated_task)
                return decorated_task

            return inner

        if callable(task_name):
            # This is an edge case,
            # when decorator called without parameters.
            return make_decorated_task(
                inner_labels=labels or {},
            )(task_name)

        return make_decorated_task(
            inner_task_name=task_name,
            inner_labels=labels or {},
        )

    def register_task(
        self,
        func: Callable[_FuncParams, _ReturnType],
        task_name: str | None = None,
        **labels: Any,
    ) -> AsyncSynTasksDecoratedTask[_FuncParams, _ReturnType]:
        """
        API for registering tasks programmatically.

        This function is basically the same as `task` decorator,
        but it doesn't decorate function, it just registers it
        and returns AsyncSynTasksDecoratedTask object, that can
        be called later.

        :param func: function to register.
        :param task_name: custom name of a task, defaults to qualified function's name.
        :param labels: some addition labels for task.

        :returns: registered task.
        """
        return self.task(task_name=task_name, **labels)(func)

    def on_event(
        self,
        *events: SynTasksEvents,
    ) -> Callable[[EventHandler], EventHandler]:
        """
        Adds event handler.

        This function adds function to call when event occurs.

        :param events: events to react to.
        :return: a decorator function.
        """

        def handler(function: EventHandler) -> EventHandler:
            for event in events:
                self.event_handlers[event].append(function)
            return function

        return handler

    def add_event_handler(
        self,
        event: SynTasksEvents,
        handler: EventHandler,
    ) -> None:  # pragma: no cover
        """
        Adds event handler.

        this function is the same as on_event.

        >>> broker.add_event_handler(SynTasksEvents.WORKER_STARTUP, my_startup)

        if similar to:

        >>> @broker.on_event(SynTasksEvents.WORKER_STARTUP)
        >>> async def my_startup(context: Context) -> None:
        >>>    ...

        :param event: Event to react to.
        :param handler: handler to call when event is started.
        """
        self.event_handlers[event].append(handler)

    def with_result_backend(
        self,
        result_backend: "AsyncResultBackend[Any]",
    ) -> "Self":  # pragma: no cover
        """
        Set a result backend and return updated broker.

        :param result_backend: new result backend.
        :return: self
        """
        self.result_backend = result_backend
        return self

    def with_id_generator(
        self,
        task_id_generator: Callable[[], str],
    ) -> "Self":  # pragma: no cover
        """
        Set a new Id generator and return an updated broker.

        :param task_id_generator: new generator function.
        :return: self
        """
        self.id_generator = task_id_generator
        return self

    def with_middlewares(
        self,
        *middlewares: "SynTasksMiddleware",
    ) -> "Self":  # pragma: no cover
        """
        Add middewares to broker.

        This method adds new middlewares to broker
        and returns and updated broker.

        :param middlewares: list of middlewares.
        :return: self
        """
        for middleware in middlewares:
            if not isinstance(middleware, SynTasksMiddleware):
                logger.warning(
                    "Middleware %s is not an instance of SynTasksMiddleware. "
                    "Skipping...",
                    middleware,
                )
                continue
            middleware.set_broker(self)
            self.middlewares.append(middleware)
        return self

    def with_event_handlers(
        self,
        event: SynTasksEvents,
        *handlers: EventHandler,
    ) -> "Self":  # pragma: no cover
        """
        Set new event handlers.

        It takes an event to handle, list of handlers
        and sets it to broker, returning an updated broker.

        :param event: event to handle.
        :param handlers: event handlers.
        :return: self
        """
        self.event_handlers[event].extend(handlers)
        return self

    def with_serializer(
        self,
        serializer: SynTasksSerializer,
    ) -> "Self":  # pragma: no cover
        """
        Set a new serializer and return an updated broker.

        :param serializer: new serializer.
        :return: self
        """
        self.serializer = serializer
        return self

    def with_formatter(self, formatter: "SynTasksFormatter") -> "Self":
        """
        Set new formatter and return an updated broker.

        :param formatter: new formatter.
        :return: self
        """
        self.formatter = formatter
        return self

    def _register_task(
        self,
        task_name: str,
        task: AsyncSynTasksDecoratedTask[Any, Any],
    ) -> None:
        """
        Method is used to register tasks.

        By default we register tasks in local task registry.
        But this behaviour can be changed in subclasses.

        This method may raise TaskBrokerMismatchError if task has already been
        registered to a different broker.

        :param task_name: Name of a task.
        :param task: Decorated task.
        """
        if task.broker != self:
            raise TaskBrokerMismatchError(broker=task.broker)
        self.local_task_registry[task_name] = task

    async def __aenter__(self) -> None:
        """Starts the broker as ctx manager."""
        await self.startup()

    async def __aexit__(self, *args: object, **kwargs: Any) -> None:
        """Shuts down the broker as ctx manager."""
        await self.shutdown()
