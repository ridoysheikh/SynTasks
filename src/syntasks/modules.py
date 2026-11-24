"""
Modules: groups of tasks, schedules, channels and queues with shared options.

A big app is split into modules, like routers in a web framework::

    # billing/payments.py
    module = Module("billing.payments", timeout="30s", retries=3, max_tasks=50)

    @module.task(timeout="2m")
    async def charge(customer_id: str, cents: int) -> None: ...

    # app.py
    app = SynTasks(timeout="5m", retries=0, max_tasks=None)
    app.extends(payments.module)

Options set on a module are defaults for everything declared in it and in
the modules it extends; the nearest link wins. :class:`~syntasks.SynTasks`
is the root module.
"""

import importlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from syntasks.config.loader import ConfigError, build
from syntasks.config.schema import QueueConfig
from syntasks.decor import AsyncSynTasksDecoratedTask, build_decorated_task
from syntasks.exceptions import SynTasksError
from syntasks.options import OptionLayer, parse_layer
from syntasks.scheduler.recurrence import Recurrence, ScheduleDecl

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks

__all__ = ["Declarations", "Module", "ModuleNotAttachedError"]


class ModuleNotAttachedError(SynTasksError):
    """A module's task was used before the module was attached to an app."""

    __template__ = (
        "{module} isn't attached to an app yet; pass it (or a module that "
        "extends it) to app.extends(...) first."
    )
    module: str


class _Detached:
    """Stands in for the broker of a task whose module isn't attached yet."""

    def __init__(self, module: "Module") -> None:
        self._module = module

    def __getattr__(self, name: str) -> Any:
        raise ModuleNotAttachedError(module=self._module.owner)

    def __repr__(self) -> str:
        return f"<not attached: {self._module.owner}>"


def split_options(
    options: dict[str, Any],
    allowed: frozenset[str] = frozenset(),
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Separate task options from other keyword arguments.

    :param options: keyword arguments of a decorator.
    :param allowed: other names the decorator accepts.
    :raises ConfigError: on a name that is neither.
    :return: ``(task options, other arguments)``.
    """
    task_options: dict[str, Any] = {}
    other: dict[str, Any] = {}
    for name, value in options.items():
        if name in allowed:
            other[name] = value
        else:
            task_options[name] = value
    return task_options, other


# Keyword arguments of @schedule that are schedule policies, not task options.
SCHEDULE_OPTIONS = frozenset(
    {
        "start_at",
        "end_at",
        "jitter",
        "misfire",
        "misfire_grace",
        "max_catchup",
        "overlap",
        "max_instances",
        "max_runs",
        "pause_after_failures",
        "paused",
    },
)


def _schedule_decls(
    owner: str,
    timing: Any,
    schedule_id: str | None,
    args: Any,
    kwargs: Any,
    policies: dict[str, Any],
) -> list[ScheduleDecl]:
    items = list(timing) if isinstance(timing, (list, tuple)) else [timing]
    items = [item for item in items if item is not None]
    if not items:
        raise ConfigError(
            detail=f"{owner}: @schedule needs a timing: every(...), cron(...), "
            "calendar(...) or at(...).",
        )
    if schedule_id is not None and len(items) > 1:
        raise ConfigError(
            detail=f"{owner}: with several timings, give each its own id "
            "(every(..., id=...)).",
        )
    decls = []
    for item in items:
        decl = ScheduleDecl(recurrence=item) if isinstance(item, Recurrence) else item
        if not isinstance(decl, ScheduleDecl):
            raise ConfigError(
                detail=f"{owner}: a timing must come from every(), cron(), "
                f"calendar() or at(), got {item!r}.",
            )
        if decl.after_ms is not None:
            raise ConfigError(
                detail=f"{owner}: after() is for schedules created at runtime.",
            )
        decls.append(
            ScheduleDecl(
                recurrence=decl.recurrence,
                id=decl.id or schedule_id,
                args=decl.args or list(args or ()),
                kwargs=decl.kwargs or dict(kwargs or {}),
                options={**policies, **decl.options},
            ),
        )
    return decls


def _resolve_path(path: str) -> "Module":
    module_name, _, attribute = path.partition(":")
    if not attribute:
        raise ConfigError(detail=f"{path!r}: use 'package.module:attribute'.")
    value = getattr(importlib.import_module(module_name), attribute)
    if not isinstance(value, Module):
        raise ConfigError(detail=f"{path!r} is {type(value).__name__}, not a Module.")
    return value


class Declarations:
    """What :class:`Module` and :class:`~syntasks.SynTasks` have in common."""

    owner: str
    layer: OptionLayer

    def _init_declarations(self, owner: str, options: dict[str, Any]) -> None:
        self.owner = owner
        self.layer = parse_layer(owner, options)

    # Hooks: a module records declarations until it is attached; the app
    # registers them right away.

    def _declare_task(
        self, task: AsyncSynTasksDecoratedTask[Any, Any], layer: OptionLayer
    ) -> None:
        raise NotImplementedError

    def _declare_queue(self, name: str, options: QueueConfig) -> None:
        raise NotImplementedError

    def _declare_channel(
        self, pattern: str, func: Callable[..., Any], options: dict[str, Any]
    ) -> None:
        raise NotImplementedError

    def _broker_for_tasks(self) -> Any:
        raise NotImplementedError

    def _extend(self, module: "Module") -> None:
        raise NotImplementedError

    # -- public API ----------------------------------------------------------

    def task(
        self,
        task_name: str | Callable[..., Any] | None = None,
        *,
        labels: dict[str, Any] | None = None,
        **options: Any,
    ) -> Any:
        """
        Turn a function into a task.

        :param task_name: custom name (default ``module:function``).
        :param labels: labels sent with every message of this task.
        :param options: task options (see :class:`~syntasks.options.TaskOptions`).
        :return: decorated task, or a decorator.
        """
        hint = task_name if isinstance(task_name, str) else None

        def wrap(func: Callable[..., Any]) -> AsyncSynTasksDecoratedTask[Any, Any]:
            return self._make_task(func, hint, labels, options, [])

        if callable(task_name):
            return wrap(task_name)
        return wrap

    def schedule(
        self,
        timing: Any = None,
        /,
        *,
        task_name: str | None = None,
        labels: dict[str, Any] | None = None,
        id: str | None = None,
        args: Any = None,
        kwargs: Any = None,
        **options: Any,
    ) -> Callable[[Callable[..., Any]], AsyncSynTasksDecoratedTask[Any, Any]]:
        """
        Turn a function into a task that runs on a schedule.

        The schedule is stored in Redis and reconciled on every worker start;
        each fire runs once across the cluster. The function is still a task
        that can be sent by hand.

        :param timing: :func:`~syntasks.every`, :func:`~syntasks.cron`,
            :func:`~syntasks.calendar` or :func:`~syntasks.at`, or a list.
        :param task_name: custom task name (default ``module:function``).
        :param labels: labels sent with every message.
        :param id: schedule id (one timing only; default: the task name).
        :param args: task arguments of every fire.
        :param kwargs: task keyword arguments of every fire.
        :param options: schedule policies (``misfire``, ``misfire_grace``,
            ``overlap``, ``max_instances``, ``jitter``, ``start_at``,
            ``end_at``, ``max_runs``, ``pause_after_failures``, ``paused``)
            and task options (``timeout``, ``retries``, ``max_tasks``, ...).
        :raises ConfigError: without a timing, or with invalid values.
        :return: decorator.
        """
        policies = {k: options.pop(k) for k in list(options) if k in SCHEDULE_OPTIONS}
        decls = _schedule_decls(self.owner, timing, id, args, kwargs, policies)

        def wrap(func: Callable[..., Any]) -> AsyncSynTasksDecoratedTask[Any, Any]:
            return self._make_task(func, task_name, labels, options, decls)

        return wrap

    def _make_task(
        self,
        func: Callable[..., Any],
        task_name: str | None,
        labels: dict[str, Any] | None,
        options: dict[str, Any],
        schedules: list[ScheduleDecl],
    ) -> AsyncSynTasksDecoratedTask[Any, Any]:
        decorated = build_decorated_task(
            self._broker_for_tasks(),
            func,
            task_name,
            dict(labels or {}),
        )
        layer = parse_layer(f"task {decorated.task_name}", options)
        decorated.declared_schedules = schedules
        self._declare_task(decorated, layer)
        return decorated

    def queue(self, name: str, **options: Any) -> None:
        """
        Declare a queue and its options.

        :param name: queue name.
        :param options: ``max_length``, ``on_full``, ``max_deliveries``,
            ``priorities``, ``dead_letter_max``.
        :raises ConfigError: on invalid options.
        """
        self._declare_queue(name, build(QueueConfig, options, f"queue {name}"))

    def channel(
        self, pattern: str, **options: Any
    ) -> Callable[[Callable[..., Any]], Any]:
        """
        Register a long-running channel handler (see :mod:`syntasks.channels`).

        :param pattern: channel name pattern, e.g. ``"room:{room_id}"``.
        :param options: handler options.
        :return: decorator.
        """

        def wrap(func: Callable[..., Any]) -> Any:
            self._declare_channel(pattern, func, options)
            return func

        return wrap

    def extends(self, *modules: "Module | str") -> None:
        """
        Attach modules: their declarations join this one, under its options.

        :param modules: :class:`Module` objects or ``"package.module:attr"`` paths.
        """
        for item in modules:
            module = _resolve_path(item) if isinstance(item, str) else item
            if module is self:
                raise ConfigError(detail=f"{self.owner} can't extend itself.")
            self._extend(module)


class Module(Declarations):
    """
    A group of declarations with shared options.

    :param name: name shown in errors and CLI filters, e.g. ``"billing"``.
    :param options: task options every member starts from.
    """

    def __init__(self, name: str, **options: Any) -> None:
        self.name = name
        self._init_declarations(f'Module("{name}")', options)
        self.app: SynTasks | None = None
        self._chain: list[OptionLayer] = []
        self._tasks: list[tuple[AsyncSynTasksDecoratedTask[Any, Any], OptionLayer]] = []
        self._queues: list[tuple[str, QueueConfig]] = []
        self._channels: list[tuple[str, Callable[..., Any], dict[str, Any]]] = []
        self._children: list[Module] = []
        self._detached = _Detached(self)

    def __repr__(self) -> str:
        state = "attached" if self.app is not None else "not attached"
        return f"<{self.owner} {len(self._tasks)} tasks, {state}>"

    @property
    def chain(self) -> list[OptionLayer]:
        """Layers from the app down to this module (empty until attached)."""
        return self._chain

    def _broker_for_tasks(self) -> Any:
        return self.app if self.app is not None else self._detached

    def _declare_task(
        self, task: AsyncSynTasksDecoratedTask[Any, Any], layer: OptionLayer
    ) -> None:
        self._tasks.append((task, layer))
        if self.app is not None:
            self.app.register_declared(task, [*self._chain, layer])

    def _declare_queue(self, name: str, options: QueueConfig) -> None:
        self._queues.append((name, options))
        if self.app is not None:
            self.app.declare_queue(name, options)

    def _declare_channel(
        self, pattern: str, func: Callable[..., Any], options: dict[str, Any]
    ) -> None:
        self._channels.append((pattern, func, options))
        if self.app is not None:
            self.app.register_channel(pattern, func, options, self._chain)

    def _extend(self, module: "Module") -> None:
        if module not in self._children:
            self._children.append(module)
        if self.app is not None:
            module.attach_to(self.app, self._chain)

    def attach_to(self, app: "SynTasks", parent_chain: list[OptionLayer]) -> None:
        """
        Bind this module, its declarations and its children to an app.

        :param app: the app.
        :param parent_chain: option layers above this module.
        :raises ConfigError: if it is attached to another app already.
        """
        if self.app is app:
            return
        if self.app is not None:
            raise ConfigError(
                detail=f"{self.owner} is already attached to another app.",
            )
        self.app = app
        self._chain = [*parent_chain, self.layer]
        for name, queue_options in self._queues:
            app.declare_queue(name, queue_options)
        for task, layer in self._tasks:
            task.broker = app
            app.register_declared(task, [*self._chain, layer])
        for pattern, func, options in self._channels:
            app.register_channel(pattern, func, options, self._chain)
        for child in self._children:
            child.attach_to(app, self._chain)
