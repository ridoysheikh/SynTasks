import os
import sys
from collections.abc import Callable, Coroutine
from copy import copy
from functools import wraps
from types import CoroutineType
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    ParamSpec,
    TypeVar,
    get_type_hints,
    overload,
)
from uuid import uuid4

from syntasks.kicker import AsyncKicker
from syntasks.task import AsyncSynTasksTask

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.abc.broker import AsyncBroker

_T = TypeVar("_T")
_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")


class AsyncSynTasksDecoratedTask(Generic[_FuncParams, _ReturnType]):
    """
    Class for all task functions.

    When function is decorated
    with the `task` decorator, it
    will return an instance of this class.

    This class parametrized with original function's
    arguments types and a return type.

    This class has kiq method which is used
    to kick tasks out of this thread and send them to
    current broker.
    """

    def __init__(
        self,
        broker: "AsyncBroker",
        task_name: str,
        original_func: Callable[_FuncParams, _ReturnType],
        labels: dict[str, Any],
        return_type: type[_ReturnType] | None = None,
    ) -> None:
        self.broker = broker
        self.task_name = task_name
        self.original_func = original_func
        self.labels = labels
        self.return_type = return_type

        # This is a hack to make ProcessPoolExecutor work
        # with decorated functions.
        #
        # The problem is that when we decorate a function
        # it becomes a new class. This class has the same
        # name as the original function.
        #
        # When receiver sends original function to another
        # process, it will have the same name as the decorated
        # class. This will cause an error, because ProcessPoolExecutor
        # uses `__name__` and `__qualname__` attributes to
        # import functions from other processes and then it verifies
        # that the function is the same as the original one.
        #
        # This hack renames the original function and injects
        # it back to the module where it was defined.
        # This way ProcessPoolExecutor will be able to import
        # the function by it's name and verify its correctness.
        new_name = f"{original_func.__name__}__syntasks_original"
        self.original_func.__name__ = new_name
        if hasattr(self.original_func, "__qualname__"):
            original_qualname = self.original_func.__qualname__.rsplit(".")
            original_qualname[-1] = new_name
            new_qualname = ".".join(original_qualname)
            self.original_func.__qualname__ = new_qualname
        setattr(
            sys.modules[original_func.__module__],
            new_name,
            original_func,
        )

    # Docs for this method are omitted in order to help
    # your IDE resolve correct docs for it.
    def __call__(  # noqa: D102
        self,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> _ReturnType:
        return self.original_func(*args, **kwargs)

    @overload
    async def kiq(
        self: "AsyncSynTasksDecoratedTask[_FuncParams, CoroutineType[Any, Any, _T]]",
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncSynTasksTask[_T]: ...

    @overload
    async def kiq(
        self: "AsyncSynTasksDecoratedTask[_FuncParams, Coroutine[Any, Any, _T]]",
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncSynTasksTask[_T]: ...

    @overload
    async def kiq(
        self: "AsyncSynTasksDecoratedTask[_FuncParams, _ReturnType]",
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncSynTasksTask[_ReturnType]: ...

    async def kiq(
        self,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> Any:
        """
        This method sends function call over the network.

        It gets current broker and calls it's kick method,
        returning what it returns.

        :param args: function's arguments.
        :param kwargs: function's key word arguments.

        :returns: syntasks task.
        """
        return await self.kicker().kiq(*args, **kwargs)

    async def schedule(self, **options: Any) -> Any:
        """
        Create a dynamic schedule for this task (SynTasks apps only).

        Same arguments as :meth:`syntasks.scheduler.ScheduleManager.create`,
        e.g. ``await check.schedule(every="1s", ttl="2h", kwargs={...})``.

        :param options: schedule options.
        :raises TypeError: if the task's broker is not a SynTasks app.
        :return: the created schedule.
        """
        schedules = getattr(self.broker, "schedules", None)
        if schedules is None:
            raise TypeError("Schedules need a SynTasks app as the broker.")
        return await schedules.create(self.task_name, **options)

    def kicker(self) -> AsyncKicker[_FuncParams, _ReturnType]:
        """
        This function returns kicker object.

        Kicker is an object that can modify kiq request
        before sending it.

        :return: AsyncKicker instance.
        """
        return AsyncKicker(
            task_name=self.task_name,
            broker=self.broker,
            labels=copy(self.labels),
            return_type=self.return_type,
        )

    def __repr__(self) -> str:
        return f"AsyncSynTasksDecoratedTask({self.task_name})"


def default_task_name(func: Callable[..., Any]) -> str:
    """
    The name a task gets when none is given: ``module:function``.

    :param func: task function.
    :return: name.
    """
    fmodule = func.__module__
    if fmodule == "__main__":  # pragma: no cover
        fmodule = ".".join(
            os.path.normpath(sys.argv[0]).removesuffix(".py").split(os.path.sep),
        )
    fname = func.__name__
    if fname == "<lambda>":
        fname = f"lambda_{uuid4().hex}"
    return f"{fmodule}:{fname}"


def build_decorated_task(
    broker: Any,
    func: Callable[..., Any],
    task_name: str | None,
    labels: dict[str, Any],
    decorator_class: type[
        AsyncSynTasksDecoratedTask[Any, Any]
    ] = AsyncSynTasksDecoratedTask,
) -> AsyncSynTasksDecoratedTask[Any, Any]:
    """
    Wrap a function in a decorated task (without registering it).

    :param broker: broker the task sends through (or a stand-in).
    :param func: task function.
    :param task_name: name, or None for :func:`default_task_name`.
    :param labels: labels of every message.
    :param decorator_class: class of the result.
    :return: decorated task.
    """
    name = task_name or default_task_name(func)
    return_type = get_type_hints(func).get("return")
    return wraps(func)(  # type: ignore[return-value]
        decorator_class(
            broker=broker,
            original_func=func,
            labels=labels,
            task_name=name,
            return_type=return_type,
        ),
    )
