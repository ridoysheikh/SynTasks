from collections.abc import Coroutine
from dataclasses import asdict, is_dataclass
from datetime import timedelta
from logging import getLogger
from types import CoroutineType
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    ParamSpec,
    TypeVar,
    overload,
)

from syntasks.abc.middleware import SynTasksMiddleware
from syntasks.exceptions import DuplicateTaskError, SendTaskError
from syntasks.labels import prepare_label
from syntasks.message import SynTasksMessage
from syntasks.task import AsyncSynTasksTask
from syntasks.utils import maybe_awaitable

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.abc.broker import AsyncBroker

_T = TypeVar("_T")
_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")

logger = getLogger("syntasks")


class AsyncKicker(Generic[_FuncParams, _ReturnType]):
    """Class that used to modify data before sending it to broker."""

    def __init__(
        self,
        task_name: str,
        broker: "AsyncBroker",
        labels: dict[str, Any],
        return_type: type[_ReturnType] | None = None,
    ) -> None:
        self.task_name = task_name
        self.broker = broker
        self.labels = labels
        self.custom_task_id: str | None = None
        self.return_type = return_type

    def with_labels(
        self,
        **labels: str | float,
    ) -> "AsyncKicker[_FuncParams, _ReturnType]":
        """
        Update function's labels before sending.

        :param labels: new labels.
        :return: kicker with new labels.
        """
        self.labels.update(labels)
        return self

    def with_queue(self, queue: str) -> "AsyncKicker[_FuncParams, _ReturnType]":
        """
        Send this call to another queue.

        :param queue: queue name.
        :return: kicker.
        """
        self.labels["queue"] = queue
        return self

    def with_priority(self, priority: int) -> "AsyncKicker[_FuncParams, _ReturnType]":
        """
        Set the priority of this call (0 = highest).

        :param priority: priority level.
        :return: kicker.
        """
        self.labels["priority"] = priority
        return self

    def with_delay(
        self,
        delay: float | timedelta,
    ) -> "AsyncKicker[_FuncParams, _ReturnType]":
        """
        Run this call after a delay.

        :param delay: seconds or a timedelta.
        :return: kicker.
        """
        seconds = (
            delay.total_seconds() if isinstance(delay, timedelta) else float(delay)
        )
        self.labels["delay"] = seconds
        return self

    def with_timeout(
        self,
        timeout: float | timedelta,
    ) -> "AsyncKicker[_FuncParams, _ReturnType]":
        """
        Override the time limit of this call.

        :param timeout: seconds or a timedelta.
        :return: kicker.
        """
        seconds = (
            timeout.total_seconds()
            if isinstance(timeout, timedelta)
            else float(timeout)
        )
        self.labels["timeout"] = seconds
        return self

    def then(self, task: Any) -> "AsyncKicker[_FuncParams, _ReturnType]":
        """
        Send ``task`` with a :class:`~syntasks.TaskOutcome` if this call succeeds.

        :param task: decorated task or task name.
        :return: kicker.
        """
        self.labels["_then"] = task if isinstance(task, str) else task.task_name
        return self

    def catch(self, task: Any) -> "AsyncKicker[_FuncParams, _ReturnType]":
        """
        Send ``task`` with a :class:`~syntasks.TaskOutcome` if this call fails.

        It is sent after the last retry.

        :param task: decorated task or task name.
        :return: kicker.
        """
        self.labels["_catch"] = task if isinstance(task, str) else task.task_name
        return self

    async def call(self, *args: Any, **kwargs: Any) -> Any:
        """
        Send this call and wait for its value (request / reply).

        Sent at the highest priority with a short-lived result. Raises the
        task's own exception if it failed, or :class:`TaskTimeoutError` if
        it ran out of time (``with_timeout(...)`` or the task's timeout).

        :param args: arguments.
        :param kwargs: keyword arguments.
        :return: the task's return value.
        """
        from syntasks.exceptions import (  # noqa: PLC0415
            SynTasksResultTimeoutError,
            TaskTimeoutError,
        )

        self.labels.setdefault("priority", 0)
        self.labels.setdefault("_result_ttl", 60)
        limit: float | None = None
        options_for = getattr(self.broker, "options_for", None)
        if "timeout" in self.labels:
            limit = float(self.labels["timeout"])
        elif options_for is not None:
            limit = options_for(self.task_name).timeout
        wait = -1.0
        if limit is not None:
            grace = options_for(self.task_name).grace if options_for else 30.0
            wait = limit + grace + 5.0
        handle = await self.kiq(*args, **kwargs)
        try:
            result = await handle.wait_result(timeout=wait)
        except SynTasksResultTimeoutError:
            raise TaskTimeoutError(
                task_name=self.task_name, timeout=limit or 0.0
            ) from None
        result.raise_for_error()
        return result.return_value

    def with_task_id(
        self,
        task_id: str | None,
    ) -> "AsyncKicker[_FuncParams, _ReturnType]":
        """
        Set task_id for current execution.

        Please use this method with caution,
        because it may brake the logic of getting results.

        :param task_id: custom task id.
        :return: kicker with custom task id.
        """
        self.custom_task_id = task_id
        return self

    def with_broker(
        self,
        broker: "AsyncBroker",
    ) -> "AsyncKicker[_FuncParams, _ReturnType]":
        """
        Replace broker for the function.

        This method can be used with
        shared tasks.

        :param broker: new broker instance.
        :return: Kicker with new broker.
        """
        self.broker = broker
        return self

    @overload
    async def kiq(
        self: "AsyncKicker[_FuncParams, CoroutineType[Any, Any, _T]]",
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncSynTasksTask[_T]:  # pragma: no cover
        ...

    @overload
    async def kiq(
        self: "AsyncKicker[_FuncParams, Coroutine[Any, Any, _T]]",
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncSynTasksTask[_T]:  # pragma: no cover
        ...

    @overload
    async def kiq(
        self: "AsyncKicker[_FuncParams, _ReturnType]",
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncSynTasksTask[_ReturnType]:  # pragma: no cover
        ...

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

        :raises SendTaskError: if we can't send task to the broker.

        :returns: syntasks task.
        """
        logger.debug(
            "Kicking %s with args=%s and kwargs=%s.",
            self.task_name,
            args,
            kwargs,
        )
        message = self._prepare_message(*args, **kwargs)
        for middleware in self.broker.middlewares:
            if middleware.__class__.pre_send != SynTasksMiddleware.pre_send:
                message = await maybe_awaitable(middleware.pre_send(message))
        try:
            await self.broker.kick(self.broker.formatter.dumps(message))
        except DuplicateTaskError as duplicate:
            # unique_for: hand back the task that is already queued.
            return AsyncSynTasksTask(
                task_id=duplicate.task_id,
                result_backend=self.broker.result_backend,
                return_type=self.return_type,  # type: ignore
                task_name=self.task_name,
            )
        except SendTaskError:
            raise
        except Exception as exc:
            raise SendTaskError from exc

        for middleware in reversed(self.broker.middlewares):
            if middleware.__class__.post_send != SynTasksMiddleware.post_send:
                await maybe_awaitable(middleware.post_send(message))

        return AsyncSynTasksTask(
            task_id=message.task_id,
            result_backend=self.broker.result_backend,
            return_type=self.return_type,  # type: ignore # (pyright issue)
            task_name=self.task_name,
        )

    @classmethod
    def _prepare_arg(cls, arg: Any) -> Any:
        """
        Parses argument if possible.

        This function is used to construct dicts
        from pydantic models or dataclasses.

        :param arg: argument to format.
        :return: Formatted argument.
        """
        if not isinstance(arg, type) and callable(getattr(arg, "model_dump", None)):
            # pydantic-compatible models.
            arg = arg.model_dump(mode="json")
        if is_dataclass(arg):
            if isinstance(arg, type):
                raise ValueError(
                    f"Cannot serialize types. The {arg} is not serializable.",
                )
            arg = asdict(arg)
        return arg

    def _prepare_message(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> SynTasksMessage:
        """
        Create a message from args and kwargs.

        :param args: function's args.
        :param kwargs: function's kwargs.
        :return: constructed message.
        """
        formatted_args = []
        formatted_kwargs = {}
        labels = {}
        labels_types = {}
        for arg in args:
            formatted_args.append(self._prepare_arg(arg))
        for kwarg_name, kwarg_val in kwargs.items():
            formatted_kwargs[kwarg_name] = self._prepare_arg(kwarg_val)

        task_only_labels = {
            label
            for middleware in self.broker.middlewares
            for label in middleware.task_only_labels()
        }
        for label, label_val in self.labels.items():
            if label in task_only_labels:
                continue
            labels[label], labels_types[label] = prepare_label(label_val)

        task_id = self.custom_task_id
        if task_id is None:
            task_id = self.broker.id_generator()

        return SynTasksMessage(
            task_id=task_id,
            task_name=self.task_name,
            labels=labels,
            labels_types=labels_types,
            args=formatted_args,
            kwargs=formatted_kwargs,
        )
