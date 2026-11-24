"""
The worker's message handler for SynTasks apps.

On top of the generic :class:`~syntasks.receiver.Receiver` it adds:

* routing fixes (a gated task found in a shared stream is moved to its
  own stream) and unknown-task handling;
* per-key concurrency (parking);
* time limits: soft timeout → hard timeout (cancel) → grace → abandon
  and recycle the process, or kill the child for isolated runs;
* process isolation with memory limits;
* native retries with backoff, dead letters, per-task result TTL;
* cancellation on request, log capture, schedule info injection.
"""

import asyncio
import contextlib
import contextvars
import functools
import inspect
import logging
import time
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from syntasks.acks import AckableMessage, AckController, AcknowledgeType
from syntasks.context import Context
from syntasks.exceptions import (
    NoResultError,
    TaskCancelledError,
    TaskRejectedError,
    TaskTimeoutError,
)
from syntasks.message import BrokerMessage, SynTasksMessage
from syntasks.options import TaskOptions, extract_argument, key_text
from syntasks.queues.engine import Delivery
from syntasks.receiver.params_parser import parse_params
from syntasks.receiver.receiver import Receiver
from syntasks.result import SynTasksResult
from syntasks.scheduler.info import ScheduleInfo
from syntasks.state import SynTasksState
from syntasks.utils import maybe_awaitable
from syntasks.worker.deadline import Deadline
from syntasks.worker.isolation import run_isolated

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks
    from syntasks.worker.runtime import WorkerRuntime

__all__ = ["AppReceiver", "RunningTask"]

logger = logging.getLogger("syntasks.worker")

_MAX_UNKNOWN_REQUEUES = 5
_NO_RETRY = (NoResultError, TaskRejectedError, TaskCancelledError)


@dataclass
class RunningTask:
    """A task run in progress in this process."""

    task_id: str
    task_name: str
    started: float
    runner: asyncio.Task[Any] | None = None
    cancel_requested: bool = False


class AppReceiver(Receiver):
    """Receiver used by SynTasks workers."""

    def __init__(
        self,
        app: "SynTasks",
        runtime: "WorkerRuntime | None" = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(broker=app, **kwargs)
        self.app = app
        self.runtime = runtime
        self.running: dict[str, RunningTask] = {}

    # -- control ---------------------------------------------------------------

    def cancel(self, task_id: str) -> bool:
        """
        Cancel a running task of this process.

        :param task_id: task id.
        :return: True if the task was running here.
        """
        running = self.running.get(task_id)
        if running is None:
            return False
        running.cancel_requested = True
        if running.runner is not None and not running.runner.done():
            running.runner.cancel()
        logger.info("Cancelling task %s (%s) on request.", task_id, running.task_name)
        return True

    def _taint(self, reason: str) -> None:
        if self.runtime is not None:
            self.runtime.taint(reason)
        else:
            logger.error("%s", reason)

    # -- message handling ---------------------------------------------------------

    async def callback(
        self,
        message: bytes | AckableMessage,
        raise_err: bool = False,
    ) -> None:
        """
        Process one message and release whatever it held.

        :param message: message from the queue.
        :param raise_err: re-raise result backend errors.
        """
        delivery = message if isinstance(message, Delivery) else None
        try:
            await self._process(message, delivery, raise_err)
        finally:
            if delivery is not None:
                await self.app.engine.finish(delivery)

    async def _dead_letter(
        self,
        delivery: Delivery | None,
        ack: AckController,
        *,
        queue: str,
        data: bytes,
        task_name: str,
        reason: str,
        error: str,
    ) -> None:
        if delivery is None:
            if ack.is_ackable:
                await ack.ack()
            return
        await self.app.engine.dead_letter(
            delivery,
            queue=queue,
            data=data,
            task_name=task_name,
            reason=reason,
            error=error,
        )
        ack.is_acked = True

    async def _process(  # noqa: C901, PLR0911, PLR0912, PLR0915
        self,
        message: bytes | AckableMessage,
        delivery: Delivery | None,
        raise_err: bool,
    ) -> None:
        ack = AckController(
            message.ack if isinstance(message, AckableMessage) else None,
        )
        data = message.data if isinstance(message, AckableMessage) else message
        queue = delivery.queue if delivery else "default"
        try:
            msg = self.app.formatter.loads(message=data)
            msg.parse_labels()
        except Exception as exc:
            logger.warning("Cannot parse message; dead-lettering it: %s", exc)
            await self._dead_letter(
                delivery,
                ack,
                queue=queue,
                data=data,
                task_name=delivery.task_name if delivery else "",
                reason="invalid_message",
                error=repr(exc),
            )
            return

        task = self.app.find_task(msg.task_name)
        if task is None:
            await self._unknown_task(delivery, ack, msg, data, queue)
            return
        options = self.app.options_for(msg.task_name)

        if (
            delivery is not None
            and not delivery.gated
            and self.app.engine.dedicated(options)
        ):
            # Sent by a process that didn't know the task is gated/tagged.
            labels = {k: v for k, v in msg.labels.items() if k != "delay"}
            await self.app.engine.kick(
                BrokerMessage(
                    task_id=msg.task_id,
                    task_name=msg.task_name,
                    message=data,
                    labels=labels,
                ),
            )
            await ack.ack()
            return

        if (
            delivery is not None
            and msg.labels.get("_batch")
            and not await self.app.completion.batch_gate(delivery, msg)
        ):
            return

        if delivery is not None and options.rate_per_key is not None and options.key:
            call = {"args": msg.args, "kwargs": msg.kwargs}
            rate_key = self.app.engine.key_of(msg.task_name, options, call)
            if rate_key is not None and not await self.app.engine.rate_key(
                delivery,
                options,
                rate_key,
            ):
                logger.debug("Task %s deferred by its key's rate.", msg.task_id)
                return

        if delivery is not None and options.key and options.max_tasks_per_key:
            try:
                key = key_text(
                    extract_argument(
                        options.key,
                        task.original_func,
                        msg.args,
                        msg.kwargs,
                    ),
                )
            except (KeyError, IndexError, ValueError, TypeError):
                logger.warning(
                    "Task %s: key %r not found in the call; running "
                    "without a key slot.",
                    msg.task_name,
                    options.key,
                )
            else:
                if not await self.app.engine.acquire_key(delivery, options, key):
                    logger.debug("Task %s parked behind key %s.", msg.task_id, key)
                    return

        overlap = msg.labels.get("_overlap")
        if delivery is not None and overlap in ("skip", "queue"):
            schedule_id = str(msg.labels.get("_schedule"))
            if not await self.app.engine.acquire_slot(
                delivery,
                f"sched:{schedule_id}",
                self.app.keys("q", "park", "sched", schedule_id),
                int(msg.labels.get("_max_instances", 1) or 1),
                "drop" if overlap == "skip" else "park",
            ):
                if overlap == "skip":
                    logger.info(
                        "Schedule %s: run %s skipped, the previous run is still going.",
                        schedule_id,
                        msg.task_id,
                    )
                return

        for middleware in self.app.middlewares:
            msg = await maybe_awaitable(middleware.pre_execute(msg))

        ack_time = self._get_ack_time(msg)
        logger.info("Executing task %s with ID: %s", msg.task_name, msg.task_id)
        if ack_time == AcknowledgeType.WHEN_RECEIVED and ack.is_ackable:
            await ack.ack()

        result = await self.run_task(
            target=task.original_func,
            message=msg,
            ack_controller=ack,
        )

        if delivery is not None:
            # The run is over: let the next one start while we store the result.
            await self.app.engine.release_slots(delivery)

        retried = False
        if result.is_err and not isinstance(result.error, _NO_RETRY):
            retried = await self._maybe_retry(msg, options)

        if ack_time == AcknowledgeType.WHEN_EXECUTED and ack.is_ackable:
            await ack.ack()

        for middleware in reversed(self.app.middlewares):
            await maybe_awaitable(middleware.post_execute(msg, result))

        if not retried:
            try:
                if options.store_result and not isinstance(result.error, NoResultError):
                    ttl = options.result_ttl
                    if msg.labels.get("_result_ttl") not in (None, ""):
                        ttl = float(msg.labels["_result_ttl"])
                    await self.app.result_backend.store(
                        msg.task_id,
                        result,
                        ttl=ttl,
                        max_size=options.max_result,
                    )
                    for middleware in reversed(self.app.middlewares):
                        await maybe_awaitable(middleware.post_save(msg, result))
            except Exception:
                logger.exception("Can't store the result of %s.", msg.task_id)
                if raise_err:
                    raise
            if (
                result.is_err
                and options.dead_letter
                and delivery is not None
                and not ack.is_acked
                and not isinstance(result.error, (*_NO_RETRY, TaskCancelledError))
            ):
                await self._dead_letter(
                    delivery,
                    ack,
                    queue=queue,
                    data=data,
                    task_name=msg.task_name,
                    reason="failed",
                    error=(
                        "".join(traceback.format_exception(result.error))
                        if result.error
                        else ""
                    ),
                )
            if self.app.schedules is not None and msg.labels.get("_schedule"):
                await self.app.schedules.record_run(msg, result)
            try:
                await self.app.completion.after_run(msg, result, options)
            except Exception:
                logger.exception("Can't send the callbacks of %s.", msg.task_id)

        if (ack_time == AcknowledgeType.WHEN_SAVED and ack.is_ackable) or (
            ack_time == AcknowledgeType.MANUAL and retried and ack.is_ackable
        ):
            await ack.ack()

    async def _unknown_task(
        self,
        delivery: Delivery | None,
        ack: AckController,
        msg: SynTasksMessage,
        data: bytes,
        queue: str,
    ) -> None:
        tries = int(msg.labels.get("_unknown", 0))
        if delivery is not None and tries < _MAX_UNKNOWN_REQUEUES:
            # Another worker may have it imported.
            msg.labels["_unknown"] = tries + 1
            msg.labels["delay"] = 1.0 * (tries + 1)
            await self.app.kick(self.app.formatter.dumps(msg))
            await ack.ack()
            logger.warning(
                'Task "%s" is not registered in this worker; requeued it (%d/%d). '
                "Did you forget to import it?",
                msg.task_name,
                tries + 1,
                _MAX_UNKNOWN_REQUEUES,
            )
            return
        logger.warning(
            'Task "%s" is not registered anywhere; dead-lettering.',
            msg.task_name,
        )
        await self._dead_letter(
            delivery,
            ack,
            queue=queue,
            data=data,
            task_name=msg.task_name,
            reason="unknown_task",
            error=f"No worker has task {msg.task_name!r} registered.",
        )

    async def _maybe_retry(self, msg: SynTasksMessage, options: TaskOptions) -> bool:
        attempt = int(msg.labels.get("_attempt", 0))
        retries = options.retries
        if "_schedule_retries" in msg.labels:
            retries = int(msg.labels["_schedule_retries"])
        if attempt >= retries:
            return False
        delay = options.retry_delay_for(attempt + 1)
        msg.labels["_attempt"] = attempt + 1
        msg.labels["_retry"] = 1
        msg.labels["delay"] = round(delay, 3)
        await self.app.kick(self.app.formatter.dumps(msg))
        logger.info(
            "Task %s (%s) failed; retry %d/%d in %.2fs.",
            msg.task_id,
            msg.task_name,
            attempt + 1,
            retries,
            delay,
        )
        return True

    # -- execution -----------------------------------------------------------------

    def _limits(
        self,
        message: SynTasksMessage,
        options: TaskOptions,
    ) -> tuple[float | None, float | None, float]:
        timeout = options.timeout
        if message.labels.get("timeout") not in (None, ""):
            timeout = float(message.labels["timeout"])
        if message.labels.get("_timeout") not in (None, ""):
            timeout = float(message.labels["_timeout"])
        soft = options.soft_timeout
        if soft is not None and timeout is not None and soft >= timeout:
            soft = None
        return timeout, soft, options.grace

    async def run_task(  # noqa: C901, PLR0912, PLR0915
        self,
        target: Callable[..., Any],
        message: SynTasksMessage,
        ack_controller: AckController | None = None,
    ) -> SynTasksResult[Any]:
        """
        Execute a task call with its limits.

        :param target: task function.
        :param message: message.
        :param ack_controller: ack controller of the message.
        :return: result.
        """
        options = self.app.options_for(message.task_name)
        timeout, soft, grace = self._limits(message, options)
        if message.task_name not in self.known_tasks:
            self._prepare_task(message.task_name, target)
        signature = (
            self.task_signatures.get(message.task_name)
            if self.validate_params
            else None
        )
        graph = self.dependency_graphs.get(message.task_name)
        returned: Any = None
        error: BaseException | None = None
        deadline = Deadline(soft_timeout=soft, timeout=timeout)
        running = RunningTask(message.task_id, message.task_name, time.monotonic())
        self.running[message.task_id] = running
        dep_ctx = None
        start = time.monotonic()
        loop = asyncio.get_running_loop()
        soft_handle: asyncio.TimerHandle | None = None
        capture = self.app.logs.capture(message, options)
        try:
            await capture.__aenter__()
            parse_params(
                signature,
                self.task_hints.get(message.task_name) or {},
                message,
            )
            kwargs: dict[str, Any] = {}
            if graph:
                context = {
                    **self.app.custom_dependency_context,
                    Context: Context(message, self.app, ack_controller),
                    SynTasksState: self.app.state,
                    Deadline: deadline,
                    ScheduleInfo: ScheduleInfo.from_labels(message.labels),
                }
                dep_ctx = graph.async_ctx(
                    context,
                    self.app.dependency_overrides or None,
                )
                kwargs = await dep_ctx.resolve_kwargs()
            kwargs.update(message.kwargs)
            if soft is not None:

                def soft_reached() -> None:
                    deadline.soft_reached.set()
                    logger.warning(
                        "Task %s (%s) passed its soft timeout of %ss.",
                        message.task_id,
                        message.task_name,
                        soft,
                    )

                soft_handle = loop.call_later(soft, soft_reached)
            if options.isolation == "process":
                runner = asyncio.create_task(
                    run_isolated(
                        target,
                        tuple(message.args),
                        kwargs,
                        task_name=message.task_name,
                        timeout=timeout,
                        grace=grace,
                        memory_limit=options.memory_limit,
                    ),
                )
                running.runner = runner
                returned = await self._await_runner(
                    runner,
                    running,
                    None,
                    grace,
                    message,
                )
            elif inspect.iscoroutinefunction(target):
                runner = asyncio.create_task(
                    target(*message.args, **kwargs),
                    context=contextvars.copy_context(),
                )
                running.runner = runner
                returned = await self._await_runner(
                    runner,
                    running,
                    timeout,
                    grace,
                    message,
                )
            else:
                ctx = contextvars.copy_context()
                future = loop.run_in_executor(
                    self.executor,
                    ctx.run,
                    functools.partial(target, *message.args, **kwargs),
                )
                returned = await self._await_thread(
                    future,
                    running,
                    timeout,
                    grace,
                    message,
                )
            if inspect.isawaitable(returned):
                returned = await returned
        except NoResultError as exc:
            error = exc
            logger.warning(
                "Task %s with id %s skipped setting result.",
                message.task_name,
                message.task_id,
            )
        except asyncio.CancelledError:
            if running.cancel_requested:
                error = TaskCancelledError(task_id=message.task_id)
            else:
                raise
        except BaseException as exc:
            error = exc
            if isinstance(exc, TaskTimeoutError):
                logger.error("%s", exc)
            else:
                logger.exception("Exception found while executing function.")
        finally:
            if soft_handle is not None:
                soft_handle.cancel()
            self.running.pop(message.task_id, None)
        execution_time = time.monotonic() - start
        if dep_ctx is not None:
            exc_info: Any = (None, None, None)
            if error is not None and self.propagate_exceptions:
                exc_info = (type(error), error, error.__traceback__)
            with contextlib.suppress(Exception):
                await dep_ctx.close(*exc_info)
        await capture.__aexit__(None, None, None)
        result: SynTasksResult[Any] = SynTasksResult(
            is_err=error is not None,
            log=None,
            return_value=returned,
            execution_time=round(execution_time, 3),
            error=error,
            labels=message.labels,
        )
        if error is not None:
            for middleware in reversed(self.app.middlewares):
                await maybe_awaitable(middleware.on_error(message, result, error))
        return result

    async def _await_runner(
        self,
        runner: asyncio.Task[Any],
        running: RunningTask,
        timeout: float | None,
        grace: float,
        message: SynTasksMessage,
    ) -> Any:
        try:
            done, _ = await asyncio.wait({runner}, timeout=timeout)
        except asyncio.CancelledError:
            runner.cancel()
            with contextlib.suppress(BaseException):
                await asyncio.wait({runner}, timeout=grace)
            raise
        if runner in done:
            if runner.cancelled():
                raise asyncio.CancelledError
            return runner.result()
        # Hard timeout: cancel, then give the task `grace` to clean up.
        runner.cancel()
        done, _ = await asyncio.wait({runner}, timeout=grace)
        if runner not in done:
            self._taint(
                f"Task {message.task_name} ({message.task_id}) ignored cancellation "
                f"for {grace}s after its timeout; this process will be recycled.",
            )
        else:
            with contextlib.suppress(BaseException):
                runner.result()
        raise TaskTimeoutError(task_name=message.task_name, timeout=timeout or 0.0)

    async def _await_thread(
        self,
        future: "asyncio.Future[Any]",
        running: RunningTask,
        timeout: float | None,
        grace: float,
        message: SynTasksMessage,
    ) -> Any:
        if timeout is None:
            return await future
        done, _ = await asyncio.wait({future}, timeout=timeout + grace)
        if future in done:
            if time.monotonic() - running.started > timeout:
                with contextlib.suppress(BaseException):
                    future.result()
                raise TaskTimeoutError(task_name=message.task_name, timeout=timeout)
            return future.result()
        self._taint(
            f"Sync task {message.task_name} ({message.task_id}) is still running "
            f"{timeout + grace}s after it started; threads can't be killed, so "
            "this process will be recycled. Use isolation = 'process' for a hard kill.",
        )
        raise TaskTimeoutError(task_name=message.task_name, timeout=timeout)
