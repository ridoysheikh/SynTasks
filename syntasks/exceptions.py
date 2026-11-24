from typing import Any

from syntasks.error import Error


class SynTasksError(Error):
    """Base exception for all errors."""

    __template__ = "Exception occurred"


class SynTasksResultTimeoutError(SynTasksError):
    """Waiting for task results has timed out."""

    __template__ = "Waiting for task results has timed out, timeout={timeout}"
    timeout: float | None = None


class BrokerError(SynTasksError):
    """Base class for all broker errors."""

    __template__ = "Base exception for all broker errors"


class ListenError(SynTasksError):
    """Error if the broker is unable to listen to the queue."""


class SharedBrokerListenError(ListenError):
    """Error when someone tries to listen to the queue with shared broker."""

    __template__ = "Shared broker cannot listen"


class SendTaskError(BrokerError):
    """Error if the broker was unable to send the task to the queue."""

    __template__ = "Cannot send task to the queue"


class SharedBrokerSendTaskError(SendTaskError):
    """Error when someone tries to send task with shared broker."""

    __template__ = (
        "You cannot use kiq directly on shared task without setting the default_broker."
    )


class UnknownTaskError(SendTaskError):
    """Error if task is unknown."""

    __template__ = "Cannot send unknown task to the queue, task name - {task_name}"
    task_name: str


class ResultBackendError(SynTasksError):
    """Base class for all ResultBackend errors."""

    __template__ = "Base exception for all result backend errors"


class ResultGetError(ResultBackendError):
    """Error if ResultBackend was unable to get result."""

    __template__ = "Cannot get result for the task"


class ResultSetError(ResultBackendError):
    """Error if ResultBackend was unable to set result."""

    __template__ = "Cannot set result for the task"


class ResultIsReadyError(ResultBackendError):
    """Error if ResultBackend was unable to find out if the task is ready."""

    __template__ = "Cannot find out if the task is ready"


class SecurityError(SynTasksError):
    """Security related exception."""

    __template__ = "Security exception occurred: {description}"
    description: str


class NoResultError(SynTasksError):
    """Error if user does not want to set result."""

    __template__ = "User doesn't want to set result"


class TaskRejectedError(SynTasksError):
    """Task was rejected."""

    __template__ = "Task was rejected"


class TaskBrokerMismatchError(TaskRejectedError):
    """Task has a different broker than the one it was registered to."""

    __template__ = "Task already has a different broker ({broker})"
    broker: Any


class ResultIsMissingError(ResultGetError):
    """Error if there is no result for the task."""

    __template__ = "There is no result for task {task_id}"
    task_id: str


class QueueFullError(SendTaskError):
    """The queue reached its ``max_length``."""

    __template__ = "Queue {queue!r} is full ({length} messages)."
    queue: str
    length: int


class DuplicateTaskError(SendTaskError):
    """An identical task was kicked within its ``unique_for`` window."""

    __template__ = "Task is a duplicate of {task_id}."
    task_id: str


class TaskTimeoutError(SynTasksError):
    """A task run exceeded its time limit."""

    __template__ = "Task {task_name} exceeded its time limit of {timeout}s."
    task_name: str
    timeout: float


class TaskMemoryError(SynTasksError):
    """An isolated task run exceeded its memory limit."""

    __template__ = "Task {task_name} exceeded its memory limit of {limit} bytes."
    task_name: str
    limit: int


class TaskCancelledError(SynTasksError):
    """A task run was cancelled on request."""

    __template__ = "Task {task_id} was cancelled."
    task_id: str


class ResultTooLargeError(SynTasksError):
    """A task's result is bigger than ``max_result``."""

    __template__ = "Result of {task_id} is {size} bytes; the limit is {limit}."
    task_id: str
    size: int
    limit: int
