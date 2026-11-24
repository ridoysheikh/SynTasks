"""Distributed task manager."""

from importlib.metadata import version

from syntasks.abc.broker import AsyncBroker, AsyncSynTasksDecoratedTask
from syntasks.abc.formatter import SynTasksFormatter
from syntasks.abc.middleware import SynTasksMiddleware
from syntasks.abc.result_backend import AsyncResultBackend
from syntasks.abc.schedule_source import ScheduleSource
from syntasks.acks import AckableMessage, AcknowledgeType
from syntasks.brokers.inmemory_broker import InMemoryBroker
from syntasks.brokers.multi_broker import MultiBroker
from syntasks.brokers.shared_broker import async_shared_broker
from syntasks.context import Context
from syntasks.dependencies import Depends as SynTasksDepends
from syntasks.events import SynTasksEvents
from syntasks.exceptions import (
    NoResultError,
    ResultGetError,
    ResultIsReadyError,
    SecurityError,
    SendTaskError,
    SynTasksError,
    SynTasksResultTimeoutError,
)
from syntasks.funcs import gather
from syntasks.message import BrokerMessage, SynTasksMessage
from syntasks.middlewares import (
    PrometheusMiddleware,
    SimpleRetryMiddleware,
    SmartRetryMiddleware,
)
from syntasks.redis import (
    Redis,
    RedisListBroker,
    RedisPubSubBroker,
    RedisResultBackend,
    RedisScheduleSource,
    RedisStreamBroker,
)
from syntasks.result import SynTasksResult
from syntasks.scheduler.scheduled_task import ScheduledTask
from syntasks.scheduler.scheduler import SynTasksScheduler
from syntasks.state import SynTasksState
from syntasks.task import AsyncSynTasksTask

__version__ = version("syntasks")

__all__ = [
    "AckableMessage",
    "AcknowledgeType",
    "AsyncBroker",
    "AsyncResultBackend",
    "AsyncSynTasksDecoratedTask",
    "AsyncSynTasksTask",
    "BrokerMessage",
    "Context",
    "InMemoryBroker",
    "MultiBroker",
    "NoResultError",
    "PrometheusMiddleware",
    "Redis",
    "RedisListBroker",
    "RedisPubSubBroker",
    "RedisResultBackend",
    "RedisScheduleSource",
    "RedisStreamBroker",
    "ResultGetError",
    "ResultIsReadyError",
    "ScheduleSource",
    "ScheduledTask",
    "SecurityError",
    "SendTaskError",
    "SimpleRetryMiddleware",
    "SmartRetryMiddleware",
    "SynTasksDepends",
    "SynTasksError",
    "SynTasksEvents",
    "SynTasksFormatter",
    "SynTasksMessage",
    "SynTasksMiddleware",
    "SynTasksResult",
    "SynTasksResultTimeoutError",
    "SynTasksScheduler",
    "SynTasksState",
    "__version__",
    "async_shared_broker",
    "gather",
]
