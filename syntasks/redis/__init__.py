"""
Built-in Redis support, implemented with the standard library only.

* :class:`Redis` - async client (pool, pipelines, pub/sub, TLS, Sentinel).
* :class:`RedisStreamBroker` - reliable broker with acknowledgements.
* :class:`RedisListBroker` - simple, fast broker on a list.
* :class:`RedisPubSubBroker` - broadcast broker.
* :class:`RedisResultBackend` - results and progress.
* :class:`RedisScheduleSource` - persistent schedules.
"""

from syntasks.redis.broker import (
    BaseRedisBroker,
    RedisListBroker,
    RedisPubSubBroker,
    RedisStreamBroker,
)
from syntasks.redis.client import Pipeline, PubSub, PubSubMessage, Redis
from syntasks.redis.connection import Connection, ConnectionOptions
from syntasks.redis.errors import (
    AuthenticationError,
    MasterNotFoundError,
    ProtocolError,
    RedisConnectionError,
    RedisError,
    RedisTimeoutError,
    ResponseError,
)
from syntasks.redis.pool import ConnectionPool, SentinelConnectionPool
from syntasks.redis.result_backend import RedisResultBackend
from syntasks.redis.schedule_source import RedisScheduleSource

__all__ = [
    "AuthenticationError",
    "BaseRedisBroker",
    "Connection",
    "ConnectionOptions",
    "ConnectionPool",
    "MasterNotFoundError",
    "Pipeline",
    "ProtocolError",
    "PubSub",
    "PubSubMessage",
    "Redis",
    "RedisConnectionError",
    "RedisError",
    "RedisListBroker",
    "RedisPubSubBroker",
    "RedisResultBackend",
    "RedisScheduleSource",
    "RedisStreamBroker",
    "RedisTimeoutError",
    "ResponseError",
    "SentinelConnectionPool",
]
