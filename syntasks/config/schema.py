"""
Configuration schema.

Every setting is a dataclass field. Field metadata says how to parse it
(``unit``: duration / size / rate), its scope (``node``: this machine,
``cluster``: the same for every machine) and a short ``doc``.
"""

from dataclasses import dataclass, field
from typing import Any, Literal

__all__ = [
    "AppConfig",
    "ChannelsConfig",
    "Config",
    "LoggingConfig",
    "NodeConfig",
    "QueueConfig",
    "RedisConfig",
    "ResultsConfig",
    "ScheduleConfig",
    "SchedulerConfig",
    "SentinelConfig",
    "TaskConfig",
    "TaskDefaults",
    "WorkerConfig",
]


def setting(
    default: Any = None,
    *,
    doc: str,
    unit: str | None = None,
    scope: str = "node",
    factory: Any = None,
) -> Any:
    """
    Declare a setting.

    :param default: default value.
    :param doc: one-line description.
    :param unit: ``duration``, ``size`` or ``rate``.
    :param scope: ``node`` or ``cluster``.
    :param factory: default factory for mutable defaults.
    :return: dataclass field.
    """
    metadata = {"doc": doc, "unit": unit, "scope": scope}
    if factory is not None:
        return field(default_factory=factory, metadata=metadata)
    return field(default=default, metadata=metadata)


@dataclass(kw_only=True)
class AppConfig:
    """``[app]``."""

    name: str = setting("default", doc="Redis namespace: keys are syntasks:{name}:…")
    app: str | None = setting(
        doc="Import path of the SynTasks object, e.g. 'billing.app:app'.",
    )
    modules: list[str] = setting(
        factory=list,
        doc="Modules imported by workers so their tasks register.",
    )
    revision: int | None = setting(
        doc="Deploy revision (e.g. CI build number); orders cluster syncs.",
        scope="cluster",
    )


@dataclass(kw_only=True)
class SentinelConfig:
    """``[redis.sentinel]``."""

    service: str = setting(doc="Master name configured in Sentinel.")
    nodes: list[str] = setting(factory=list, doc="Sentinel addresses 'host:port'.")
    username: str | None = setting(doc="Sentinel ACL user.")
    password: str | None = setting(doc="Sentinel password.")


@dataclass(kw_only=True)
class RedisConfig:
    """``[redis]``."""

    url: str = setting("redis://localhost:6379/0", doc="Redis URL.")
    pool_size: int = setting(50, doc="Connections per process.")
    connect_timeout: float = setting(5.0, unit="duration", doc="Connect timeout.")
    socket_timeout: float | None = setting(
        30.0,
        unit="duration",
        doc="Timeout for non-blocking commands.",
    )
    sentinel: SentinelConfig | None = setting(doc="Use Redis Sentinel.")


@dataclass(kw_only=True)
class WorkerConfig:
    """``[worker]`` (node scope)."""

    processes: int = setting(2, doc="Worker processes; 0 = CPU count.")
    max_async_tasks: int = setting(
        100,
        doc="Tasks running at once on this machine (split across processes).",
    )
    max_threads: int | None = setting(doc="Threads for sync tasks, per process.")
    prefetch: int = setting(
        1,
        doc="Messages fetched ahead of free capacity, per process.",
    )
    queues: list[str] = setting(
        factory=lambda: ["default"],
        doc="Queues this machine serves, in priority order.",
    )
    tags: list[str] = setting(factory=list, doc="Tags for task routing.")
    shutdown_grace: float = setting(
        30.0,
        unit="duration",
        doc="Time running tasks get to finish on shutdown.",
    )
    memory_soft_limit: int | None = setting(
        unit="size",
        doc="Per process: above it, stop taking work, drain and restart.",
    )
    memory_hard_limit: int | None = setting(
        unit="size",
        doc="Per process RLIMIT_AS (Linux); allocations above it fail.",
    )
    memory_check_interval: float = setting(
        5.0,
        unit="duration",
        doc="How often memory is checked.",
    )
    max_tasks_per_process: int = setting(
        0,
        doc="Restart a process after this many tasks (0 = never).",
    )
    max_restarts: int = setting(
        0,
        doc="Stop the worker after this many crash restarts (0 = never).",
    )
    loop: Literal["auto", "asyncio", "uvloop"] = setting(
        "auto",
        doc="Event loop; 'auto' uses uvloop if installed.",
    )
    loop_block_warn: float = setting(
        1.0,
        unit="duration",
        doc="Log a stack trace when the event loop is blocked this long.",
    )
    loop_block_kill: float = setting(
        0.0,
        unit="duration",
        doc="Restart the process when the loop is blocked this long (0 = off).",
    )
    ack: Literal["when_received", "when_executed", "when_saved"] = setting(
        "when_saved",
        doc="When messages are acknowledged.",
    )
    claim_idle: float = setting(
        60.0,
        unit="duration",
        doc="Messages unacknowledged this long are taken over by others.",
    )
    xread_block: float = setting(
        1.0,
        unit="duration",
        doc="How long one blocking read waits.",
    )


@dataclass(kw_only=True)
class NodeConfig:
    """``[node]``."""

    heartbeat: float = setting(5.0, unit="duration", doc="Heartbeat interval.")
    dead_after: float = setting(
        30.0,
        unit="duration",
        doc="A node without heartbeat this long is dead.",
        scope="cluster",
    )


@dataclass(kw_only=True)
class TaskDefaults:
    """``[defaults]``: options every task starts from (cluster scope)."""

    queue: str = setting("default", doc="Queue.", scope="cluster")
    priority: int = setting(0, doc="Priority (0 = highest).", scope="cluster")
    timeout: float | None = setting(
        None,
        unit="duration",
        doc="Hard time limit of one run.",
        scope="cluster",
    )
    grace: float = setting(
        30.0,
        unit="duration",
        doc="Cleanup time after the timeout before the run is killed.",
        scope="cluster",
    )
    soft_timeout: float | None = setting(
        None,
        unit="duration",
        doc="Warn the task (Deadline) at this time.",
        scope="cluster",
    )
    max_payload: int = setting(
        256 * 1024,
        unit="size",
        doc="Max size of one task message.",
        scope="cluster",
    )
    max_result: int | None = setting(
        None,
        unit="size",
        doc="Max size of a stored result.",
        scope="cluster",
    )
    retries: int = setting(0, doc="Retries after an exception.", scope="cluster")
    retry_delay: float = setting(
        1.0,
        unit="duration",
        doc="First retry delay.",
        scope="cluster",
    )
    retry_backoff: float = setting(
        2.0,
        doc="Retry delay multiplier per attempt (1 = fixed).",
        scope="cluster",
    )
    retry_max_delay: float = setting(
        300.0,
        unit="duration",
        doc="Max retry delay.",
        scope="cluster",
    )
    retry_jitter: bool = setting(True, doc="Randomize retry delays.", scope="cluster")
    result_ttl: float | None = setting(
        86400.0,
        unit="duration",
        doc="How long results are kept.",
        scope="cluster",
    )
    store_result: bool = setting(True, doc="Store results.", scope="cluster")
    dead_letter: bool = setting(
        True,
        doc="Move finally failed messages to the queue's dead letters.",
        scope="cluster",
    )


@dataclass(kw_only=True)
class TaskConfig:
    """``[tasks."module:function"]``: per-task options (cluster scope)."""

    queue: str | None = setting(doc="Queue.", scope="cluster")
    priority: int | None = setting(doc="Priority (0 = highest).", scope="cluster")
    timeout: float | None = setting(
        unit="duration",
        doc="Hard time limit.",
        scope="cluster",
    )
    grace: float | None = setting(unit="duration", doc="Cleanup time.", scope="cluster")
    soft_timeout: float | None = setting(
        unit="duration",
        doc="Soft time limit.",
        scope="cluster",
    )
    concurrency: int | None = setting(
        doc="Runs at once across all machines.",
        scope="cluster",
    )
    node_concurrency: int | None = setting(
        doc="Runs at once per machine.",
        scope="cluster",
    )
    concurrency_key: str | None = setting(
        doc="Argument that groups runs, e.g. 'kwargs.customer_id' or 'args.0'.",
        scope="cluster",
    )
    key_concurrency: int | None = setting(
        doc="Runs at once per key (1 = strict order per key).",
        scope="cluster",
    )
    rate: str | None = setting(
        unit="rate",
        doc="Rate limit, e.g. '100/s'.",
        scope="cluster",
    )
    rate_burst: int | None = setting(doc="Rate limit burst.", scope="cluster")
    retries: int | None = setting(doc="Retries after an exception.", scope="cluster")
    retry_delay: float | None = setting(
        unit="duration",
        doc="First retry delay.",
        scope="cluster",
    )
    retry_backoff: float | None = setting(doc="Retry multiplier.", scope="cluster")
    retry_max_delay: float | None = setting(
        unit="duration",
        doc="Max retry delay.",
        scope="cluster",
    )
    retry_jitter: bool | None = setting(doc="Randomize retry delays.", scope="cluster")
    max_payload: int | None = setting(
        unit="size",
        doc="Max message size.",
        scope="cluster",
    )
    max_result: int | None = setting(
        unit="size",
        doc="Max result size.",
        scope="cluster",
    )
    isolation: Literal["none", "process"] | None = setting(
        doc="'process' runs each call in a child process (hard kill, memory limit).",
        scope="cluster",
    )
    memory_limit: int | None = setting(
        unit="size",
        doc="Memory limit of an isolated run.",
        scope="cluster",
    )
    capture_logs: bool | None = setting(
        doc="Capture this task's logs.",
        scope="cluster",
    )
    unique_for: float | None = setting(
        unit="duration",
        doc="Drop identical kicks within this window.",
        scope="cluster",
    )
    unique_key: str | None = setting(
        doc="Argument used for uniqueness instead of all arguments.",
        scope="cluster",
    )
    tags: list[str] | None = setting(
        doc="Only machines with these tags run it.",
        scope="cluster",
    )
    result_ttl: float | None = setting(
        unit="duration",
        doc="Result TTL.",
        scope="cluster",
    )
    store_result: bool | None = setting(doc="Store results.", scope="cluster")
    dead_letter: bool | None = setting(
        doc="Dead-letter final failures.",
        scope="cluster",
    )


@dataclass(kw_only=True)
class QueueConfig:
    """``[queues.<name>]`` (cluster scope)."""

    priorities: int = setting(1, doc="Priority levels.", scope="cluster")
    max_length: int | None = setting(doc="Max waiting messages.", scope="cluster")
    on_full: Literal["reject", "drop_oldest"] = setting(
        "reject",
        doc="What a kick does when the queue is full.",
        scope="cluster",
    )
    max_deliveries: int = setting(
        5,
        doc="Deliveries (worker crashes) before a message is dead-lettered.",
        scope="cluster",
    )
    dead_letter_max: int = setting(
        10_000,
        doc="Dead letters kept per queue.",
        scope="cluster",
    )


@dataclass(kw_only=True)
class SchedulerConfig:
    """``[scheduler]`` (cluster scope)."""

    enabled: bool = setting(True, doc="Run the dispatcher on this machine's workers.")
    shards: int = setting(1, doc="Dispatcher shards.", scope="cluster")
    min_interval: float = setting(
        1.0,
        unit="duration",
        doc="Shortest allowed recurrence.",
        scope="cluster",
    )
    misfire_grace: float = setting(
        30.0,
        unit="duration",
        doc="A fire later than this counts as missed.",
        scope="cluster",
    )
    max_catchup: int = setting(
        10,
        doc="Missed fires replayed with misfire='all'.",
        scope="cluster",
    )
    max_dynamic: int = setting(
        100_000,
        doc="Max dynamic schedules.",
        scope="cluster",
    )
    max_payload: int = setting(
        16 * 1024,
        unit="size",
        doc="Max size of schedule arguments.",
        scope="cluster",
    )
    prune_removed: bool = setting(
        True,
        doc="Delete code schedules that were removed from code.",
        scope="cluster",
    )
    lease_ttl: float = setting(
        5.0,
        unit="duration",
        doc="Dispatcher lease TTL.",
        scope="cluster",
    )
    claim_lease: float = setting(
        30.0,
        unit="duration",
        doc="Time a claimed fire has to be committed.",
        scope="cluster",
    )
    max_sleep: float = setting(1.0, unit="duration", doc="Longest dispatcher sleep.")
    batch: int = setting(100, doc="Fires claimed per round.")
    history: int = setting(20, doc="Runs kept per schedule.", scope="cluster")


@dataclass(kw_only=True)
class ScheduleConfig:
    """``[schedules.<id>]``: a static schedule declared in config."""

    task: str = setting(doc="Task name, e.g. 'billing.tasks:report'.")
    every: float | None = setting(unit="duration", doc="Interval.")
    cron: str | None = setting(doc="Cron expression (5 or 6 fields).")
    at: str | None = setting(doc="One-shot time (ISO 8601).")
    tz: str | None = setting(doc="Time zone for cron.")
    args: list[Any] = setting(factory=list, doc="Positional arguments (JSON).")
    kwargs: dict[str, Any] = setting(factory=dict, doc="Keyword arguments (JSON).")
    start_at: str | None = setting(doc="Don't fire before (ISO 8601).")
    end_at: str | None = setting(doc="Don't fire after (ISO 8601).")
    jitter: float | None = setting(unit="duration", doc="Random delay per fire.")
    misfire: Literal["skip", "once", "all"] | None = setting(doc="Missed fires policy.")
    overlap: Literal["skip", "allow", "queue"] | None = setting(doc="Overlap policy.")
    max_instances: int | None = setting(doc="Runs at once for overlap='queue'.")
    timeout: float | None = setting(unit="duration", doc="Run time limit.")
    retries: int | None = setting(doc="Retries per fire.")
    paused: bool = setting(False, doc="Start paused.")


@dataclass(kw_only=True)
class ChannelsConfig:
    """``[channels]``."""

    max_handlers: int = setting(500, doc="Channel handlers per process.")
    max_message: int = setting(
        64 * 1024,
        unit="size",
        doc="Max message size.",
        scope="cluster",
    )
    retention: float = setting(
        600.0,
        unit="duration",
        doc="How long broadcast messages are kept for replay.",
        scope="cluster",
    )
    max_length: int = setting(
        10_000,
        doc="Max broadcast messages kept.",
        scope="cluster",
    )
    inbox_max: int = setting(
        10_000,
        doc="Max unprocessed inbox messages.",
        scope="cluster",
    )
    idle_expiry: float = setting(
        86400.0,
        unit="duration",
        doc="Unused channels are deleted after this.",
        scope="cluster",
    )
    lease_ttl: float = setting(
        10.0,
        unit="duration",
        doc="Handler ownership lease.",
        scope="cluster",
    )
    presence_ttl: float = setting(30.0, unit="duration", doc="Reader presence timeout.")
    hub_max_streams: int = setting(500, doc="Channels per hub connection.")
    reader_buffer: int = setting(1000, doc="Messages buffered per reader.")


@dataclass(kw_only=True)
class ResultsConfig:
    """``[results]``."""

    backend: Literal["redis", "none"] = setting("redis", doc="Where results go.")
    serializer: Literal["json", "pickle", "orjson", "msgpack", "cbor"] = setting(
        "json",
        doc="Result encoding.",
        scope="cluster",
    )
    keep_results: bool = setting(True, doc="Keep results after reading.")


@dataclass(kw_only=True)
class LoggingConfig:
    """``[logging]``."""

    level: str = setting("INFO", doc="Log level.")
    format: Literal["text", "json"] = setting("text", doc="Log format.")
    configure: bool = setting(True, doc="Configure Python logging in workers.")
    capture_level: str = setting("INFO", doc="Lowest level captured for tasks.")
    capture_max_lines: int = setting(1000, doc="Lines kept per captured run.")
    capture_max_record: int = setting(8192, unit="size", doc="Max size of one record.")
    capture_retention: float = setting(
        86400.0,
        unit="duration",
        doc="How long captured logs are kept.",
    )


@dataclass(kw_only=True)
class Config:
    """The whole configuration."""

    app: AppConfig = field(default_factory=AppConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    worker: WorkerConfig = field(default_factory=WorkerConfig)
    node: NodeConfig = field(default_factory=NodeConfig)
    defaults: TaskDefaults = field(default_factory=TaskDefaults)
    tasks: dict[str, TaskConfig] = field(default_factory=dict)
    queues: dict[str, QueueConfig] = field(default_factory=dict)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    schedules: dict[str, ScheduleConfig] = field(default_factory=dict)
    channels: ChannelsConfig = field(default_factory=ChannelsConfig)
    results: ResultsConfig = field(default_factory=ResultsConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    # Not settings: where the config came from.
    path: str | None = field(
        default=None,
        repr=False,
        compare=False,
        metadata={"internal": True},
    )
    sources: dict[str, str] = field(
        default_factory=dict,
        repr=False,
        compare=False,
        metadata={"internal": True},
    )

    def queue(self, name: str) -> QueueConfig:
        """
        Settings of a queue (defaults if not configured).

        :param name: queue name.
        :return: queue settings.
        """
        return self.queues.get(name) or QueueConfig()
