"""
Per-task options: resolved from defaults, decorator arguments and config.

Precedence (low → high): library defaults → ``[defaults]`` in config →
decorator arguments → ``[tasks."<name>"]`` in config.
"""

import dataclasses
import inspect
import json
import random
from collections.abc import Callable, Mapping
from typing import Any

from syntasks.config.loader import ConfigError, build
from syntasks.config.schema import TaskConfig, TaskDefaults
from syntasks.units import Rate, parse_rate

__all__ = [
    "OPTION_NAMES",
    "TaskOptions",
    "extract_argument",
    "parse_code_options",
    "resolve_options",
]

OPTION_NAMES = frozenset(f.name for f in dataclasses.fields(TaskConfig))


@dataclasses.dataclass(kw_only=True, frozen=True)
class TaskOptions:
    """Effective options of one task."""

    queue: str = "default"
    priority: int = 0
    timeout: float | None = None
    grace: float = 30.0
    soft_timeout: float | None = None
    concurrency: int | None = None
    node_concurrency: int | None = None
    concurrency_key: str | None = None
    key_concurrency: int | None = None
    rate: Rate | None = None
    retries: int = 0
    retry_delay: float = 1.0
    retry_backoff: float = 2.0
    retry_max_delay: float = 300.0
    retry_jitter: bool = True
    max_payload: int | None = 256 * 1024
    max_result: int | None = None
    isolation: str = "none"
    memory_limit: int | None = None
    capture_logs: bool = False
    unique_for: float | None = None
    unique_key: str | None = None
    tags: tuple[str, ...] = ()
    result_ttl: float | None = 86400.0
    store_result: bool = True
    dead_letter: bool = True

    @property
    def gated(self) -> bool:
        """Whether the task needs a slot or rate check before it is fetched."""
        return bool(self.concurrency or self.node_concurrency or self.rate)

    def retry_delay_for(self, attempt: int) -> float:
        """
        Delay before retry number ``attempt`` (1-based).

        :param attempt: retry number.
        :return: seconds.
        """
        delay = min(
            self.retry_delay * self.retry_backoff ** max(attempt - 1, 0),
            self.retry_max_delay,
        )
        if self.retry_jitter:
            delay *= random.uniform(0.5, 1.0)  # noqa: S311
        return delay


def parse_code_options(task_name: str, options: Mapping[str, Any]) -> TaskConfig:
    """
    Validate options given to ``@app.task(...)``.

    :param task_name: task name, for error messages.
    :param options: decorator keyword arguments that are task options.
    :raises ConfigError: if an option is invalid.
    :return: parsed options (unset ones are None).
    """
    data = {k: (list(v) if isinstance(v, tuple) else v) for k, v in options.items()}
    if isinstance(data.get("rate"), Rate):
        data["rate"] = str(data["rate"])
    return build(TaskConfig, data, f"task {task_name}")


def resolve_options(
    defaults: TaskDefaults,
    *layers: TaskConfig | None,
) -> TaskOptions:
    """
    Merge option layers on top of the defaults.

    :param defaults: ``[defaults]`` from config.
    :param layers: later layers win; None values don't override.
    :raises ConfigError: if the combination is invalid.
    :return: effective options.
    """
    values: dict[str, Any] = {
        f.name: getattr(defaults, f.name) for f in dataclasses.fields(defaults)
    }
    rate_text: str | None = None
    rate_burst: int | None = None
    for layer in layers:
        if layer is None:
            continue
        for f in dataclasses.fields(layer):
            value = getattr(layer, f.name)
            if value is None:
                continue
            if f.name == "rate":
                rate_text = value
            elif f.name == "rate_burst":
                rate_burst = value
            else:
                values[f.name] = value
    known = {f.name for f in dataclasses.fields(TaskOptions)}
    values = {k: v for k, v in values.items() if k in known}
    if "tags" in values and values["tags"] is not None:
        values["tags"] = tuple(values["tags"])
    rate = parse_rate(rate_text, burst=rate_burst) if rate_text else None
    options = TaskOptions(**values, rate=rate)
    _check(options)
    return options


def _check(options: TaskOptions) -> None:
    for name in ("concurrency", "node_concurrency", "key_concurrency"):
        value = getattr(options, name)
        if value is not None and value < 1:
            raise ConfigError(detail=f"{name} must be at least 1, got {value}.")
    if options.key_concurrency and not options.concurrency_key:
        raise ConfigError(detail="key_concurrency needs concurrency_key.")
    if options.concurrency_key and not options.key_concurrency:
        object.__setattr__(options, "key_concurrency", 1)
    if (
        options.soft_timeout is not None
        and options.timeout is not None
        and options.soft_timeout >= options.timeout
    ):
        raise ConfigError(detail="soft_timeout must be shorter than timeout.")
    if options.memory_limit is not None and options.isolation != "process":
        raise ConfigError(detail="memory_limit needs isolation = 'process'.")
    if options.retries < 0:
        raise ConfigError(detail="retries must not be negative.")
    if options.priority < 0:
        raise ConfigError(detail="priority must not be negative.")


def extract_argument(
    spec: str,
    func: Callable[..., Any] | None,
    args: list[Any],
    kwargs: dict[str, Any],
) -> Any:
    """
    Find the value an option such as ``concurrency_key`` points at.

    :param spec: ``"kwargs.name"``, ``"args.0"`` or a parameter name.
    :param func: task function (to map parameter names to positions).
    :param args: call arguments.
    :param kwargs: call keyword arguments.
    :raises KeyError: if the argument is not present.
    :return: the value.
    """
    head, _, rest = spec.partition(".")
    if head == "kwargs" and rest:
        return _dig(kwargs[rest.split(".")[0]], rest.split(".")[1:])
    if head == "args" and rest:
        index, *path = rest.split(".")
        return _dig(args[int(index)], path)
    name, *path = spec.split(".")
    if name in kwargs:
        return _dig(kwargs[name], path)
    if func is not None:
        params = [
            p.name
            for p in inspect.signature(func).parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
        if name in params and params.index(name) < len(args):
            return _dig(args[params.index(name)], path)
    raise KeyError(spec)


def _dig(value: Any, path: list[str]) -> Any:
    for part in path:
        value = value[int(part)] if isinstance(value, list) else value[part]
    return value


def key_text(value: Any) -> str:
    """
    Stable text for a key value (used in Redis key names).

    :param value: argument value.
    :return: text.
    """
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
