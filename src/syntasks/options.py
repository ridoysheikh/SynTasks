"""
Task options: layers set in code, resolved along the module chain.

Every task sits in a chain ``SynTasks(...) → Module(...) → ... → @task(...)``.
Each link is an :class:`OptionLayer`; the nearest link that sets an option
wins. ``timeout``, ``retries`` and ``max_tasks`` are required: some link must
set them (``None`` is an explicit "unbounded").
"""

import dataclasses
import difflib
import inspect
import json
import random
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from syntasks.config.loader import ConfigError, build
from syntasks.config.schema import TaskConfig
from syntasks.units import Rate, parse_rate

__all__ = [
    "OPTION_NAMES",
    "REQUIRED",
    "MissingLimitError",
    "OptionLayer",
    "TaskOptions",
    "extract_argument",
    "parse_layer",
    "resolve_chain",
]

OPTION_NAMES = frozenset(f.name for f in dataclasses.fields(TaskConfig))
REQUIRED = ("timeout", "retries", "max_tasks")


class MissingLimitError(ConfigError):
    """A task, schedule or channel doesn't set a required limit anywhere."""


@dataclasses.dataclass(kw_only=True, frozen=True)
class TaskOptions:
    """Effective options of one task."""

    queue: str = "default"
    priority: int = 0
    timeout: float | None = None
    grace: float = 30.0
    soft_timeout: float | None = None
    max_tasks: int | None = None
    max_tasks_per_node: int | None = None
    key: str | None = None
    max_tasks_per_key: int | None = None
    rate: Rate | None = None
    rate_per_key: Rate | None = None
    fair: bool = False
    debounce: float | None = None
    throttle: float | None = None
    events: bool = False
    on_success: str | None = None
    on_failure: str | None = None
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
        return bool(self.max_tasks or self.max_tasks_per_node or self.rate or self.fair)

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


_NULLABLE = frozenset(
    f.name
    for f in dataclasses.fields(TaskOptions)
    if f.default is None and f.name not in ("rate", "rate_per_key")
) | {"rate", "rate_burst", "rate_per_key"}


@dataclasses.dataclass(frozen=True)
class OptionLayer:
    """
    Options one link of a chain sets.

    :param values: parsed options (unset ones are None).
    :param explicit: names given at this link, including explicit ``None``.
    :param owner: the link in error messages, e.g. ``'Module("billing")'``.
    """

    values: TaskConfig
    explicit: frozenset[str]
    owner: str

    def as_dict(self) -> dict[str, Any]:
        """The options this link sets, as plain data."""
        return {
            name: _plain(getattr(self.values, name)) for name in sorted(self.explicit)
        }


def _plain(value: Any) -> Any:
    return list(value) if isinstance(value, tuple) else value


def parse_layer(owner: str, options: Mapping[str, Any]) -> OptionLayer:
    """
    Validate options given in code.

    :param owner: the link, for error messages.
    :param options: keyword arguments that are task options.
    :raises ConfigError: on unknown names or invalid values.
    :return: the layer.
    """
    for name in options:
        if name not in OPTION_NAMES:
            close = difflib.get_close_matches(name, OPTION_NAMES, n=1)
            hint = f" Did you mean {close[0]!r}?" if close else ""
            raise ConfigError(detail=f"{owner}: unknown option {name!r}.{hint}")
        if options[name] is None and name not in _NULLABLE:
            fix = " Use 0 for no retries." if name == "retries" else ""
            raise ConfigError(detail=f"{owner}: {name} can't be None.{fix}")
    data = {k: _plain(v) for k, v in options.items() if v is not None}
    for name in ("on_success", "on_failure"):
        if name in data and not isinstance(data[name], str):
            data[name] = getattr(data[name], "task_name", data[name])
    if isinstance(data.get("rate"), Rate):
        data["rate"] = str(data["rate"])
    return OptionLayer(build(TaskConfig, data, owner), frozenset(options), owner)


def resolve_chain(
    layers: Sequence[OptionLayer],
    *,
    what: str,
    required: bool = True,
) -> TaskOptions:
    """
    Merge a chain of layers, outermost first; the nearest link wins.

    :param layers: ``[app, module, ..., declaration]``.
    :param what: the declaration, for error messages.
    :param required: check :data:`REQUIRED`.
    :raises MissingLimitError: if a required option is set nowhere.
    :raises ConfigError: if the combination is invalid.
    :return: effective options.
    """
    values: dict[str, Any] = {}
    for layer in layers:
        for name in layer.explicit:
            values[name] = getattr(layer.values, name)
    if required:
        missing = [name for name in REQUIRED if name not in values]
        if missing:
            owners = [layer.owner for layer in reversed(layers)]
            places = (
                ", ".join(owners[:-1]) + f" or {owners[-1]}"
                if len(owners) > 1
                else owners[0]
            )
            raise MissingLimitError(
                detail=f"{what} has no {' or '.join(missing)}; set it on {places}.",
            )
    rate_text = values.pop("rate", None)
    rate_burst = values.pop("rate_burst", None)
    per_key_text = values.pop("rate_per_key", None)
    known = {f.name for f in dataclasses.fields(TaskOptions)}
    values = {k: v for k, v in values.items() if k in known}
    if values.get("tags") is not None:
        values["tags"] = tuple(values["tags"])
    rate = parse_rate(rate_text, burst=rate_burst) if rate_text else None
    per_key = parse_rate(per_key_text) if per_key_text else None
    options = TaskOptions(**values, rate=rate, rate_per_key=per_key)
    _check(options)
    return options


def _check(options: TaskOptions) -> None:
    _check_limits(options)
    _check_keys(options)


def _check_keys(options: TaskOptions) -> None:
    for name in ("max_tasks_per_key", "rate_per_key", "fair", "debounce", "throttle"):
        if getattr(options, name) and not options.key:
            raise ConfigError(detail=f"{name} needs key, e.g. key='kwargs.tenant_id'.")
    exclusive = (
        ("debounce", "throttle"),
        ("throttle", "unique_for"),
        ("debounce", "fair"),
    )
    for first, second in exclusive:
        if getattr(options, first) and getattr(options, second):
            raise ConfigError(detail=f"{first} and {second} can't be combined.")


def _check_limits(options: TaskOptions) -> None:
    for name in ("max_tasks", "max_tasks_per_node", "max_tasks_per_key"):
        value = getattr(options, name)
        if value is not None and value < 1:
            raise ConfigError(detail=f"{name} must be at least 1, got {value}.")
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
    Find the value an option such as ``key`` points at.

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
