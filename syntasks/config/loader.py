"""
Loading and validating the configuration.

Lookup: explicit path → ``$SYNTASKS_CONFIG`` → ``./syntasks.toml`` →
``[tool.syntasks]`` in ``./pyproject.toml`` → built-in defaults.

Precedence (low → high): defaults → file → profile → environment
(``SYNTASKS__WORKER__PROCESSES=4``) → explicit overrides (CLI flags).
Strings may reference environment variables: ``"${REDIS_URL}"``,
``"${REDIS_URL:-redis://localhost}"``.
"""

import dataclasses
import difflib
import os
import re
import tomllib
import types
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal, Union, get_args, get_origin, get_type_hints

from syntasks.config.schema import Config
from syntasks.exceptions import SynTasksError
from syntasks.units import parse_duration, parse_rate, parse_size

__all__ = [
    "ConfigError",
    "build",
    "find_config_file",
    "load_config",
]

ENV_PREFIX = "SYNTASKS__"
_INTERPOLATION = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


class ConfigError(SynTasksError, ValueError):
    """The configuration is invalid."""

    __template__ = "{detail}"
    detail: str


def find_config_file(
    path: str | os.PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
) -> Path | None:
    """
    Find the configuration file.

    :param path: explicit path.
    :param env: environment (defaults to ``os.environ``).
    :param cwd: directory to search (defaults to the current one).
    :raises ConfigError: if an explicit path doesn't exist.
    :return: the file, or None to use defaults.
    """
    env = os.environ if env is None else env
    cwd = cwd or Path.cwd()
    explicit = path or env.get("SYNTASKS_CONFIG")
    if explicit:
        candidate = Path(explicit)
        if not candidate.is_file():
            raise ConfigError(detail=f"Config file {str(candidate)!r} not found.")
        return candidate
    for name in ("syntasks.toml", ".syntasks.toml"):
        candidate = cwd / name
        if candidate.is_file():
            return candidate
    pyproject = cwd / "pyproject.toml"
    if pyproject.is_file():
        try:
            data = tomllib.loads(pyproject.read_text())
        except tomllib.TOMLDecodeError:
            return None
        if "syntasks" in data.get("tool", {}):
            return pyproject
    return None


def _read_file(path: Path) -> dict[str, Any]:
    try:
        data = tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(detail=f"{path}: invalid TOML: {exc}") from exc
    if path.name == "pyproject.toml":
        return dict(data.get("tool", {}).get("syntasks", {}))
    return data


def _deep_merge(
    base: dict[str, Any],
    extra: Mapping[str, Any],
    sources: dict[str, str],
    source: str,
    prefix: str = "",
) -> None:
    for key, value in extra.items():
        dotted = f"{prefix}{key}"
        if isinstance(value, Mapping) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value, sources, source, f"{dotted}.")
        else:
            base[key] = _copy(value)
            _mark(value, sources, source, dotted)


def _copy(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {k: _copy(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_copy(v) for v in value]
    return value


def _mark(value: Any, sources: dict[str, str], source: str, dotted: str) -> None:
    if isinstance(value, Mapping) and value:
        for key, sub in value.items():
            _mark(sub, sources, source, f"{dotted}.{key}")
    else:
        sources[dotted] = source


def _interpolate(value: Any, env: Mapping[str, str], path: str) -> Any:
    if isinstance(value, str):

        def replace(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            if name in env:
                return env[name]
            if default is not None:
                return default
            raise ConfigError(
                detail=f"{path or '<root>'}: environment variable {name} is not set.",
            )

        return _INTERPOLATION.sub(replace, value)
    if isinstance(value, dict):
        return {
            k: _interpolate(v, env, f"{path}.{k}" if path else k)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_interpolate(v, env, f"{path}[{i}]") for i, v in enumerate(value)]
    return value


def _parse_env_value(text: str) -> Any:
    try:
        return tomllib.loads(f"v = {text}")["v"]
    except tomllib.TOMLDecodeError:
        return text


def _set_dotted(data: dict[str, Any], dotted: list[str], value: Any) -> None:
    node = data
    for part in dotted[:-1]:
        child = node.get(part)
        if not isinstance(child, dict):
            child = {}
            node[part] = child
        node = child
    node[dotted[-1]] = value


def load_config(
    path: str | os.PathLike[str] | None = None,
    *,
    profile: str | None = None,
    overrides: Mapping[str, Any] | None = None,
    env: Mapping[str, str] | None = None,
    use_env: bool = True,
    search: bool = True,
) -> Config:
    """
    Load, merge and validate the configuration.

    :param path: config file (else looked up).
    :param profile: profile overlay to apply (else ``$SYNTASKS_PROFILE``).
    :param overrides: dotted keys set last, e.g. ``{"worker.processes": 4}``.
    :param env: environment (defaults to ``os.environ``).
    :param use_env: apply ``SYNTASKS__*`` overrides.
    :param search: look for a config file when ``path`` is not given.
    :raises ConfigError: if anything is invalid.
    :return: validated config.
    """
    env = os.environ if env is None else env
    sources: dict[str, str] = {}
    raw: dict[str, Any] = {}
    file_path = find_config_file(path, env) if (path or search) else None
    if file_path is not None:
        _deep_merge(raw, _read_file(file_path), sources, f"file:{file_path.name}")
    profiles = raw.pop("profiles", {})
    profile = profile or env.get("SYNTASKS_PROFILE") or None
    if profile:
        if profile not in profiles:
            known = ", ".join(sorted(profiles)) or "none"
            raise ConfigError(
                detail=f"Profile {profile!r} is not defined (known: {known}).",
            )
        _deep_merge(raw, profiles[profile], sources, f"profile:{profile}")
    raw = _interpolate(raw, env, "")
    if use_env:
        for name, text in env.items():
            if not name.startswith(ENV_PREFIX):
                continue
            dotted = [p.lower() for p in name[len(ENV_PREFIX) :].split("__") if p]
            if dotted:
                _set_dotted(raw, dotted, _parse_env_value(text))
                sources[".".join(dotted)] = f"env:{name}"
    for dotted_key, value in (overrides or {}).items():
        _set_dotted(raw, dotted_key.split("."), value)
        sources[dotted_key] = "cli"
    config = build(Config, raw)
    config.path = str(file_path) if file_path else None
    config.sources = sources
    return config


def _describe(hint: Any) -> str:  # noqa: PLR0911
    if hint is type(None):
        return "nothing"
    origin = get_origin(hint)
    if origin is Literal:
        return " or ".join(repr(a) for a in get_args(hint))
    if origin in (Union, types.UnionType):
        return " or ".join(_describe(a) for a in get_args(hint))
    if origin is list:
        return "a list"
    if origin is dict:
        return "a table"
    if dataclasses.is_dataclass(hint):
        return "a table"
    return {
        str: "a string",
        int: "an integer",
        float: "a number",
        bool: "true/false",
    }.get(
        hint,
        getattr(hint, "__name__", str(hint)),
    )


def _convert(  # noqa: C901, PLR0911, PLR0912
    value: Any,
    hint: Any,
    meta: Mapping[str, Any],
    path: str,
) -> Any:
    origin = get_origin(hint)
    if hint is Any:
        return value
    if origin in (Union, types.UnionType):
        args = [a for a in get_args(hint) if a is not type(None)]
        if value is None and len(args) != len(get_args(hint)):
            return None
        if len(args) == 1:
            return _convert(value, args[0], meta, path)
        for arg in args:
            try:
                return _convert(value, arg, meta, path)
            except ConfigError:
                continue
        raise ConfigError(detail=f"{path}: expected {_describe(hint)}, got {value!r}.")
    unit = meta.get("unit")
    try:
        if unit == "duration":
            return parse_duration(value)
        if unit == "size":
            return parse_size(value)
        if unit == "rate":
            parse_rate(value)
            return value
    except ValueError as exc:
        raise ConfigError(detail=f"{path}: {exc}") from exc
    if dataclasses.is_dataclass(hint):
        if not isinstance(value, Mapping):
            raise ConfigError(detail=f"{path}: expected a table, got {value!r}.")
        return build(hint, value, path)  # type: ignore[arg-type]
    if origin is dict:
        if not isinstance(value, Mapping):
            raise ConfigError(detail=f"{path}: expected a table, got {value!r}.")
        _, value_hint = get_args(hint)
        return {
            str(k): _convert(
                v,
                value_hint,
                {},
                f'{path}."{k}"' if not str(k).isidentifier() else f"{path}.{k}",
            )
            for k, v in value.items()
        }
    if origin is list:
        if not isinstance(value, (list, tuple)):
            raise ConfigError(detail=f"{path}: expected a list, got {value!r}.")
        (item_hint,) = get_args(hint)
        return [_convert(v, item_hint, {}, f"{path}[{i}]") for i, v in enumerate(value)]
    if origin is Literal:
        if value not in get_args(hint):
            raise ConfigError(
                detail=f"{path}: expected {_describe(hint)}, got {value!r}.",
            )
        return value
    if hint is bool:
        if not isinstance(value, bool):
            raise ConfigError(detail=f"{path}: expected true/false, got {value!r}.")
        return value
    if hint is int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ConfigError(detail=f"{path}: expected an integer, got {value!r}.")
        return value
    if hint is float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ConfigError(detail=f"{path}: expected a number, got {value!r}.")
        return float(value)
    if hint is str:
        if not isinstance(value, str):
            raise ConfigError(detail=f"{path}: expected a string, got {value!r}.")
        return value
    raise ConfigError(detail=f"{path}: unsupported setting type {hint!r}.")


def _allows_none(hint: Any) -> bool:
    return get_origin(hint) in (Union, types.UnionType) and type(None) in get_args(hint)


def build(cls: type[Any], data: Mapping[str, Any], path: str = "") -> Any:
    """
    Build a settings dataclass from plain data, validating every value.

    :param cls: settings dataclass.
    :param data: plain data (e.g. parsed TOML).
    :param path: dotted path for error messages.
    :raises ConfigError: on unknown keys, wrong types or missing values.
    :return: dataclass instance.
    """
    hints = get_type_hints(cls)
    fields = {
        f.name: f for f in dataclasses.fields(cls) if not f.metadata.get("internal")
    }
    kwargs: dict[str, Any] = {}
    for key, value in data.items():
        dotted = f"{path}.{key}" if path else key
        if key not in fields:
            close = difflib.get_close_matches(key, fields, n=1)
            hint = f" Did you mean {close[0]!r}?" if close else ""
            raise ConfigError(detail=f"{dotted}: unknown setting.{hint}")
        kwargs[key] = _convert(value, hints[key], fields[key].metadata, dotted)
    instance = cls(**kwargs)
    for name in fields:
        if getattr(instance, name) is None and not _allows_none(hints[name]):
            dotted = f"{path}.{name}" if path else name
            raise ConfigError(detail=f"{dotted}: required setting is missing.")
    return instance


def apply_overrides(config: Config, overrides: Mapping[str, Any]) -> None:
    """
    Set dotted settings on an existing config (e.g. CLI flags).

    :param config: config to change in place.
    :param overrides: ``{"worker.processes": 4, ...}``.
    :raises ConfigError: if a key or value is invalid.
    """
    for dotted, value in overrides.items():
        parts = dotted.split(".")
        target: Any = config
        for part in parts[:-1]:
            target = getattr(target, part, None)
            if target is None or not dataclasses.is_dataclass(target):
                raise ConfigError(detail=f"{dotted}: unknown setting.")
        name = parts[-1]
        fields = {f.name: f for f in dataclasses.fields(target)}
        if name not in fields:
            raise ConfigError(detail=f"{dotted}: unknown setting.")
        hint = get_type_hints(type(target))[name]
        setattr(target, name, _convert(value, hint, fields[name].metadata, dotted))
        config.sources[dotted] = "cli"
