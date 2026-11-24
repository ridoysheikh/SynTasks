"""Shared helpers of the CLI commands."""

import argparse
import asyncio
import dataclasses
import datetime as dt
import json
import sys
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, TypeVar

from syntasks.config import Config, ConfigError, load_config

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks

__all__ = [
    "add_config_args",
    "app_from_args",
    "config_from_args",
    "print_json",
    "print_table",
    "run_with_app",
]

_T = TypeVar("_T")


def add_config_args(parser: argparse.ArgumentParser, *, app_arg: bool = True) -> None:
    """
    Add ``--config``, ``--profile`` and the optional app argument.

    :param parser: parser.
    :param app_arg: add the positional ``app`` argument.
    """
    if app_arg:
        parser.add_argument(
            "app",
            nargs="?",
            help="SynTasks app as 'module:attribute' (default: [app] app in config).",
        )
    parser.add_argument("-c", "--config", help="Config file (default: syntasks.toml).")
    parser.add_argument("--profile", help="Config profile to apply.")


def config_from_args(
    args: argparse.Namespace,
    overrides: dict[str, Any] | None = None,
) -> Config:
    """
    Load the config named by the CLI arguments.

    :param args: parsed arguments.
    :param overrides: dotted settings from CLI flags.
    :return: config.
    """
    return load_config(args.config, profile=args.profile, overrides=overrides)


def app_from_args(args: argparse.Namespace) -> "SynTasks":
    """
    The app for management commands.

    Imports the user's app when one is named (in the arguments or
    ``[app] app``), otherwise builds a bare app from the config, which is
    enough to inspect queues, schedules, channels and nodes.

    :param args: parsed arguments.
    :return: app.
    """
    from syntasks.app import SynTasks  # noqa: PLC0415
    from syntasks.worker.supervisor import load_app  # noqa: PLC0415

    config = config_from_args(args)
    if getattr(args, "app", None) or config.app.app:
        return load_app(config, getattr(args, "app", None))
    return SynTasks(config)


def run_with_app(
    args: argparse.Namespace,
    action: Callable[["SynTasks"], Awaitable[_T]],
) -> _T:
    """
    Run an async action with the app, closing connections afterwards.

    :param args: parsed arguments.
    :param action: coroutine function taking the app.
    :return: the action's result.
    """
    app = app_from_args(args)

    async def main() -> _T:
        try:
            return await action(app)
        finally:
            await app.redis.close()

    return asyncio.run(main())


def _plain(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {k: _plain(v) for k, v in dataclasses.asdict(value).items()}
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_plain(v) for v in value]
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    return value


def print_json(value: Any) -> None:
    """
    Print a value as JSON.

    :param value: dataclass, list or plain data.
    """
    print(json.dumps(_plain(value), indent=2, default=str))


def print_table(rows: Sequence[Sequence[Any]], headers: Sequence[str]) -> None:
    """
    Print an aligned text table.

    :param rows: rows.
    :param headers: column names.
    """
    cells = [[str(h) for h in headers]] + [
        ["" if c is None else str(c) for c in row] for row in rows
    ]
    widths = [max(len(row[i]) for row in cells) for i in range(len(headers))]
    for index, row in enumerate(cells):
        print(
            "  ".join(c.ljust(w) for c, w in zip(row, widths, strict=True)).rstrip(),
        )
        if index == 0:
            print("  ".join("-" * w for w in widths))
    if len(cells) == 1:
        print("(none)")


def fail(message: str) -> int:
    """
    Print an error and return the failure status.

    :param message: error text.
    :return: 1.
    """
    print(f"error: {message}", file=sys.stderr)
    return 1


def guarded(run: Callable[[], int | None]) -> int | None:
    """
    Turn config errors into a clean message instead of a traceback.

    :param run: command body.
    :return: status.
    """
    try:
        return run()
    except ConfigError as exc:
        return fail(str(exc))


def ms_to_text(ms: float | None) -> str:
    """
    Unix milliseconds as local ISO time.

    :param ms: milliseconds or None.
    :return: text.
    """
    if ms is None:
        return "-"
    return dt.datetime.fromtimestamp(ms / 1000).isoformat(timespec="seconds")
