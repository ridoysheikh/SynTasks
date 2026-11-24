"""
``syntasks worker``: start and supervise worker processes.

The supervisor (the process you start) only reads the config, starts
``worker.processes`` children and restarts them when they exit:

* exit code 0 (``max_tasks_per_process`` reached) or 75 (memory soft
  limit, a stuck task, a blocked loop): restarted at once;
* any other exit: restarted with a short backoff, counted towards
  ``worker.max_restarts``.

SIGINT/SIGTERM stop everything gracefully (running tasks get
``worker.shutdown_grace``); SIGHUP restarts all children (e.g. after a
deploy); ``--reload`` restarts them when files change.
"""

import asyncio
import contextlib
import json
import logging
import multiprocessing
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from syntasks.config import Config, ConfigError, load_config
from syntasks.config.loader import apply_overrides

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks

__all__ = ["Supervisor", "WorkerSpec", "child_main", "configure_logging", "load_app"]

logger = logging.getLogger("syntasks.supervisor")

_RESTART_BACKOFF = 1.0
_FORCE_AFTER_SIGNALS = 3


@dataclass(kw_only=True)
class WorkerSpec:
    """Everything a worker process needs to start (picklable)."""

    app: str | None = None
    config_path: str | None = None
    profile: str | None = None
    overrides: dict[str, Any] = field(default_factory=dict)
    reload: bool = False
    reload_dirs: list[str] = field(default_factory=list)
    use_gitignore: bool = True


class _JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data = {
            "time": round(record.created, 3),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "process": record.processName,
        }
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        return json.dumps(data)


def configure_logging(config: Config) -> None:
    """
    Set up Python logging from ``[logging]``.

    :param config: config.
    """
    if not config.logging.configure:
        return
    handler = logging.StreamHandler()
    if config.logging.format == "json":
        handler.setFormatter(_JSONFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s][%(processName)s][%(name)s][%(levelname)s] %(message)s",
            ),
        )
    root = logging.getLogger()
    for existing in list(root.handlers):
        if isinstance(existing, logging.StreamHandler) and not isinstance(
            existing,
            logging.FileHandler,
        ):
            root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(config.logging.level.upper())


def _load_spec_config(spec: WorkerSpec) -> Config:
    if spec.config_path:
        os.environ["SYNTASKS_CONFIG"] = str(Path(spec.config_path).resolve())
    if spec.profile:
        os.environ["SYNTASKS_PROFILE"] = spec.profile
    return load_config(spec.config_path, profile=spec.profile, overrides=spec.overrides)


def load_app(config: Config, app_spec: str | None) -> "SynTasks":
    """
    Import the user's app object and task modules.

    :param config: config.
    :param app_spec: ``module:attribute`` of the app (overrides ``[app] app``).
    :raises ConfigError: if no single app can be found.
    :return: the app.
    """
    from syntasks.app import SynTasks  # noqa: PLC0415
    from syntasks.cli.utils import import_from_modules, import_object  # noqa: PLC0415

    target = app_spec or config.app.app
    app: Any
    if target:
        app = import_object(target)
        if callable(app) and not isinstance(app, SynTasks):
            app = app()
        if not isinstance(app, SynTasks):
            raise ConfigError(detail=f"{target} is not a SynTasks app.")
    else:
        import_from_modules(list(config.app.modules))
        apps = list(SynTasks._instances)  # noqa: SLF001
        if len(apps) != 1:
            raise ConfigError(
                detail="Cannot tell which SynTasks app to run "
                f"({len(apps)} found). Set [app] app = 'module:attribute' "
                "or pass it: syntasks worker module:attribute.",
            )
        app = apps[0]
    import_from_modules(list(config.app.modules))
    return app


def child_main(spec: WorkerSpec, index: int, processes: int, group: str) -> None:
    """
    Entry point of one worker process.

    :param spec: worker spec.
    :param index: process index.
    :param processes: number of worker processes.
    :param group: id shared by the processes of one supervisor.
    """
    from syntasks.worker.runtime import WorkerRuntime  # noqa: PLC0415

    config = _load_spec_config(spec)
    configure_logging(config)
    app = load_app(config, spec.app)
    apply_overrides(app.config, spec.overrides)
    app.reload_options()
    runtime = WorkerRuntime(
        app,
        processes=processes,
        process_index=index,
        group=group,
        own_process=True,
    )

    async def main() -> None:
        stop = asyncio.Event()
        signals = 0

        def on_signal() -> None:
            nonlocal signals
            signals += 1
            stop.set()
            if signals >= _FORCE_AFTER_SIGNALS:
                logger.warning("Forced exit.")
                os._exit(1)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(sig, on_signal)
        await runtime.run(stop)

    loop_kind = app.config.worker.loop
    if loop_kind in ("auto", "uvloop"):
        try:
            import uvloop  # noqa: PLC0415

            uvloop.run(main())
        except ImportError:
            if loop_kind == "uvloop":
                raise ConfigError(
                    detail="worker.loop = 'uvloop' needs 'syntasks[uv]'.",
                ) from None
            asyncio.run(main())
    else:
        asyncio.run(main())
    sys.exit(runtime.exit_code)


class Supervisor:
    """Starts, watches and restarts worker processes."""

    def __init__(self, spec: WorkerSpec) -> None:
        self.spec = spec
        self.config = _load_spec_config(spec)
        processes = self.config.worker.processes
        self.processes = processes if processes > 0 else (os.cpu_count() or 1)
        self.group = (
            f"{os.uname().nodename if hasattr(os, 'uname') else 'host'}-{os.getpid()}"
        )
        self.children: list[multiprocessing.process.BaseProcess | None] = [
            None,
        ] * self.processes
        self.restart_at: list[float] = [0.0] * self.processes
        self.crashes = 0
        self.stopping = threading.Event()
        self.reload_requested = threading.Event()
        self._signals = 0

    def _start(self, index: int) -> None:
        process = multiprocessing.Process(
            target=child_main,
            args=(self.spec, index, self.processes, self.group),
            name=f"worker-{index}",
            daemon=False,
        )
        process.start()
        self.children[index] = process
        logger.info("Started worker-%d (pid %s).", index, process.pid)

    def _signal(self, signum: int, _frame: Any) -> None:
        self._signals += 1
        if signum == getattr(signal, "SIGHUP", None):
            logger.info("SIGHUP: restarting workers.")
            self.reload_requested.set()
            return
        logger.info("Stopping workers (signal %s).", signum)
        self.stopping.set()
        if self._signals >= _FORCE_AFTER_SIGNALS:
            self._kill_all(signal.SIGKILL)

    def _kill_all(self, sig: int) -> None:
        for child in self.children:
            if child is not None and child.is_alive() and child.pid:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(child.pid, sig)

    def _stop_children(self) -> None:
        self._kill_all(signal.SIGTERM)
        deadline = time.monotonic() + self.config.worker.shutdown_grace + 10
        for child in self.children:
            if child is None:
                continue
            child.join(max(deadline - time.monotonic(), 0.1))
        self._kill_all(signal.SIGKILL)
        for child in self.children:
            if child is not None:
                child.join(1)

    def _watch_files(self) -> Any:
        if not self.spec.reload:
            return None
        try:
            from watchdog.observers import Observer  # noqa: PLC0415

            from syntasks.cli.watcher import FileWatcher  # noqa: PLC0415
        except ImportError as exc:
            raise ConfigError(detail="--reload needs 'syntasks[reload]'.") from exc
        observer = Observer()
        for path in self.spec.reload_dirs or ["."]:
            observer.schedule(  # type: ignore[call-arg]
                FileWatcher(  # type: ignore[arg-type]
                    callback=self.reload_requested.set,
                    path=Path(path),
                    use_gitignore=self.spec.use_gitignore,
                ),
                path=path,
                recursive=True,
            )
        observer.start()
        return observer

    def run(self) -> int:  # noqa: C901
        """
        Supervise until stopped.

        :return: exit status.
        """
        for sig in (signal.SIGINT, signal.SIGTERM, getattr(signal, "SIGHUP", None)):
            if sig is not None:
                signal.signal(sig, self._signal)
        observer = self._watch_files()
        logger.info("Starting %d worker process(es).", self.processes)
        for index in range(self.processes):
            self._start(index)
        status = 0
        try:
            while not self.stopping.wait(0.5):
                if self.reload_requested.is_set():
                    self.reload_requested.clear()
                    self._stop_children()
                    for index in range(self.processes):
                        self._start(index)
                    continue
                for index, child in enumerate(self.children):
                    if child is None or child.is_alive():
                        continue
                    status = self._handle_exit(index, child.exitcode)
                    if status:
                        self.stopping.set()
                        break
        finally:
            self._stop_children()
            if observer is not None:
                observer.stop()
        return status

    def _handle_exit(self, index: int, code: int | None) -> int:
        from syntasks.worker.runtime import EXIT_RECYCLE  # noqa: PLC0415

        now = time.monotonic()
        if code in (0, EXIT_RECYCLE):
            logger.info(
                "worker-%d exited (code %s); starting a fresh one.",
                index,
                code,
            )
            self._start(index)
            return 0
        if self.restart_at[index] == 0.0:
            self.crashes += 1
            limit = self.config.worker.max_restarts
            logger.error("worker-%d crashed (code %s).", index, code)
            if limit and self.crashes >= limit:
                logger.error("worker.max_restarts (%d) reached; stopping.", limit)
                return 1
            self.restart_at[index] = now + _RESTART_BACKOFF
        if now >= self.restart_at[index]:
            self.restart_at[index] = 0.0
            self._start(index)
        return 0
