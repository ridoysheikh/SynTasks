"""
Process isolation: run one task call in a child process.

The only way to hard-kill a run (``SIGTERM`` at the timeout, ``SIGKILL``
after the grace period) and to cap one run's memory (``RLIMIT_AS`` in the
child plus RSS checks from the parent). Arguments, return values and
exceptions cross the process boundary with pickle, and the task function
must be importable (a module-level function).
"""

import asyncio
import contextlib
import importlib
import inspect
import multiprocessing
import os
import pickle
import signal
import sys
import time
import traceback
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

from syntasks.exceptions import TaskMemoryError, TaskTimeoutError

__all__ = ["IsolationError", "process_rss", "run_isolated"]

_MEMORY_POLL = 0.2


class IsolationError(RuntimeError):
    """The child process failed without a regular exception."""


def process_rss(pid: int | None = None) -> int | None:
    """
    Resident memory of a process in bytes (Linux; None elsewhere).

    :param pid: process id (default: this process).
    :return: bytes or None.
    """
    path = f"/proc/{pid or 'self'}/statm"
    try:
        with open(path) as handle:  # noqa: PTH123
            pages = int(handle.read().split()[1])
    except (OSError, ValueError, IndexError):
        if pid is None:
            try:
                import resource  # noqa: PLC0415
            except ImportError:  # pragma: no cover
                return None
            peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return peak if sys.platform == "darwin" else peak * 1024
        return None
    return pages * os.sysconf("SC_PAGE_SIZE")


def set_memory_limit(limit: int) -> bool:
    """
    Cap this process's address space (where supported).

    :param limit: bytes.
    :return: True if the limit was set.
    """
    try:
        import resource  # noqa: PLC0415

        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
    except (ImportError, ValueError, OSError, AttributeError):
        return False
    return True


def _resolve(module_name: str, name: str) -> Any:
    """Find a task function in a freshly imported module."""
    module = importlib.import_module(module_name)
    found = getattr(module, name, None)
    if found is None:
        base = name.removesuffix("__syntasks_original")
        found = getattr(module, base)
        found = getattr(found, "original_func", found)
    return found


def _import_root(module_name: str) -> str | None:
    """
    The ``sys.path`` entry a module was imported from.

    Workers import the app with the project folder on ``sys.path`` only
    while loading it, so a child process must be told where to look.

    :param module_name: dotted module name.
    :return: directory, or None if unknown.
    """
    top = sys.modules.get(module_name.split(".", 1)[0])
    path = getattr(top, "__file__", None)
    if not path:
        return None
    root = Path(path).resolve().parent
    if Path(path).name == "__init__.py":
        root = root.parent
    return str(root)


def _child_main(
    conn: Connection,
    target: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    memory_limit: int | None,
    import_root: str | None = None,
) -> None:  # pragma: no cover (runs in the child)
    if memory_limit:
        set_memory_limit(memory_limit)
    if import_root and import_root not in sys.path:
        sys.path.insert(0, import_root)
    try:
        func = _resolve(*target) if isinstance(target, tuple) else target
        if inspect.iscoroutinefunction(func):
            with contextlib.suppress(Exception):
                asyncio.events._set_running_loop(None)  # noqa: SLF001
            value = asyncio.run(func(*args, **kwargs))
        else:
            value = func(*args, **kwargs)
            if inspect.isawaitable(value):
                value = asyncio.run(value)  # type: ignore[arg-type]
        payload: tuple[str, Any] = ("ok", value)
    except BaseException as exc:
        payload = ("err", exc)
    try:
        conn.send_bytes(pickle.dumps(payload))
    except Exception as exc:
        text = "".join(traceback.format_exception(exc))
        conn.send_bytes(pickle.dumps(("err", IsolationError(text))))
    finally:
        conn.close()


def _context() -> Any:
    methods = multiprocessing.get_all_start_methods()
    if "forkserver" in methods:
        return multiprocessing.get_context("forkserver")
    return multiprocessing.get_context("spawn")


async def run_isolated(  # noqa: C901, PLR0912
    func: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    task_name: str,
    timeout: float | None,
    grace: float,
    memory_limit: int | None,
) -> Any:
    """
    Run ``func(*args, **kwargs)`` in a fresh child process.

    :param func: importable function (sync or async).
    :param args: arguments (picklable).
    :param kwargs: keyword arguments (picklable).
    :param task_name: task name, for errors.
    :param timeout: hard time limit.
    :param grace: time between SIGTERM and SIGKILL.
    :param memory_limit: max memory of the child in bytes.
    :raises TaskTimeoutError: if the run took too long.
    :raises TaskMemoryError: if the child used too much memory.
    :raises IsolationError: if the child died without a result.
    :return: the function's return value.
    """
    ctx = _context()
    parent_conn, child_conn = ctx.Pipe(duplex=False)
    target = (func.__module__, func.__name__) if hasattr(func, "__module__") else func
    root = _import_root(func.__module__) if hasattr(func, "__module__") else None
    process = ctx.Process(
        target=_child_main,
        args=(child_conn, target, args, kwargs, memory_limit, root),
        daemon=True,
    )
    process.start()
    child_conn.close()
    loop = asyncio.get_running_loop()
    started = time.monotonic()
    try:
        while True:
            ready = await loop.run_in_executor(None, parent_conn.poll, _MEMORY_POLL)
            if ready:
                try:
                    status, value = pickle.loads(parent_conn.recv_bytes())  # noqa: S301
                except EOFError:
                    break
                if status == "ok":
                    return value
                if isinstance(value, MemoryError) and memory_limit is not None:
                    # The child hit its address-space limit (RLIMIT_AS).
                    raise TaskMemoryError(
                        task_name=task_name, limit=memory_limit
                    ) from value
                raise value
            if not process.is_alive():
                break
            if memory_limit is not None:
                rss = process_rss(process.pid)
                if rss is not None and rss > memory_limit:
                    _kill(process, signal.SIGKILL)
                    raise TaskMemoryError(task_name=task_name, limit=memory_limit)
            if timeout is not None and time.monotonic() - started > timeout:
                _kill(process, signal.SIGTERM)
                deadline = time.monotonic() + grace
                while process.is_alive() and time.monotonic() < deadline:
                    await asyncio.sleep(0.05)
                _kill(process, signal.SIGKILL)
                raise TaskTimeoutError(task_name=task_name, timeout=timeout)
        await loop.run_in_executor(None, process.join, 2)
        code = process.exitcode
        if code is not None and code < 0 and memory_limit is not None:
            raise TaskMemoryError(task_name=task_name, limit=memory_limit)
        raise IsolationError(f"Task {task_name} child process exited with code {code}.")
    finally:
        if process.is_alive():
            _kill(process, signal.SIGKILL)
        await loop.run_in_executor(None, process.join, 5)
        parent_conn.close()


def _kill(process: Any, sig: int) -> None:
    if process.pid is None or not process.is_alive():
        return
    with contextlib.suppress(ProcessLookupError, OSError):
        os.kill(process.pid, sig)
