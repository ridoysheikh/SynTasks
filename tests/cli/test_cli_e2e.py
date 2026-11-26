"""Run the real ``syntasks`` CLI against a throwaway Redis."""

import asyncio
import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from importlib.metadata import entry_points
from pathlib import Path

import pytest

from syntasks import SynTasks
from syntasks.config import load_config

APP = """
from syntasks import SynTasks, every

app = SynTasks(timeout="1m", retries=0, max_tasks=None)

@app.task(timeout="5s")
async def add(a: int, b: int) -> int:
    return a + b

@app.schedule(every("200ms"), id="tick")
async def tick() -> None:
    pass

@app.schedule(runtime=True)
async def remind(note: str) -> None:
    pass
"""


def _project(tmp_path: Path, redis_url: str) -> Path:
    (tmp_path / "e2e_app.py").write_text(APP)
    (tmp_path / "syntasks.toml").write_text(
        textwrap.dedent(
            f"""
            [app]
            name = "e2e"
            app = "e2e_app:app"
            revision = 1

            [redis]
            url = "{redis_url}"

            [worker]
            processes = 2
            xread_block = "200ms"
            shutdown_grace = "5s"

            [scheduler]
            max_sleep = "100ms"
            min_interval = "100ms"

            """,
        ),
    )
    return tmp_path


def test_run_is_the_command_that_starts_a_machine() -> None:
    """One command runs everything on a machine; there is no `worker` command."""
    commands = {entry.name for entry in entry_points().select(group="syntasks_cli")}
    assert "run" in commands
    assert "worker" not in commands


def _cli(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [sys.executable, "-m", "syntasks", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


async def test_run_and_management_commands(  # noqa: PLR0915
    tmp_path: Path,
    redis_url: str,
) -> None:
    project = _project(tmp_path, redis_url)
    show = _cli(project, "config", "show", "--sources")
    assert show.returncode == 0, show.stderr
    assert "processes = 2  # file:syntasks.toml" in show.stdout
    check = _cli(project, "config", "check")
    assert check.returncode == 0, check.stdout + check.stderr
    assert "app: e2e (3 tasks)" in check.stdout

    env = {**os.environ, "PYTHONPATH": str(project)}
    worker = subprocess.Popen(
        [sys.executable, "-m", "syntasks", "run"],
        cwd=project,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    sys.path.insert(0, str(project))
    try:
        app = SynTasks(load_config(project / "syntasks.toml"))
        deadline = time.monotonic() + 30
        while len([n for n in await app.nodes() if n.alive]) < 2:
            assert worker.poll() is None, worker.stdout.read() if worker.stdout else ""
            assert time.monotonic() < deadline, "workers did not start"
            await asyncio.sleep(0.2)
        task = await app.send("e2e_app:add", 2, 3)
        result = await task.wait_result(timeout=20)
        assert result.return_value == 5
        deadline = time.monotonic() + 20
        while (await app.schedules.get("tick")).runs < 3:
            assert time.monotonic() < deadline
            await asyncio.sleep(0.2)
        nodes = _cli(project, "nodes")
        assert nodes.returncode == 0, nodes.stderr
        assert nodes.stdout.count("alive") == 2
        queues = _cli(project, "queues", "list")
        assert "default" in queues.stdout
        schedules = _cli(project, "schedules", "list")
        assert "tick" in schedules.stdout and "every 200ms" in schedules.stdout
        assert "late" in schedules.stdout.splitlines()[0]
        preview = _cli(project, "schedules", "preview", "tick", "-n", "3")
        assert preview.returncode == 0, preview.stderr
        assert len(preview.stdout.strip().splitlines()) == 3
        runs = _cli(project, "schedules", "runs", "tick")
        assert "late" in runs.stdout.splitlines()[0]
        paused = _cli(project, "schedules", "pause", "tick")
        assert paused.returncode == 0, paused.stderr
        assert (await app.schedules.get("tick")).state == "paused"

        async def remind(note: str) -> None: ...

        app.schedule(
            runtime=True,
            task_name="e2e_app:remind",
            timeout="5s",
            retries=0,
            max_tasks=None,
        )(remind)
        for owner, note in (("user:1", "a"), ("user:1", "b"), ("user:2", "c")):
            await app.schedules.create(
                "e2e_app:remind",
                after="1h",
                kwargs={"note": note},
                owner=owner,
                tags=["camp"],
            )
        mine = _cli(project, "schedules", "list", "--owner", "user:1", "--json")
        assert mine.returncode == 0, mine.stderr
        assert (
            sorted(item["owner"] for item in json.loads(mine.stdout)) == ["user:1"] * 2
        )
        cancelled = _cli(project, "schedules", "cancel", "--owner", "user:1")
        assert "cancelled 2" in cancelled.stdout, cancelled.stdout + cancelled.stderr
        left = _cli(project, "schedules", "list", "--tag", "camp", "--json")
        assert [item["owner"] for item in json.loads(left.stdout)] == ["user:2"]
        await app.redis.close()
    finally:
        sys.path.remove(str(project))
        worker.send_signal(signal.SIGTERM)
        try:
            output, _ = worker.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            worker.kill()
            output, _ = worker.communicate()
    assert worker.returncode == 0, output
    assert "Starting 2 worker process(es)." in output


@pytest.mark.parametrize("args", [["config", "show", "--config", "missing.toml"]])
def test_config_errors_are_clean(tmp_path: Path, args: list[str]) -> None:
    result = _cli(tmp_path, *args)
    assert result.returncode == 1
    assert "error: Config file" in result.stderr
    assert "Traceback" not in result.stderr


BROKEN_APP = """
from syntasks import SynTasks, SynTasksEvents

app = SynTasks(timeout=None, retries=0, max_tasks=None)

@app.on_event(SynTasksEvents.WORKER_STARTUP)
async def explode(state) -> None:
    raise RuntimeError("database is unreachable")
"""


def test_supervisor_stops_after_failed_startups(
    tmp_path: Path, redis_server: str
) -> None:
    (tmp_path / "broken_app.py").write_text(BROKEN_APP)
    (tmp_path / "syntasks.toml").write_text(
        textwrap.dedent(
            f"""
            [app]
            name = "broken"
            app = "broken_app:app"

            [redis]
            url = "{redis_server}"

            [worker]
            processes = 1
            max_startup_failures = 2
            """,
        ),
    )
    started = time.monotonic()
    result = _cli(tmp_path, "run")
    assert result.returncode != 0
    output = result.stdout + result.stderr
    assert "database is unreachable" in output
    assert "failed to start 2 times in a row" in output
    assert time.monotonic() - started < 30
