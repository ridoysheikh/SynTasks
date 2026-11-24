"""
Run the whole SynTasks story on a small shop, one real command at a time.

    python walkthrough.py            # starts a throwaway redis-server if it can
    python walkthrough.py --pause    # wait for Enter after each step

It starts two `syntasks worker` supervisors (two "machines", two processes
each), runs every `python -m shop ...` command against them, then kills one
machine to show the other taking over. Every step prints the command it
runs, so you can repeat any of them by hand. Set SHOP_REDIS_URL to use your
own (disposable!) Redis instead.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
BOLD, DIM, CYAN, RESET = "\033[1m", "\033[2m", "\033[36m", "\033[0m"
PORT = 55460

STEPS: list[tuple[str, str, list[list[str]]]] = [
    (
        "Check the app",
        "Tasks, schedules and channels are declared in shop/ (three modules under one "
        "SynTasks app); syntasks.toml only says where Redis is and how workers run. "
        "`config check` imports the app and checks every declaration, including the "
        "required timeout / retries / max_tasks.",
        [["syntasks", "config", "check"]],
    ),
    (
        "Two machines are running",
        "Each `syntasks worker` supervises 2 processes; every process is a node with a "
        "heartbeat. Declared schedules were synced into Redis when they started.",
        [["syntasks", "nodes"], ["syntasks", "schedules", "list"]],
    ),
    (
        "Retries and callbacks",
        "A group of charges. Order A-1007 fails once (a flaky provider) and is retried "
        "after 200ms. Every success sends the audit callback exactly once.",
        [["shop", "orders", "8"]],
    ),
    (
        "Order per key",
        "Shipments of one customer run one at a time, in order (key + "
        "max_tasks_per_key=1); two customers run side by side, on any machine.",
        [["shop", "ship"]],
    ),
    (
        "Limits across the cluster",
        "max_tasks=3 and rate='10/s' hold across all four worker processes.",
        [["shop", "partner", "12"]],
    ),
    (
        "Request / reply",
        "task.call(...) sends at the highest priority and waits for the value, or "
        "raises the task's own exception.",
        [["shop", "call"]],
    ),
    (
        "Timeouts, isolation, memory",
        "A run over its 1s timeout is cancelled and stored as TaskTimeoutError. "
        "isolation='process' runs a call in its own process; over memory_limit it "
        "fails with TaskMemoryError instead of taking the worker down.",
        [["shop", "export"], ["shop", "resize", "50"], ["shop", "resize", "400"]],
    ),
    (
        "Live events and streamed output",
        "events=True publishes the task's life to a channel; Emit streams partial "
        "output (think AI tokens). The client replays from the start, so it can "
        "connect late.",
        [["shop", "stream"]],
    ),
    (
        "Fair share between tenants",
        "With fair=True, 100 emails of one tenant don't make three small shops wait "
        "behind them: keys are served round-robin.",
        [["shop", "fair"]],
    ),
    (
        "Debounce",
        "20 quick edits of one product: one reindex, with the last version. The other "
        "19 handles end with TaskSupersededError instead of hanging.",
        [["shop", "debounce"]],
    ),
    (
        "Calendar schedules and previews",
        "monthly_report fires 09:00 on the first Monday of every month in Asia/Dhaka. "
        "Missed fires run as soon as the cluster is back.",
        [
            [
                "syntasks",
                "schedules",
                "preview",
                "shop.marketing:monthly_report",
                "-n",
                "3",
            ]
        ],
    ),
    (
        "Schedules your users create (no table needed)",
        "publish_post is a runtime template: users schedule posts with an owner, tags "
        "and metadata; the app lists them by owner, edits them with optimistic "
        "locking, and the owner cap (3) is enforced in Redis.",
        [
            ["shop", "posts"],
            [
                "syntasks",
                "schedules",
                "list",
                "--template",
                "shop.marketing:publish_post",
            ],
        ],
    ),
    (
        "A campaign you can pause and cancel",
        "40 emails sent as one batch while paused, resumed for a moment, then "
        "cancelled; campaign_report runs once with the totals.",
        [["shop", "campaign"]],
    ),
    (
        "A live channel with authorization and presence",
        "One handler for sales:live in the whole cluster. authorize lets staff read and "
        "only Ada post; presence shows who is connected.",
        [["shop", "live"]],
    ),
    (
        "Feeds: watchlists, groups, runtime subscriptions",
        "quote is a feed: one function that asks the vendor for each watched symbol, "
        "run once for the whole cluster. Users subscribe groups of symbols while "
        "connected; the feed only works on what somebody watches, keeps the last "
        "value, and groups are saved per user.",
        [["shop", "watchlists"]],
    ),
]


def run(command: list[str], env: dict[str, str]) -> None:
    if command[0] == "shop":
        argv = [sys.executable, "-m", "shop", *command[1:]]
        shown = "python -m shop " + " ".join(command[1:])
    else:
        argv = [sys.executable, "-m", "syntasks", *command[1:]]
        shown = "syntasks " + " ".join(command[1:])
    print(f"{CYAN}$ {shown}{RESET}", flush=True)
    subprocess.run(argv, cwd=HERE, env=env, check=False, timeout=300)


def step(title: str, text: str, pause: bool) -> None:
    print(f"\n{BOLD}== {title}{RESET}\n{DIM}{text}{RESET}", flush=True)
    if pause:
        input(f"{DIM}(Enter to run){RESET}")


def free(port: int) -> bool:
    with socket.socket() as sock:
        return sock.connect_ex(("127.0.0.1", port)) != 0


def start_redis(workdir: Path) -> tuple[str, subprocess.Popen[bytes] | None]:
    url = os.environ.get("SHOP_REDIS_URL")
    if url:
        return url, None
    binary = shutil.which("redis-server")
    if binary is None:
        sys.exit(
            "No redis-server on PATH. Either install it, or run\n"
            "  docker compose up -d --wait\n"
            f"  SHOP_REDIS_URL=redis://127.0.0.1:{PORT}/0 python walkthrough.py",
        )
    if not free(PORT):
        sys.exit(
            f"Port {PORT} is busy; set SHOP_REDIS_URL to a disposable Redis instead."
        )
    process = subprocess.Popen(
        [
            binary,
            "--port",
            str(PORT),
            "--bind",
            "127.0.0.1",
            "--save",
            "",
            "--appendonly",
            "no",
            "--dir",
            str(workdir),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 10
    while free(PORT):
        if time.monotonic() > deadline:
            sys.exit("redis-server did not start.")
        time.sleep(0.05)
    return f"redis://127.0.0.1:{PORT}/0", process


def start_worker(name: str, env: dict[str, str], logs: Path) -> subprocess.Popen[bytes]:
    log = (logs / f"{name}.log").open("wb")
    return subprocess.Popen(
        [sys.executable, "-m", "syntasks", "worker"],
        cwd=HERE,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,  # its own process group: we can kill the "machine"
    )


def wait_for_nodes(env: dict[str, str], count: int) -> None:
    script = (
        "import asyncio, sys\n"
        "from shop.app import app\n"
        "async def main():\n"
        "    for _ in range(150):\n"
        "        if len([n for n in await app.nodes() if n.alive]) >= int(sys.argv[1]):\n"
        "            return\n"
        "        await asyncio.sleep(0.2)\n"
        "    sys.exit('workers did not start')\n"
        "asyncio.run(main())\n"
    )
    subprocess.run(
        [sys.executable, "-c", script, str(count)], cwd=HERE, env=env, check=True
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--pause", action="store_true", help="wait for Enter after each step"
    )
    args = parser.parse_args()

    workdir = Path(tempfile.mkdtemp(prefix="syntasks-shop-"))
    url, redis = start_redis(workdir)
    env = {**os.environ, "SHOP_REDIS_URL": url, "PYTHONPATH": str(HERE)}
    print(f"{BOLD}Redis:{RESET} {url}   {BOLD}worker logs:{RESET} {workdir}")
    workers: list[subprocess.Popen[bytes]] = []
    try:
        workers = [
            start_worker("machine-a", env, workdir),
            start_worker("machine-b", env, workdir),
        ]
        wait_for_nodes(env, 4)
        for title, text, commands in STEPS:
            step(title, text, args.pause)
            for command in commands:
                run(command, env)

        step(
            "A machine dies",
            "Machine A is killed with SIGKILL: no clean shutdown. Its nodes stop sending "
            "heartbeats and are marked dead after 4s. Whatever it held (the schedule "
            "dispatcher, the sales:live handler, messages it had taken) moves to machine "
            "B by lease or claim; the handler keeps its saved state, so n goes on from 1.",
            args.pause,
        )
        with contextlib.suppress(ProcessLookupError):
            os.killpg(workers[0].pid, signal.SIGKILL)
        print(f"{CYAN}$ kill -9 <machine A>{RESET}", flush=True)
        time.sleep(6)
        run(["syntasks", "nodes"], env)
        run(["shop", "live"], env)
        run(["syntasks", "schedules", "runs", "heartbeat", "-n", "5"], env)
        print(
            f"\n{BOLD}Done.{RESET} Worker logs are in {workdir} (audit callbacks, posts, the campaign summary)."
        )
    finally:
        for worker in workers:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(worker.pid, signal.SIGTERM)
        for worker in workers:
            with contextlib.suppress(subprocess.TimeoutExpired):
                worker.wait(timeout=15)
        if redis is not None:
            redis.terminate()
            redis.wait(timeout=10)


if __name__ == "__main__":
    main()
