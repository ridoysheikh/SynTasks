"""Management commands: queues, schedules, channels, nodes, tasks, logs."""

import argparse
import datetime as dt
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from syntasks.abc.cmd import SynTasksCMD
from syntasks.cli.common import (
    add_config_args,
    guarded,
    ms_to_text,
    print_json,
    print_table,
    run_with_app,
)

if TYPE_CHECKING:  # pragma: no cover
    from syntasks.app import SynTasks


def _parser(prog: str) -> tuple[argparse.ArgumentParser, Any]:
    parser = argparse.ArgumentParser(prog=prog)
    sub = parser.add_subparsers(dest="action", required=True)
    return parser, sub


def _action(
    sub: Any,
    name: str,
    help_text: str,
    *,
    target: str | None = None,
) -> argparse.ArgumentParser:
    parser: argparse.ArgumentParser = sub.add_parser(name, help=help_text)
    if target:
        parser.add_argument(target)
    add_config_args(parser, app_arg=False)
    parser.add_argument("--app", dest="app", help="SynTasks app as 'module:attribute'.")
    parser.add_argument("--json", action="store_true", help="Print JSON.")
    return parser


def _age(ms: int | None) -> str:
    if ms is None:
        return "-"
    seconds = max(time.time() - ms / 1000, 0)
    if seconds < 120:
        return f"{seconds:.0f}s"
    if seconds < 7200:
        return f"{seconds / 60:.0f}m"
    return f"{seconds / 3600:.1f}h"


class QueuesCMD(SynTasksCMD):
    """Inspect and operate queues."""

    short_help = "Inspect and operate queues"

    def exec(self, args: Sequence[str]) -> int | None:
        """
        Run a queue action.

        :param args: CLI arguments.
        :return: status.
        """
        parser, sub = _parser("syntasks queues")
        _action(sub, "list", "All queues with their lengths.")
        _action(sub, "info", "Lengths and state of a queue.", target="queue")
        peek = _action(sub, "peek", "Oldest waiting messages.", target="queue")
        peek.add_argument("-n", type=int, default=10)
        _action(sub, "pause", "Stop workers taking messages.", target="queue")
        _action(sub, "resume", "Let workers take messages again.", target="queue")
        purge = _action(
            sub,
            "purge",
            "Delete waiting and delayed messages.",
            target="queue",
        )
        purge.add_argument("--yes", action="store_true", help="Don't ask.")
        dlq = _action(sub, "dlq", "Dead letters of a queue.", target="queue")
        dlq.add_argument("-n", type=int, default=20)
        replay = _action(sub, "replay", "Send dead letters back.", target="queue")
        replay.add_argument("ids", nargs="*", help="Dead-letter ids (default: all).")
        _action(sub, "purge-dlq", "Delete all dead letters.", target="queue")
        parsed = parser.parse_args(args)

        async def act(app: "SynTasks") -> int:
            queues = app.queues
            action = parsed.action
            if action == "list":
                infos = await queues.list()
                if parsed.json:
                    print_json(infos)
                else:
                    print_table(
                        [
                            (
                                i.name,
                                i.waiting,
                                i.running,
                                i.delayed,
                                i.dead,
                                "yes" if i.paused else "",
                                _age(i.oldest_ms),
                            )
                            for i in infos
                        ],
                        [
                            "queue",
                            "waiting",
                            "running",
                            "delayed",
                            "dead",
                            "paused",
                            "oldest",
                        ],
                    )
            elif action == "info":
                print_json(await queues.info(parsed.queue))
            elif action == "peek":
                messages = await queues.peek(parsed.queue, parsed.n)
                if parsed.json:
                    print_json(messages)
                else:
                    print_table(
                        [
                            (m.id, m.task_name, m.task_id, _age(m.enqueued_ms))
                            for m in messages
                        ],
                        ["id", "task", "task id", "age"],
                    )
            elif action == "pause":
                await queues.pause(parsed.queue)
                print(f"paused {parsed.queue}")
            elif action == "resume":
                await queues.resume(parsed.queue)
                print(f"resumed {parsed.queue}")
            elif action == "purge":
                if (
                    not parsed.yes
                    and input(
                        f"Delete all waiting messages of {parsed.queue}? [y/N] ",
                    ).lower()
                    != "y"
                ):
                    return 1
                print(
                    f"deleted {await queues.purge(parsed.queue)} message(s)",
                )
            elif action == "dlq":
                letters = await queues.dead_letters(parsed.queue, parsed.n)
                if parsed.json:
                    print_json(letters)
                else:
                    print_table(
                        [
                            (
                                d.id,
                                d.task_name,
                                d.reason,
                                ms_to_text(d.at_ms),
                                d.error.splitlines()[-1][:80] if d.error else "",
                            )
                            for d in letters
                        ],
                        ["id", "task", "reason", "at", "error"],
                    )
            elif action == "replay":
                count = await queues.replay_dead(parsed.queue, parsed.ids or None)
                print(f"replayed {count} message(s)")
            elif action == "purge-dlq":
                print(
                    f"deleted {await queues.purge_dead(parsed.queue)} dead letter(s)",
                )
            return 0

        return guarded(lambda: run_with_app(parsed, act))


def _overdue(next_ms: int | None, now: int, state: str) -> str:
    """How far an active schedule is past its next fire time, or "-"."""
    if state != "active" or next_ms is None or next_ms >= now - 1000:
        return "-"
    return f"{(now - next_ms) / 1000:.0f}s"


class SchedulesCMD(SynTasksCMD):
    """Inspect and operate schedules."""

    short_help = "Inspect and operate schedules"

    def exec(self, args: Sequence[str]) -> int | None:
        """
        Run a schedule action.

        :param args: CLI arguments.
        :return: status.
        """
        parser, sub = _parser("syntasks schedules")
        listing = _action(sub, "list", "All schedules.")
        listing.add_argument("--task", help="Only schedules of this task.")
        listing.add_argument("--source", choices=["code", "api"])
        for name, text in (
            ("show", "Details of a schedule."),
            ("pause", "Pause a schedule."),
            ("resume", "Resume a schedule."),
            ("trigger", "Fire a schedule now."),
            ("delete", "Delete a dynamic schedule."),
        ):
            _action(sub, name, text, target="id")
        runs = _action(sub, "runs", "Recent runs of a schedule.", target="id")
        runs.add_argument("-n", type=int, default=20)
        preview = _action(sub, "preview", "Next fire times of a schedule.", target="id")
        preview.add_argument("-n", type=int, default=10)
        parsed = parser.parse_args(args)

        async def act(app: "SynTasks") -> int:
            schedules = app.schedules
            action = parsed.action
            if action == "list":
                items = [
                    s
                    async for s in schedules.list(
                        task=parsed.task,
                        source=parsed.source,
                    )
                ]
                if parsed.json:
                    print_json([s.to_dict() for s in items])
                else:
                    now = app.cluster.clock.now_ms()
                    print_table(
                        [
                            (
                                s.id,
                                s.task,
                                s.source,
                                s.state,
                                s.describe(),
                                ms_to_text(s.next_ms),
                                _overdue(s.next_ms, now, s.state),
                                s.runs,
                                s.last_status or "-",
                            )
                            for s in items
                        ],
                        [
                            "id",
                            "task",
                            "source",
                            "state",
                            "when",
                            "next",
                            "late",
                            "runs",
                            "last",
                        ],
                    )
                return 0
            if action == "preview":
                schedule = await schedules.get(parsed.id)
                times = schedule.spec.next_times(parsed.n, app.cluster.clock.now_ms())
                if parsed.json:
                    print_json(times)
                else:
                    for fire in times:
                        print(ms_to_text(fire))
                return 0
            if action == "runs":
                runs_list = await schedules.runs(parsed.id, limit=parsed.n)
                if parsed.json:
                    print_json(runs_list)
                else:
                    print_table(
                        [
                            (
                                ms_to_text(r.get("fire")),
                                r.get("task_id"),
                                r.get("status"),
                                r.get("duration"),
                                r.get("late_by", 0),
                            )
                            for r in runs_list
                        ],
                        ["fire", "task id", "status", "seconds", "late"],
                    )
                return 0
            if action == "show":
                schedule = await schedules.get(parsed.id)
                print_json(schedule.to_dict())
                return 0
            method = getattr(schedules, action)
            await method(parsed.id)
            print(f"{action}: {parsed.id}")
            return 0

        return guarded(lambda: run_with_app(parsed, act))


class ChannelsCMD(SynTasksCMD):
    """Inspect channels."""

    short_help = "Inspect and close channels"

    def exec(self, args: Sequence[str]) -> int | None:
        """
        Run a channel action.

        :param args: CLI arguments.
        :return: status.
        """
        parser, sub = _parser("syntasks channels")
        listing = _action(sub, "list", "Active channels.")
        listing.add_argument("--prefix", default="")
        _action(sub, "info", "Details of a channel.", target="name")
        _action(sub, "close", "Close a channel and stop its handler.", target="name")
        parsed = parser.parse_args(args)

        async def act(app: "SynTasks") -> int:
            if parsed.action == "list":
                infos = await app.channels.list(parsed.prefix)
                if parsed.json:
                    print_json(infos)
                else:
                    print_table(
                        [
                            (
                                c.name,
                                c.length,
                                c.inbox,
                                c.readers,
                                c.owner or "-",
                                _age(c.last_active_ms),
                            )
                            for c in infos
                        ],
                        [
                            "channel",
                            "messages",
                            "inbox",
                            "readers",
                            "handler on",
                            "last active",
                        ],
                    )
            elif parsed.action == "info":
                print_json(await app.channels.info(parsed.name))
            else:
                await app.channels.close(parsed.name)
                print(f"closed {parsed.name}")
            return 0

        return guarded(lambda: run_with_app(parsed, act))


class NodesCMD(SynTasksCMD):
    """List worker processes."""

    short_help = "List worker processes in the cluster"

    def exec(self, args: Sequence[str]) -> int | None:
        """
        Print the node registry.

        :param args: CLI arguments.
        :return: status.
        """
        parser = argparse.ArgumentParser(prog="syntasks nodes")
        add_config_args(parser, app_arg=False)
        parser.add_argument("--app", dest="app")
        parser.add_argument("--json", action="store_true")
        parsed = parser.parse_args(args)

        async def act(app: "SynTasks") -> int:
            nodes = await app.nodes()
            if parsed.json:
                print_json(nodes)
            else:
                print_table(
                    [
                        (
                            n.node_id,
                            n.host,
                            n.pid,
                            "alive" if n.alive else "dead",
                            f"{n.running}/{n.capacity}",
                            n.channels,
                            ",".join(n.queues),
                            ms_to_text(n.last_seen * 1000),
                        )
                        for n in nodes
                    ],
                    [
                        "node",
                        "host",
                        "pid",
                        "state",
                        "running",
                        "channels",
                        "queues",
                        "last seen",
                    ],
                )
            return 0

        return guarded(lambda: run_with_app(parsed, act))


class TasksCMD(SynTasksCMD):
    """Running tasks, cancellation and results."""

    short_help = "List running tasks, cancel them, read results"

    def exec(self, args: Sequence[str]) -> int | None:
        """
        Run a task action.

        :param args: CLI arguments.
        :return: status.
        """
        parser, sub = _parser("syntasks tasks")
        _action(sub, "running", "Tasks running now.")
        _action(sub, "cancel", "Cancel a running task.", target="task_id")
        _action(sub, "result", "Result of a task.", target="task_id")
        parsed = parser.parse_args(args)

        async def act(app: "SynTasks") -> int:
            if parsed.action == "running":
                running = await app.tasks.running()
                if parsed.json:
                    print_json(running)
                else:
                    print_table(
                        [(r.task_id, r.task_name, r.node_id) for r in running],
                        ["task id", "task", "node"],
                    )
            elif parsed.action == "cancel":
                await app.tasks.cancel(parsed.task_id)
                print(f"cancel requested: {parsed.task_id}")
            else:
                handle = app.tasks.handle(parsed.task_id)
                if not await handle.is_ready():
                    print("not ready")
                    return 1
                result = await handle.get_result()
                print_json(
                    {
                        "is_err": result.is_err,
                        "return_value": result.return_value,
                        "error": repr(result.error) if result.error else None,
                        "execution_time": result.execution_time,
                    },
                )
            return 0

        return guarded(lambda: run_with_app(parsed, act))


class LogsCMD(SynTasksCMD):
    """Captured logs of a task."""

    short_help = "Show captured logs of a task"

    def exec(self, args: Sequence[str]) -> int | None:
        """
        Print (or follow) a task's captured logs.

        :param args: CLI arguments.
        :return: status.
        """
        parser = argparse.ArgumentParser(prog="syntasks logs")
        parser.add_argument("task_id")
        parser.add_argument(
            "-f",
            "--follow",
            action="store_true",
            help="Wait for new lines.",
        )
        add_config_args(parser, app_arg=False)
        parser.add_argument("--app", dest="app")
        parsed = parser.parse_args(args)

        async def act(app: "SynTasks") -> int:
            async for record in app.logs.read(parsed.task_id, follow=parsed.follow):
                stamp = dt.datetime.fromtimestamp(record.time).isoformat(
                    timespec="milliseconds",
                )
                print(
                    f"{stamp} {record.level:<8} {record.logger}: {record.message}",
                    flush=True,
                )
                if record.exception:
                    print(record.exception, flush=True)
            return 0

        def run() -> int:
            try:
                return run_with_app(parsed, act)
            except KeyboardInterrupt:
                return 0

        return guarded(run)


__all__ = [
    "ChannelsCMD",
    "LogsCMD",
    "NodesCMD",
    "QueuesCMD",
    "SchedulesCMD",
    "TasksCMD",
]
