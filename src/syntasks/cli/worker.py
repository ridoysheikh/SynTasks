"""``syntasks worker``: run worker processes."""

import argparse
from collections.abc import Sequence
from typing import Any

from syntasks.abc.cmd import SynTasksCMD
from syntasks.cli.common import add_config_args, guarded
from syntasks.worker.supervisor import Supervisor, WorkerSpec, configure_logging


class WorkerCMD(SynTasksCMD):
    """Run workers."""

    short_help = "Run worker processes"

    def exec(self, args: Sequence[str]) -> int | None:
        """
        Start the supervisor and its worker processes.

        :param args: CLI arguments.
        :return: status code.
        """
        parser = argparse.ArgumentParser(
            prog="syntasks worker",
            description="Run worker processes. Settings come from syntasks.toml; "
            "flags override them.",
        )
        add_config_args(parser)
        parser.add_argument(
            "-p",
            "--processes",
            type=int,
            help="Worker processes (0 = CPUs).",
        )
        parser.add_argument("-q", "--queues", help="Comma-separated queues to serve.")
        parser.add_argument(
            "--max-async-tasks",
            type=int,
            help="Tasks at once on this machine.",
        )
        parser.add_argument("--log-level", help="Log level, e.g. INFO or DEBUG.")
        parser.add_argument(
            "--reload",
            action="store_true",
            help="Restart on file changes.",
        )
        parser.add_argument(
            "--reload-dir",
            action="append",
            default=[],
            help="Directory to watch with --reload (repeatable).",
        )
        parser.add_argument(
            "--no-gitignore",
            action="store_true",
            help="Also reload on changes to git-ignored files.",
        )
        parsed = parser.parse_args(args)
        overrides: dict[str, Any] = {}
        if parsed.processes is not None:
            overrides["worker.processes"] = parsed.processes
        if parsed.queues:
            overrides["worker.queues"] = [
                q.strip() for q in parsed.queues.split(",") if q.strip()
            ]
        if parsed.max_async_tasks is not None:
            overrides["worker.max_async_tasks"] = parsed.max_async_tasks
        if parsed.log_level:
            overrides["logging.level"] = parsed.log_level.upper()
        if parsed.reload:
            overrides["worker.processes"] = 1
        spec = WorkerSpec(
            app=parsed.app,
            config_path=parsed.config,
            profile=parsed.profile,
            overrides=overrides,
            reload=parsed.reload,
            reload_dirs=parsed.reload_dir,
            use_gitignore=not parsed.no_gitignore,
        )

        def run() -> int:
            supervisor = Supervisor(spec)
            configure_logging(supervisor.config)
            return supervisor.run()

        return guarded(run)
