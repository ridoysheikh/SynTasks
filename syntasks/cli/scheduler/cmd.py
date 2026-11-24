import asyncio
from collections.abc import Sequence

from syntasks.abc.cmd import SynTasksCMD
from syntasks.cli.scheduler.args import SchedulerArgs
from syntasks.cli.scheduler.run import run_scheduler


class SchedulerCMD(SynTasksCMD):
    """Command for syntasks scheduler."""

    short_help = "Run task scheduler"

    def exec(self, args: Sequence[str]) -> None:
        """
        Run task scheduler.

        This function starts scheduler function.

        It periodically loads schedule for tasks
        and executes them.

        :param args: CLI arguments.
        """
        parsed = SchedulerArgs.from_cli(args)
        asyncio.run(run_scheduler(parsed))
