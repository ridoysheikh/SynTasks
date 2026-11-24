from argparse import ArgumentParser
from collections.abc import Sequence

from syntasks.abc.cmd import SynTasksCMD


class MyCommand(SynTasksCMD):
    short_help = "Demo command"

    def exec(self, args: Sequence[str]) -> None:
        parser = ArgumentParser()
        parser.add_argument(
            "--test",
            dest="test",
            default="default",
            help="My test parameter.",
        )
        parsed = parser.parse_args(args)
        print(parsed)
