"""``syntasks config``: check and show the configuration."""

import argparse
from collections.abc import Sequence

from syntasks.abc.cmd import SynTasksCMD
from syntasks.cli.common import add_config_args, config_from_args, guarded
from syntasks.config import dump_config


class ConfigCMD(SynTasksCMD):
    """Check and show the configuration."""

    short_help = "Check or show the configuration"

    def exec(self, args: Sequence[str]) -> int | None:
        """
        Run ``check`` or ``show``.

        :param args: CLI arguments.
        :return: status code.
        """
        parser = argparse.ArgumentParser(prog="syntasks config")
        sub = parser.add_subparsers(dest="action", required=True)
        check = sub.add_parser(
            "check",
            help="Validate the config (and the app's tasks).",
        )
        add_config_args(check)
        show = sub.add_parser("show", help="Print the effective config as TOML.")
        add_config_args(show, app_arg=False)
        show.add_argument(
            "--sources",
            action="store_true",
            help="Show where values came from.",
        )
        parsed = parser.parse_args(args)
        if parsed.action == "show":

            def show_config() -> int:
                print(
                    dump_config(config_from_args(parsed), with_sources=parsed.sources),
                    end="",
                )
                return 0

            return guarded(show_config)
        return guarded(lambda: self._check(parsed))

    def _check(self, parsed: argparse.Namespace) -> int:
        config = config_from_args(parsed)
        warnings: list[str] = []
        print(f"config: {config.path or '(defaults, no file found)'}")
        if config.app.revision is None:
            warnings.append(
                "[app] revision is not set; during rolling deploys an old node "
                "can override newer schedules and cluster settings.",
            )
        if parsed.app or config.app.app or config.app.modules:
            from syntasks.worker.supervisor import load_app  # noqa: PLC0415

            app = load_app(config, parsed.app)
            names = set(app.get_all_tasks())
            for name in config.tasks:
                if name not in names:
                    warnings.append(
                        f"[tasks.{name!r}] doesn't match any registered task.",
                    )
                else:
                    app.options_for(name)
            for schedule_id, schedule in config.schedules.items():
                if schedule.task not in names:
                    warnings.append(
                        f"[schedules.{schedule_id}] runs unknown task "
                        f"{schedule.task!r}.",
                    )
            app.schedules.validate_declared()
            print(f"app: {config.app.name} ({len(names)} tasks)")
        for warning in warnings:
            print(f"warning: {warning}")
        print("ok")
        return 0
