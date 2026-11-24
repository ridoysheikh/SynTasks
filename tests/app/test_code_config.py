import re
from pathlib import Path

import pytest

from syntasks.config import ConfigError, load_config
from syntasks.config.cluster import publish_cluster_config

from .conftest import AppFactory


@pytest.mark.parametrize(
    ("section", "where"),
    [("tasks", "@app.task"), ("defaults", "SynTasks("), ("queues", "app.queue")],
)
def test_code_sections_rejected(tmp_path: Path, section: str, where: str) -> None:
    path = tmp_path / "syntasks.toml"
    path.write_text(f'[{section}."x"]\nvalue = 1\n')
    with pytest.raises(
        ConfigError, match=f"isn.t a config section.*{re.escape(where)}"
    ):
        load_config(path, use_env=False)


async def test_manifest_stale_revision_adopts_limits(app_factory: AppFactory) -> None:
    def revision(number: int) -> object:
        def configure(config: object) -> None:
            config.app.revision = number  # type: ignore[attr-defined]

        return configure

    new = app_factory(revision(2))
    old = app_factory(revision(1))
    for app, limit in ((new, 5), (old, 2)):

        @app.task("shared:job", max_tasks=limit)
        async def job() -> None: ...

    assert await publish_cluster_config(new) is False
    assert await publish_cluster_config(old) is True
    assert old.options_for("shared:job").max_tasks == 5
