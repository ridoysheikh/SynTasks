import importlib
import logging
import sys
from pathlib import Path

import pytest

from syntasks import InMemoryBroker
from syntasks.worker.isolation import run_isolated


async def test_isolated_run_finds_modules_from_a_removed_path(tmp_path: Path) -> None:
    """Workers import the app with the project folder on sys.path only for a moment."""
    package = tmp_path / "isopkg"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "jobs.py").write_text("def triple(x):\n    return x * 3\n")
    sys.path.insert(0, str(tmp_path))
    try:
        module = importlib.import_module("isopkg.jobs")
    finally:
        sys.path.remove(str(tmp_path))
    value = await run_isolated(
        module.triple,
        (2,),
        {},
        task_name="isopkg.jobs:triple",
        timeout=20,
        grace=1,
        memory_limit=None,
    )
    assert value == 6


async def test_failed_result_has_no_conversion_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    broker = InMemoryBroker()

    @broker.task
    async def boom() -> int:
        raise ValueError("no")

    with caplog.at_level(logging.WARNING):
        result = await (await boom.kiq()).wait_result()
    assert result.is_err
    assert "Cannot parse return type" not in caplog.text
