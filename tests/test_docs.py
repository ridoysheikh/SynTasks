from pathlib import Path

from syntasks.config.dump import config_reference

ROOT = Path(__file__).parent.parent


def test_config_reference_is_current() -> None:
    page = ROOT / "docs" / "reference" / "config.md"
    assert page.read_text() == config_reference(), (
        "docs/reference/config.md is out of date; regenerate it with "
        "syntasks.config.dump.config_reference()."
    )
