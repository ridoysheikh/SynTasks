"""A module attached by dotted path in test_modules."""

from syntasks import Module

module = Module("by-path", timeout="5s", retries=0, max_tasks=None)


@module.task
async def from_path() -> str:
    return "path"
