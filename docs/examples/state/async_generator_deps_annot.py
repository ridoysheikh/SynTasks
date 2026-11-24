import asyncio
from collections.abc import AsyncGenerator
from typing import Annotated

from syntasks import SynTasksDepends


async def dependency() -> AsyncGenerator[str, None]:
    print("Startup")
    await asyncio.sleep(0.1)

    yield "value"

    await asyncio.sleep(0.1)
    print("Shutdown")


async def my_task(dep: Annotated[str, SynTasksDepends(dependency)]) -> None:
    print(dep.upper())
