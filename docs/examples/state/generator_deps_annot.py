from collections.abc import Generator
from typing import Annotated

from syntasks import SynTasksDepends


def dependency() -> Generator[str, None, None]:
    print("Startup")

    yield "value"

    print("Shutdown")


async def my_task(dep: Annotated[str, SynTasksDepends(dependency)]) -> None:
    print(dep.upper())
