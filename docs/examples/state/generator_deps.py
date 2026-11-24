from collections.abc import Generator

from syntasks import SynTasksDepends


def dependency() -> Generator[str, None, None]:
    print("Startup")

    yield "value"

    print("Shutdown")


async def my_task(dep: str = SynTasksDepends(dependency)) -> None:
    print(dep.upper())
