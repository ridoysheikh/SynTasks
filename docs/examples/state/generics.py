from typing import Generic, TypeVar

from syntasks import SynTasksDepends

T = TypeVar("T")


def default_dependency() -> int:
    return 1


class GenericDep(Generic[T]):
    def __init__(self, dep: T = SynTasksDepends()) -> None:
        self.dep = dep

    def print_dep(self) -> None:
        print(self.dep)


class MyClassDep:
    def __init__(self, i: int = SynTasksDepends(default_dependency)) -> None:
        self.i = i


async def my_task(dep: GenericDep[MyClassDep] = SynTasksDepends()) -> None:
    assert isinstance(dep.dep, MyClassDep)
    dep.print_dep()
