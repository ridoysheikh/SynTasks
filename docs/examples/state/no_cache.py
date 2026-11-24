import random

from syntasks import SynTasksDepends


def common_dep() -> int:
    return random.randint(1, 10)


def dep1(cd: int = SynTasksDepends(common_dep)) -> int:
    return cd + 1


def dep2(cd: int = SynTasksDepends(common_dep, use_cache=False)) -> int:
    return cd + 2


def my_task(
    d1: int = SynTasksDepends(dep1),
    d2: int = SynTasksDepends(dep2),
) -> int:
    return d1 + d2
