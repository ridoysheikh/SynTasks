"""Module-level functions for process isolation tests."""

import time


def add(a: int, b: int) -> int:
    return a + b


def hog_memory(megabytes: int) -> int:
    block = bytearray(megabytes * 1024 * 1024)
    time.sleep(2)
    return len(block)


def sleep_forever() -> None:
    time.sleep(60)


async def async_double(x: int) -> int:
    return x * 2
