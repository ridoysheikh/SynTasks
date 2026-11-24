import asyncio
import os
import shutil
import socket
import subprocess
import time
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from unittest.mock import patch

import pytest

from syntasks.abc.broker import AsyncBroker
from syntasks.redis import Redis


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """
    Anyio backend.

    Backend for anyio pytest plugin.
    :return: backend name.
    """
    return "asyncio"


@pytest.fixture(autouse=True)
def reset_broker() -> Generator[None, None, None]:
    """
    Restore async broker.

    This fixtures sets some global
    broker variables to default state.
    """
    yield
    AsyncBroker.global_task_registry = {}
    AsyncBroker.is_worker_process = False
    AsyncBroker.is_scheduler_process = False


@pytest.fixture
def mock_sleep() -> Generator[None, None, None]:
    async def _fast_sleep(delay: float) -> None:
        await asyncio_sleep(delay / 10000)

    asyncio_sleep = asyncio.sleep
    with patch("asyncio.sleep", _fast_sleep):
        yield


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"Redis on port {port} did not start.")


class RedisServer:
    """A throwaway redis-server process used by the tests."""

    def __init__(self, workdir: Path, *extra: str) -> None:
        binary = shutil.which("redis-server")
        if binary is None:
            pytest.skip("redis-server is not installed")
        self.port = _free_port()
        self.workdir = workdir
        workdir.mkdir(parents=True, exist_ok=True)
        self.process = subprocess.Popen(  # noqa: S603
            [
                binary,
                "--port",
                str(self.port),
                "--bind",
                "127.0.0.1",
                "--save",
                "",
                "--appendonly",
                "no",
                "--dir",
                str(workdir),
                *extra,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _wait_for_port(self.port)

    @property
    def url(self) -> str:
        return f"redis://127.0.0.1:{self.port}/0"

    def stop(self) -> None:
        self.process.terminate()
        self.process.wait(timeout=10)


@pytest.fixture(scope="session")
def redis_server(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[str, None, None]:
    """URL of a Redis server dedicated to the tests."""
    external = os.environ.get("SYNTASKS_TEST_REDIS_URL")
    if external:
        yield external
        return
    server = RedisServer(tmp_path_factory.mktemp("redis"))
    try:
        yield server.url
    finally:
        server.stop()


@pytest.fixture
async def redis_url(redis_server: str) -> AsyncGenerator[str, None]:
    """Server URL; the database is flushed after each test."""
    yield redis_server
    client = Redis.from_url(redis_server)
    await client.flushdb()
    await client.close()


@pytest.fixture
async def redis(redis_url: str) -> AsyncGenerator[Redis, None]:
    client = Redis.from_url(redis_url)
    yield client
    await client.close()
