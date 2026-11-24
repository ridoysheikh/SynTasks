import shutil
import subprocess
from collections.abc import Generator

import pytest

from syntasks.redis import (
    ConnectionOptions,
    ConnectionPool,
    MasterNotFoundError,
    Redis,
    SentinelConnectionPool,
)
from tests.conftest import RedisServer, _free_port, _wait_for_port


@pytest.fixture(scope="module")
def sentinel(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[tuple[int, int], None, None]:
    """Start a master and a Sentinel watching it; yield both ports."""
    binary = shutil.which("redis-server")
    if binary is None:
        pytest.skip("redis-server is not installed")
    workdir = tmp_path_factory.mktemp("sentinel")
    master = RedisServer(workdir / "master")
    port = _free_port()
    config = workdir / "sentinel.conf"
    config.write_text(
        f"port {port}\n"
        "bind 127.0.0.1\n"
        f"dir {workdir}\n"
        f"sentinel monitor mymaster 127.0.0.1 {master.port} 1\n",
    )
    process = subprocess.Popen(  # noqa: S603
        [binary, str(config), "--sentinel"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(port)
        yield master.port, port
    finally:
        process.terminate()
        process.wait(timeout=10)
        master.stop()


async def test_discovers_master(sentinel: tuple[int, int]) -> None:
    master_port, sentinel_port = sentinel
    pool = SentinelConnectionPool(
        "mymaster",
        [("127.0.0.1", 1), ("127.0.0.1", sentinel_port)],
        ConnectionOptions(db=1, connect_timeout=1),
        sentinel_options=ConnectionOptions(connect_timeout=1),
    )
    assert await pool.discover_master() == ("127.0.0.1", master_port)
    # The Sentinel that answered moves to the front.
    assert pool.sentinels[0] == ("127.0.0.1", sentinel_port)

    client = Redis(pool)
    direct = Redis(ConnectionPool(ConnectionOptions(port=master_port, db=1)))
    try:
        await client.set("via-sentinel", "1")
        assert await direct.get("via-sentinel") == b"1"
    finally:
        await direct.flushdb()
        await direct.close()
        await client.close()


async def test_unknown_master(sentinel: tuple[int, int]) -> None:
    _, sentinel_port = sentinel
    pool = SentinelConnectionPool("nope", [("127.0.0.1", sentinel_port)])
    with pytest.raises(MasterNotFoundError):
        await pool.discover_master()
