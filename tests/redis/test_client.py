import asyncio

import pytest

from syntasks.redis import (
    ConnectionPool,
    Redis,
    RedisConnectionError,
    ResponseError,
)


async def test_strings(redis: Redis) -> None:
    assert await redis.ping()
    assert await redis.set("k", "v")
    assert await redis.get("k") == b"v"
    assert not await redis.set("k", "other", nx=True)
    assert await redis.set("k", "new", xx=True, ex=100)
    assert 0 < await redis.ttl("k") <= 100
    assert await redis.mget(["k", "missing"]) == [b"new", None]
    assert await redis.getdel("k") == b"new"
    assert await redis.exists("k") == 0
    assert await redis.incr("n", 5) == 5


async def test_binary_values(redis: Redis) -> None:
    payload = bytes(range(256)) * 1000
    await redis.set("bin", payload)
    assert await redis.get("bin") == payload


async def test_error_reply(redis: Redis) -> None:
    await redis.set("k", "v")
    with pytest.raises(ResponseError) as exc:
        await redis.lpush("k", "x")
    assert exc.value.code == "WRONGTYPE"
    # The connection is still usable after an error reply.
    assert await redis.get("k") == b"v"


async def test_lists(redis: Redis) -> None:
    await redis.lpush("q", "a", "b")
    assert await redis.llen("q") == 2
    assert await redis.brpop("q", 1) == (b"q", b"a")
    assert await redis.brpop(["other", "q"], 1) == (b"q", b"b")
    assert await redis.brpop("q", 0.1) is None


async def test_hashes(redis: Redis) -> None:
    assert await redis.hset("h", {"a": 1, "b": 2}) == 2
    assert await redis.hget("h", "a") == b"1"
    assert await redis.hgetall("h") == {b"a": b"1", b"b": b"2"}
    assert await redis.hlen("h") == 2
    items = [item async for item in redis.hscan_iter("h", count=1)]
    assert sorted(items) == [(b"a", b"1"), (b"b", b"2")]
    assert await redis.hdel("h", "a") == 1


async def test_sorted_sets(redis: Redis) -> None:
    await redis.zadd("z", {"a": 1, "b": 2, "c": 3})
    assert await redis.zrangebyscore("z", 2, "+inf") == [b"b", b"c"]
    assert await redis.zrangebyscore("z", "-inf", "+inf", offset=0, count=1) == [b"a"]
    assert await redis.zrem("z", "a") == 1
    assert await redis.zcard("z") == 2


async def test_scan(redis: Redis) -> None:
    for i in range(30):
        await redis.set(f"scan:{i}", i)
    keys = {key async for key in redis.scan_iter(match="scan:*", count=5)}
    assert len(keys) == 30


async def test_pipeline(redis: Redis) -> None:
    pipe = redis.pipeline()
    pipe.add("SET", "a", 1).add("INCR", "a").add("GET", "a")
    assert await pipe.execute() == [b"OK", 2, b"2"]


async def test_pipeline_errors_in_place(redis: Redis) -> None:
    await redis.set("s", "v")
    pipe = redis.pipeline().add("LPUSH", "s", "x").add("GET", "s")
    with pytest.raises(ResponseError):
        await pipe.execute()
    pipe = redis.pipeline().add("LPUSH", "s", "x").add("GET", "s")
    error, value = await pipe.execute(raise_on_error=False)
    assert isinstance(error, ResponseError)
    assert value == b"v"


async def test_transaction(redis: Redis) -> None:
    pipe = redis.pipeline(transaction=True)
    pipe.add("INCR", "t").add("INCR", "t")
    assert await pipe.execute() == [1, 2]


async def test_eval(redis: Redis) -> None:
    assert (
        await redis.eval("return ARGV[1] .. KEYS[1]", keys=["k"], args=["a"]) == b"ak"
    )


async def test_streams(redis: Redis) -> None:
    assert await redis.xgroup_create("s", "g", "0")
    assert not await redis.xgroup_create("s", "g", "0")
    first = await redis.xadd("s", {"data": b"1"})
    await redis.xadd("s", {"data": b"2"}, maxlen=100)
    assert await redis.xlen("s") == 2
    batches = await redis.xreadgroup("g", "c1", {"s": ">"}, count=1, block=100)
    assert batches == [(b"s", [(first, {b"data": b"1"})])]
    assert await redis.xpending_count("s", "g") == 1
    # Another consumer takes over the pending entry.
    _, claimed = await redis.xautoclaim("s", "g", "c2", 0)
    assert claimed == [(first, {b"data": b"1"})]
    assert await redis.xack("s", "g", first) == 1
    assert await redis.xreadgroup("g", "c1", {"s": ">"}, count=10, block=10)
    assert await redis.xreadgroup("g", "c1", {"s": ">"}, count=10, block=10) == []


async def test_pubsub(redis: Redis) -> None:
    async with redis.pubsub() as pubsub:
        await pubsub.subscribe("chan")
        await pubsub.psubscribe("pat.*")
        # Wait until the subscriptions are active.
        assert await pubsub.get_message(timeout=0.2) is None
        await redis.publish("chan", "hello")
        await redis.publish("pat.x", "world")
        first = await pubsub.get_message(timeout=2)
        second = await pubsub.get_message(timeout=2)
    assert first is not None
    assert (first.channel, first.data) == (b"chan", b"hello")
    assert second is not None
    assert (second.channel, second.data, second.pattern) == (
        b"pat.x",
        b"world",
        b"pat.*",
    )


async def test_pool_limits_connections(redis_url: str) -> None:
    client = Redis.from_url(redis_url, max_connections=2)
    try:
        results = await asyncio.gather(*(client.incr("c") for _ in range(50)))
        assert sorted(results) == list(range(1, 51))
        pool = client.pool
        assert len(pool._idle) <= 2
    finally:
        await client.close()


async def test_pool_replaces_broken_connections(redis_url: str) -> None:
    client = Redis.from_url(redis_url, max_connections=1)
    try:
        await client.ping()
        await client.execute("CLIENT", "KILL", "TYPE", "normal", "SKIPME", "no")
        # The killed connection is detected and replaced.
        for _ in range(3):
            try:
                assert await client.ping()
                break
            except RedisConnectionError:
                continue
        assert await client.ping()
    finally:
        await client.close()


async def test_cancelled_command_does_not_poison_pool(redis_url: str) -> None:
    client = Redis.from_url(redis_url, max_connections=1)
    try:
        task = asyncio.create_task(client.brpop("never", 5))
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await client.set("k", "v")
        assert await client.get("k") == b"v"
    finally:
        await client.close()


async def test_auth_and_select(redis_url: str) -> None:
    admin = Redis.from_url(redis_url)
    try:
        await admin.execute("ACL", "SETUSER", "tester", "on", ">pw", "~*", "+@all")
        client = Redis.from_url(
            redis_url.rsplit("/", 1)[0] + "/2",
            username="tester",
            password="pw",
        )
        await client.set("in-db-2", "yes")
        assert await client.get("in-db-2") == b"yes"
        assert await admin.get("in-db-2") is None
        await client.flushdb()
        await client.close()
        bad = Redis.from_url(redis_url, username="tester", password="wrong")
        with pytest.raises(ResponseError):
            await bad.ping()
        await bad.close()
    finally:
        await admin.execute("ACL", "DELUSER", "tester")
        await admin.close()


async def test_connection_refused() -> None:
    pool = ConnectionPool.from_url("redis://127.0.0.1:1", connect_timeout=1)
    with pytest.raises(RedisConnectionError):
        await Redis(pool).ping()
