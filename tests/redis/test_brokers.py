import asyncio
import contextlib
import uuid
from collections.abc import AsyncGenerator
from typing import Any

import pytest

from syntasks import (
    AckableMessage,
    BrokerMessage,
    RedisListBroker,
    RedisPubSubBroker,
    RedisResultBackend,
    RedisStreamBroker,
)
from syntasks.api import run_receiver_task
from syntasks.redis import Redis
from syntasks.utils import maybe_awaitable


def _message(data: bytes, **labels: Any) -> BrokerMessage:
    return BrokerMessage(task_id="id", task_name="name", message=data, labels=labels)


async def _next(gen: AsyncGenerator[Any, None], timeout: float = 5) -> Any:
    return await asyncio.wait_for(anext(gen), timeout=timeout)


@pytest.fixture
def queue() -> str:
    return f"q-{uuid.uuid4().hex}"


async def test_list_broker_roundtrip(redis_url: str, queue: str) -> None:
    broker = RedisListBroker(redis_url, queue_name=queue, poll_timeout=0.1)
    await broker.startup()
    gen = broker.listen()
    try:
        await broker.kick(_message(b"one"))
        await broker.kick(_message(b"two"))
        # FIFO order.
        assert await _next(gen) == b"one"
        assert await _next(gen) == b"two"
    finally:
        await gen.aclose()
        await broker.shutdown()


async def test_list_broker_delay(redis_url: str, queue: str) -> None:
    broker = RedisListBroker(redis_url, queue_name=queue, poll_timeout=0.1)
    await broker.startup()
    gen = broker.listen()
    try:
        loop = asyncio.get_running_loop()
        start = loop.time()
        await broker.kick(_message(b"later", delay=0.5))
        await broker.kick(_message(b"now"))
        assert await _next(gen) == b"now"
        assert await _next(gen) == b"later"
        assert loop.time() - start >= 0.5
    finally:
        await gen.aclose()
        await broker.shutdown()


async def test_stream_broker_ack(redis_url: str, queue: str) -> None:
    broker = RedisStreamBroker(redis_url, queue_name=queue, xread_block=100)
    broker.is_worker_process = True
    await broker.startup()
    gen = broker.listen()
    try:
        await broker.kick(_message(b"payload"))
        message = await _next(gen)
        assert isinstance(message, AckableMessage)
        assert message.data == b"payload"
        redis = broker.redis
        assert await redis.xpending_count(queue, broker.consumer_group_name) == 1
        await maybe_awaitable(message.ack())
        assert await redis.xpending_count(queue, broker.consumer_group_name) == 0
    finally:
        await gen.aclose()
        await broker.shutdown()


async def test_stream_broker_reclaims_abandoned(redis_url: str, queue: str) -> None:
    crashed = RedisStreamBroker(
        redis_url,
        queue_name=queue,
        consumer_name="crashed",
        xread_block=100,
    )
    crashed.is_worker_process = True
    await crashed.startup()
    gen = crashed.listen()
    await crashed.kick(_message(b"work"))
    taken = await _next(gen)
    assert taken.data == b"work"
    # The worker dies without acknowledging.
    await gen.aclose()
    await crashed.shutdown()

    rescuer = RedisStreamBroker(
        redis_url,
        queue_name=queue,
        consumer_name="rescuer",
        xread_block=100,
        idle_timeout=50,
    )
    rescuer.is_worker_process = True
    await rescuer.startup()
    await asyncio.sleep(0.1)
    gen = rescuer.listen()
    try:
        message = await _next(gen)
        assert message.data == b"work"
        await maybe_awaitable(message.ack())
        assert await rescuer.redis.xpending_count(queue, "syntasks") == 0
    finally:
        await gen.aclose()
        await rescuer.shutdown()


async def test_stream_broker_delay_and_maxlen(redis_url: str, queue: str) -> None:
    broker = RedisStreamBroker(
        redis_url,
        queue_name=queue,
        xread_block=100,
        maxlen=1000,
    )
    broker.is_worker_process = True
    await broker.startup()
    gen = broker.listen()
    try:
        await broker.kick(_message(b"later", delay=0.3))
        message = await _next(gen)
        assert message.data == b"later"
        await maybe_awaitable(message.ack())
    finally:
        await gen.aclose()
        await broker.shutdown()


async def test_stream_broker_recreates_deleted_group(
    redis_url: str,
    queue: str,
) -> None:
    broker = RedisStreamBroker(redis_url, queue_name=queue, xread_block=100)
    broker.is_worker_process = True
    await broker.startup()
    await broker.redis.delete(queue)
    gen = broker.listen()
    try:
        await asyncio.sleep(0.2)
        await broker.kick(_message(b"again"))
        message = await _next(gen)
        assert message.data == b"again"
    finally:
        await gen.aclose()
        await broker.shutdown()


async def test_pubsub_broker_fanout(redis_url: str, queue: str) -> None:
    first = RedisPubSubBroker(redis_url, queue_name=queue)
    second = RedisPubSubBroker(redis_url, queue_name=queue)
    gens = [first.listen(), second.listen()]
    tasks = [asyncio.create_task(anext(gen)) for gen in gens]
    try:
        # Give both listeners time to subscribe.
        await asyncio.sleep(0.3)
        await first.kick(_message(b"broadcast"))
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
        assert results == [b"broadcast", b"broadcast"]
    finally:
        for task in tasks:
            task.cancel()
        for gen in gens:
            with contextlib.suppress(Exception):
                await gen.aclose()
        await first.shutdown()
        await second.shutdown()


async def test_listener_survives_redis_outage(queue: str) -> None:
    # Points at a closed port: the listener must keep retrying, not crash.
    broker = RedisListBroker(
        "redis://127.0.0.1:1",
        queue_name=queue,
        connect_timeout=0.2,
    )
    gen = broker.listen()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(anext(gen), timeout=0.8)
    await gen.aclose()


async def test_shared_client(redis_url: str, queue: str) -> None:
    client = Redis.from_url(redis_url)
    broker = RedisListBroker(queue_name=queue, client=client, poll_timeout=0.1)
    await broker.startup()
    await broker.kick(_message(b"x"))
    await broker.shutdown()
    # The broker doesn't own the shared client, so it still works.
    assert await client.llen(queue) == 1
    await client.close()


@pytest.mark.parametrize("broker_cls", [RedisStreamBroker, RedisListBroker])
async def test_end_to_end(redis_url: str, queue: str, broker_cls: type) -> None:
    kwargs: dict[str, Any] = {"queue_name": queue}
    if broker_cls is RedisStreamBroker:
        kwargs["xread_block"] = 100
    else:
        kwargs["poll_timeout"] = 0.1
    broker = broker_cls(redis_url, **kwargs).with_result_backend(
        RedisResultBackend(redis_url, prefix=f"res-{queue}"),
    )

    @broker.task
    async def add(a: int, b: int) -> int:
        return a + b

    @broker.task
    async def fail() -> None:
        raise ValueError("boom")

    worker_broker = broker_cls(redis_url, **kwargs).with_result_backend(
        RedisResultBackend(redis_url, prefix=f"res-{queue}"),
    )
    worker_broker.register_task(add.original_func, task_name=add.task_name)
    worker_broker.register_task(fail.original_func, task_name=fail.task_name)
    worker_broker.is_worker_process = True

    await broker.startup()
    worker = asyncio.create_task(run_receiver_task(worker_broker))
    try:
        task = await add.kiq("2", 3)  # "2" is converted to int by the worker.
        result = await task.wait_result(timeout=10)
        assert not result.is_err
        assert result.return_value == 5

        failing = await fail.kiq()
        failed = await failing.wait_result(timeout=10)
        assert failed.is_err
        assert isinstance(failed.error, ValueError)
        assert str(failed.error) == "boom"
    finally:
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker
        await worker_broker.shutdown()
        await broker.shutdown()
