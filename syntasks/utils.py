import asyncio
import contextlib
import inspect
from collections.abc import AsyncGenerator, Awaitable, Coroutine
from types import CoroutineType
from typing import Any, TypeVar, Union

_T = TypeVar("_T")


async def maybe_awaitable(
    possible_coroutine: Union[
        _T,
        Coroutine[Any, Any, _T],
        "CoroutineType[Any, Any, _T]",
        Awaitable[_T],
    ],
) -> _T:
    """
    Awaits coroutine if needed.

    This function allows run function
    that may return coroutine.

    It not awaitable value passed, it
    returned immediately.

    :param possible_coroutine: some value.
    :return: value.
    """
    if inspect.isawaitable(possible_coroutine):
        return await possible_coroutine
    return possible_coroutine  # type: ignore


_DONE: Any = object()


async def _start_producer(
    stream: AsyncGenerator[_T, None],
    queue: "asyncio.Queue[tuple[_T, asyncio.Future[None]] | Any]",
) -> None:
    """
    Forward all values of one stream to the shared queue.

    Every value is handed over one at a time: the producer waits until
    the consumer has taken the value before pulling the next one, so a
    slow consumer applies backpressure to every source.

    The source generator is closed even if the producer was cancelled
    while waiting for the hand-off and not inside the generator itself.
    """
    loop = asyncio.get_running_loop()
    try:
        async for val in stream:
            delivered: asyncio.Future[None] = loop.create_future()
            await queue.put((val, delivered))
            await delivered
    finally:
        await stream.aclose()


async def _run_producers(
    iterables: "tuple[AsyncGenerator[_T, None], ...]",
    queue: "asyncio.Queue[tuple[_T, asyncio.Future[None]] | Any]",
) -> None:
    """
    Pump every iterable into the queue in a task group.

    This coroutine runs as a dedicated task, so its task group
    is always entered and exited in the same task, no matter
    which task iterates over or closes the merged generator.
    """
    try:
        async with asyncio.TaskGroup() as tg:
            for iterable in iterables:
                tg.create_task(_start_producer(iterable, queue))
    finally:
        queue.put_nowait(_DONE)


async def merge_async_iterables(
    *iterables: AsyncGenerator[_T, None],
) -> AsyncGenerator[_T, None]:
    """
    Merge multiple async generators into a single one.

    All values from every generator are yielded
    as soon as they arrive, in no particular order.

    If any of the inner generators raises an exception,
    all other generators are cancelled and the error is
    re-raised for the consumer wrapped in an exception group.

    When the merged generator is closed or cancelled,
    all inner generators are closed as well.

    :param iterables: async generators to merge.
    :return: single generator with values from all generators.
    """
    queue: asyncio.Queue[tuple[_T, asyncio.Future[None]] | Any] = asyncio.Queue()
    producers = asyncio.create_task(_run_producers(iterables, queue))
    try:
        while True:
            item = await queue.get()
            if item is _DONE:
                break
            value, delivered = item
            # The producer gave up on this value (it was cancelled),
            # so it was never delivered.
            if delivered.done():
                continue
            delivered.set_result(None)
            yield value
        # Every producer finished. If any of them failed,
        # its error surfaces now.
        await producers
    finally:
        producers.cancel()
        with contextlib.suppress(asyncio.CancelledError, BaseExceptionGroup):
            await producers


async def run_shielded(awaitable: Awaitable[_T]) -> _T:
    """
    Run an awaitable to completion even if the caller is cancelled.

    Cleanup code uses this so that cancelling the outer task cannot
    interrupt it halfway. If the caller was cancelled meanwhile, the
    cancellation is re-raised once the awaitable has finished.

    :param awaitable: awaitable to run.
    :return: its result.
    """
    task = asyncio.ensure_future(awaitable)
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                raise
            cancelled = True
        else:
            break
    if cancelled:
        raise asyncio.CancelledError
    return result
