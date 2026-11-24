"""
Instrument `syntasks`_ to trace SynTasks applications.

.. _syntasks: https://pypi.org/project/syntasks/

Usage
-----

* Run instrumented task

.. code:: python

    import asyncio

    from syntasks import InMemoryBroker
    from syntasks.instrumentation import SynTasksInstrumentor

    broker = InMemoryBroker()

    @broker.task
    async def add(x, y):
        return x + y

    async def main():
        SynTasksInstrumentor().instrument()
        await broker.startup()
        await my_task.kiq(1, 2)
        await broker.shutdown()

    if __name__ == "__main__":
        asyncio.run(main())

API
---
"""

import logging
from collections.abc import Callable, Collection
from typing import Any
from weakref import WeakSet as _WeakSet

try:
    import opentelemetry  # noqa: F401
except ImportError as exc:
    raise ImportError(
        "Cannot instrument. Please install 'syntasks[opentelemetry]'.",
    ) from exc

from opentelemetry.instrumentation.auto_instrumentation import initialize
from opentelemetry.instrumentation.instrumentor import (  # type: ignore[attr-defined]
    BaseInstrumentor,
)
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.metrics import MeterProvider
from opentelemetry.trace import TracerProvider
from wrapt import wrap_function_wrapper

import syntasks.worker.supervisor
from syntasks import AsyncBroker
from syntasks.middlewares.opentelemetry_middleware import OpenTelemetryMiddleware

logger = logging.getLogger("syntasks.opentelemetry")


class SynTasksInstrumentor(BaseInstrumentor):
    """OpenTelemetry instrumentor for SynTasks."""

    _instrumented_brokers: _WeakSet[AsyncBroker] = _WeakSet()
    _original_child_main: Callable[..., None] = syntasks.worker.supervisor.child_main

    def __init__(self) -> None:
        super().__init__()
        self._middleware = None

    def instrument_broker(
        self,
        broker: AsyncBroker,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
    ) -> None:
        """Instrument broker."""
        if not hasattr(broker, "_is_instrumented_by_opentelemetry"):
            broker._is_instrumented_by_opentelemetry = False  # type: ignore[attr-defined]  # noqa: SLF001

        if not getattr(broker, "is_instrumented_by_opentelemetry", False):
            broker.middlewares.insert(
                0,
                OpenTelemetryMiddleware(
                    tracer_provider=tracer_provider,
                    meter_provider=meter_provider,
                ),
            )
            broker._is_instrumented_by_opentelemetry = True  # type: ignore[attr-defined]  # noqa: SLF001
            if broker not in self._instrumented_brokers:
                self._instrumented_brokers.add(broker)
        else:
            logger.warning(
                "Attempting to instrument syntasks broker while already instrumented",
            )

    def uninstrument_broker(self, broker: AsyncBroker) -> None:
        """Uninstrument broker."""
        broker.middlewares = [
            middleware
            for middleware in broker.middlewares
            if not isinstance(middleware, OpenTelemetryMiddleware)
        ]
        broker._is_instrumented_by_opentelemetry = False  # type: ignore[attr-defined]  # noqa: SLF001
        self._instrumented_brokers.discard(broker)

    def instrumentation_dependencies(self) -> Collection[str]:
        """This function tells which library this instrumentor instruments."""
        return ("syntasks >= 0.0.0",)

    @classmethod
    def _child_main_with_initialize(cls, *args: Any, **kwargs: Any) -> None:
        initialize()
        cls._original_child_main(*args, **kwargs)

    def _instrument(self, **kwargs: Any) -> None:
        def broker_init(
            init: Callable[[Any], Any],
            broker: AsyncBroker,
            args: Any,
            kwargs: Any,
        ) -> None:
            result = init(*args, **kwargs)
            self.instrument_broker(broker)
            return result

        wrap_function_wrapper(
            "syntasks.abc.broker",
            "AsyncBroker.__init__",
            broker_init,
        )
        syntasks.worker.supervisor.child_main = self._child_main_with_initialize  # type: ignore[assignment]

    def _uninstrument(self, **kwargs: Any) -> None:
        instances_to_uninstrument = list(self._instrumented_brokers)
        for broker in instances_to_uninstrument:
            self.uninstrument_broker(broker)
        self._instrumented_brokers.clear()
        unwrap(AsyncBroker, "__init__")
        syntasks.worker.supervisor.child_main = self._original_child_main  # type: ignore[assignment]
