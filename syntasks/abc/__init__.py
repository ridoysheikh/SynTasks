"""Abstract classes for syntasks."""

from syntasks.abc.broker import AsyncBroker
from syntasks.abc.result_backend import AsyncResultBackend

__all__ = [
    "AsyncBroker",
    "AsyncResultBackend",
]
