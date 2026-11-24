import unittest

from syntasks.instrumentation import SynTasksInstrumentor

from .syntasks_test_tasks import broker


class TestUtils(unittest.TestCase):
    @staticmethod
    def test_duplicate_instrumentation() -> None:
        first = SynTasksInstrumentor()
        first.instrument_broker(broker)
        second = SynTasksInstrumentor()
        second.instrument_broker(broker)
        SynTasksInstrumentor().uninstrument()
