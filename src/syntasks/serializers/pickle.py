import pickle
from typing import Any

from syntasks.abc.serializer import SynTasksSerializer


class PickleSerializer(SynTasksSerializer):
    """Serializer that uses pickle."""

    def dumpb(self, value: Any) -> bytes:
        """Dumps value to bytes."""
        return pickle.dumps(value)

    def loadb(self, value: bytes) -> Any:
        """Loads value from bytes."""
        return pickle.loads(value)  # noqa: S301
