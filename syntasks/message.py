from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from syntasks.codec import to_jsonable
from syntasks.labels import parse_label


def _require(
    data: Mapping[str, Any],
    key: str,
    expected: type | tuple[type, ...],
) -> Any:
    if key not in data:
        raise ValueError(f"Message is missing the required field {key!r}.")
    value = data[key]
    if not isinstance(value, expected):
        raise ValueError(
            f"Message field {key!r} has type {type(value).__name__}, "
            f"expected {expected}.",
        )
    return value


@dataclass(kw_only=True)
class SynTasksMessage:
    """
    Message abstractions.

    This an internal class used
    by brokers. Every remote call
    receive such messages.
    """

    task_id: str
    task_name: str
    labels: dict[str, Any]
    labels_types: dict[str, int] | None = None
    args: list[Any]
    kwargs: dict[str, Any]

    def parse_labels(self) -> None:
        """
        Parse labels.

        :return: None
        """
        if self.labels_types is None:
            return

        for label, label_type in self.labels_types.items():
            if label in self.labels:
                self.labels[label] = parse_label(self.labels[label], label_type)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the message to plain, JSON-compatible data.

        :return: message as a dict.
        """
        return {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "labels": to_jsonable(self.labels),
            "labels_types": self.labels_types,
            "args": to_jsonable(self.args),
            "kwargs": to_jsonable(self.kwargs),
        }

    @classmethod
    def from_dict(cls, data: Any) -> "SynTasksMessage":
        """
        Build a message from plain data, validating its shape.

        :param data: decoded message.
        :raises ValueError: if the data is not a valid message.
        :return: parsed message.
        """
        if not isinstance(data, Mapping):
            raise ValueError(
                f"Message must be a mapping, got {type(data).__name__}.",
            )
        labels_types = data.get("labels_types")
        if labels_types is not None and not (
            isinstance(labels_types, Mapping)
            and all(
                isinstance(k, str) and isinstance(v, int)
                for k, v in labels_types.items()
            )
        ):
            raise ValueError("Message field 'labels_types' must map str to int.")
        return cls(
            task_id=_require(data, "task_id", str),
            task_name=_require(data, "task_name", str),
            labels=dict(_require(data, "labels", Mapping)),
            labels_types=dict(labels_types) if labels_types is not None else None,
            args=list(_require(data, "args", (list, tuple))),
            kwargs=dict(_require(data, "kwargs", Mapping)),
        )


@dataclass(kw_only=True)
class BrokerMessage:
    """Format of messages for brokers."""

    task_id: str
    task_name: str
    message: bytes
    labels: dict[str, Any] = field(default_factory=dict)
