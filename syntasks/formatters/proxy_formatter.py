from typing import TYPE_CHECKING

from syntasks.abc.formatter import SynTasksFormatter
from syntasks.message import BrokerMessage, SynTasksMessage

if TYPE_CHECKING:
    from syntasks.abc.broker import AsyncBroker


class ProxyFormatter(SynTasksFormatter):
    """Default syntasks formatter."""

    def __init__(self, broker: "AsyncBroker") -> None:
        self.broker = broker

    def dumps(self, message: SynTasksMessage) -> BrokerMessage:
        """
        Dumps syntasks message to some broker message format.

        :param message: message to send.
        :return: Dumped message.
        """
        return BrokerMessage(
            task_id=message.task_id,
            task_name=message.task_name,
            message=self.broker.serializer.dumpb(message.to_dict()),
            labels=message.labels,
        )

    def loads(self, message: bytes) -> SynTasksMessage:
        """
        Loads json from message.

        :param message: broker's message.
        :return: parsed syntasks message.
        """
        return SynTasksMessage.from_dict(self.broker.serializer.loadb(message))
