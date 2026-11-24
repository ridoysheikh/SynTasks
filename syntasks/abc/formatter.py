from abc import ABC, abstractmethod

from syntasks.message import BrokerMessage, SynTasksMessage


class SynTasksFormatter(ABC):
    """Custom formatter for brokers."""

    @abstractmethod
    def dumps(self, message: SynTasksMessage) -> BrokerMessage:
        """
        Dump message to broker message instance.

        :param message: message to send.
        :return: message for brokers.
        """

    @abstractmethod
    def loads(self, message: bytes) -> SynTasksMessage:
        """
        Parses broker message to SynTasksMessage.

        :param message: message to parse.
        :return: parsed syntasks message.
        """
