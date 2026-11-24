import json

from syntasks.abc.formatter import SynTasksFormatter
from syntasks.message import BrokerMessage, SynTasksMessage


class JSONFormatter(SynTasksFormatter):
    """JSON syntasks formatter."""

    def dumps(self, message: SynTasksMessage) -> BrokerMessage:
        """
        Dumps syntasks message to some broker message format.

        :param message: message to send.
        :return: Dumped message.
        """
        return BrokerMessage(
            task_id=message.task_id,
            task_name=message.task_name,
            message=json.dumps(message.to_dict()).encode(),
            labels=message.labels,
        )

    def loads(self, message: bytes) -> SynTasksMessage:
        """
        Loads json from message.

        :param message: broker's message.
        :return: parsed syntasks message.
        """
        return SynTasksMessage.from_dict(json.loads(message))
