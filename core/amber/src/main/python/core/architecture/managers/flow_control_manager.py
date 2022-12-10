import queue
from collections import defaultdict
from typing import MutableMapping, Optional

from core.models import InternalQueueElement, DataElement
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity


class FlowControlManager:
    """
    Credit-based flow control logic based off of the equivalent Scala FlowControl unit
    """

    # TODO : REPLACE DUMMY VALUES WITH ACTUAL CONFIGURED VALUES
    FLOW_CONTROL_IS_ENABLED = False
    UNPROCESSED_BATCHES_CREDIT_LIMIT_PER_SENDER = 100

    def __init__(self):
        self.receiver_id_to_credits: MutableMapping[
            ActorVirtualIdentity, int
        ] = defaultdict(lambda: self.UNPROCESSED_BATCHES_CREDIT_LIMIT_PER_SENDER)
        self.data_messages_awaiting_credits: MutableMapping[
            ActorVirtualIdentity, queue.Queue[InternalQueueElement]
        ] = defaultdict(queue.Queue)

    def get_message_to_forward(
        self, message: InternalQueueElement
    ) -> Optional[InternalQueueElement]:
        """
        Return next queued message to be sent to receiver, only if there are enough credits

        :param message: incoming message
        :return: next message to be sent to receiver, or None if not enough credits
        """
        if (
            isinstance(message, DataElement)
            and FlowControlManager.FLOW_CONTROL_IS_ENABLED
        ):
            self.data_messages_awaiting_credits[message.tag].put(message)
            if self.receiver_id_to_credits[message.tag] > 0:
                self.receiver_id_to_credits[message.tag] -= 1
                return self.data_messages_awaiting_credits[message.tag].get()
            else:
                return None
        return message

    def update_credits(
        self, receiver_id: ActorVirtualIdentity, credit_amount: int
    ) -> None:
        """
        Save the credit amount returned by the receiver worker

        :param receiver_id: the ActorVirtualIdentity of the receiver worker this ack is from
        :param credit_amount: the amount of credits the receiver worker has for this sender
        """
        if credit_amount <= 0:
            self.receiver_id_to_credits[receiver_id] = 0
        else:
            self.receiver_id_to_credits[receiver_id] = credit_amount
