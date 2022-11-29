from dataclasses import dataclass

from core.models.marker import Marker
from core.models.payload import DataPayload
from core.util.customized_queue import DoubleBlockingQueue
from core.util.customized_queue.queue_base import IQueue
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlPayloadV2
from collections import defaultdict
from typing import T

class InternalQueue(DoubleBlockingQueue):
    def __init__(self):
        super().__init__(DataElement, Marker)

        """
        input_batches_put_in_queue : maps actor to number of batches in its queue, written/read by network_receiver thread
        input_batches_taken_out_of_queue : maps actor to number of batches taken out of its queue, written by DP, read by network_receiver
        """
        self._input_batches_put_in_queue = defaultdict(int)
        self._input_batches_taken_out_of_queue = defaultdict(int)

    def get_num_batches_in_queue(self, sender: "ActorVirtualIdentity") -> int:
        """
        Returns the number of batches (not tuples) from the specified sender that are currently in the queue, which will be used
        to calculate the available credits for this sender

        :param sender: the ActorVirtualIdentity of the sender
        :return: number of batches from sender currently in queue
        """
        # credits = batch_limit_per_sender - num_batches_in_queue
        # here we only return num_batches_in_queue since we dont know batch_limit_per_sender
        return self._input_batches_put_in_queue[sender] - self._input_batches_taken_out_of_queue[sender]

    # override get / put methods to update dictionaries tracking number of tuples taken in / out
    def get(self) -> T:
        item = DoubleBlockingQueue.get(self)

        if isinstance(item, DataElement):
            self._input_batches_taken_out_of_queue[item.tag] -= 1

        return item

    def put(self, item: T) -> None:
        if isinstance(item, DataElement):
            self._input_batches_put_in_queue[item.tag] += 1

        DoubleBlockingQueue.put(self, item)

@dataclass
class InternalQueueElement(IQueue.QueueElement):
    pass


@dataclass
class DataElement(InternalQueueElement):
    tag: ActorVirtualIdentity
    payload: DataPayload


@dataclass
class ControlElement(InternalQueueElement):
    tag: ActorVirtualIdentity
    payload: ControlPayloadV2
