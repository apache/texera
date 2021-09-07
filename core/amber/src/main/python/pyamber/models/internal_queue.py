from dataclasses import dataclass

from pyamber.models.marker import Marker
from pyamber.models.payload import DataPayload
from pyamber.util.customized_queue import DoubleBlockingQueue
from pyamber.util.customized_queue.queue_base import IQueue
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlPayloadV2


class InternalQueue(DoubleBlockingQueue):
    def __init__(self):
        super().__init__(DataElement, Marker)


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
