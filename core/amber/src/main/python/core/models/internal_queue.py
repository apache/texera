from dataclasses import dataclass

from core.models.marker import Marker
from core.models.payload import DataPayload
from core.util.customized_queue import DoubleBlockingQueue
from core.util.customized_queue.queue_base import IQueue
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlPayloadV2


class InternalQueue(DoubleBlockingQueue):
    def __init__(self):
        super().__init__(InputDataElement, OutputDataElement, Marker)


@dataclass
class InternalQueueElement(IQueue.QueueElement):
    pass


@dataclass
class InputDataElement(InternalQueueElement):
    payload: DataPayload
    from_: ActorVirtualIdentity


@dataclass
class OutputDataElement(InternalQueueElement):
    payload: DataPayload
    to: ActorVirtualIdentity


@dataclass
class ControlElement(InternalQueueElement):
    cmd: ControlPayloadV2
    from_: ActorVirtualIdentity
