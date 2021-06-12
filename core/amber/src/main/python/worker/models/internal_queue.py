from dataclasses import dataclass

from worker.models.control_payload import ControlPayload
from worker.models.generated.virtualidentity_pb2 import LinkIdentity, ActorVirtualIdentity
from worker.models.tuple import ITuple
from worker.util.stable_priority_queue import StablePriorityQueue, QueueElement


class InternalQueue(StablePriorityQueue):
    pass


@dataclass
class InternalQueueElement(QueueElement):
    pass


@dataclass
class InputTuple(InternalQueueElement):
    tuple: ITuple
    _priority: int = 1


@dataclass
class ControlElement(InternalQueueElement):
    cmd: ControlPayload
    from_: ActorVirtualIdentity
    _priority: int = 0


@dataclass
class SenderChangeMarker(InternalQueueElement):
    link: LinkIdentity
    _priority: int = 1


@dataclass
class EndMarker(InternalQueueElement):
    _priority: int = 1


@dataclass
class EndOfAllMarker(InternalQueueElement):
    _priority: int = 1
