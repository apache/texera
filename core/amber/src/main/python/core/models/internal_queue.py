from dataclasses import dataclass
from typing import TypeVar

from core.models.marker import Marker
from core.models.payload import DataPayload
from core.util.customized_queue.linked_blocking_multi_queue import (
    LinkedBlockingMultiQueue, )
from core.util.customized_queue.queue_base import IQueue
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlPayloadV2

T = TypeVar("T")


class InternalQueue(IQueue):
    def __init__(self):
        self._queue = LinkedBlockingMultiQueue()
        self._queue._add_sub_queue("system", 0)
        self._queue._add_sub_queue("control", 1)
        self._queue._add_sub_queue("data", 2)

    def is_empty(self, key=None) -> bool:
        return self._queue.is_empty(key)

    def get(self) -> T:
        return self._queue.get()

    def put(self, item):
        if isinstance(item, DataElement):
            self._queue.put("data", item)
        elif isinstance(item, ControlElement):
            self._queue.put("control", item)
        else:
            self._queue.put("system", item)


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
