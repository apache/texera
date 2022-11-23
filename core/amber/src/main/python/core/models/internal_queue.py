from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TypeVar

from core.models.marker import Marker
from core.models.payload import DataPayload
from core.util.customized_queue.linked_blocking_multi_queue import (
    LinkedBlockingMultiQueue,
)
from core.util.customized_queue.queue_base import IQueue, QueueElement
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlPayloadV2


@dataclass
class InternalQueueElement(QueueElement):
    pass


@dataclass
class DataElement(InternalQueueElement):
    tag: ActorVirtualIdentity
    payload: DataPayload


@dataclass
class ControlElement(InternalQueueElement):
    tag: ActorVirtualIdentity
    payload: ControlPayloadV2


T = TypeVar("T", bound=InternalQueueElement)


class InternalQueue(IQueue):
    class QueueID(Enum):
        SYSTEM = "system"
        CONTROL = "control"
        DATA = "data"

    def __init__(self):
        self._queue = LinkedBlockingMultiQueue()
        self._queue.add_sub_queue(InternalQueue.QueueID.SYSTEM.value, 0)
        self._queue.add_sub_queue(InternalQueue.QueueID.CONTROL.value, 1)
        self._queue.add_sub_queue(InternalQueue.QueueID.DATA.value, 2)

    def is_empty(self, key=None) -> bool:
        return self._queue.is_empty(key)

    def get(self) -> T:
        return self._queue.get()

    def put(self, item: T) -> None:
        if isinstance(item, (DataElement, Marker)):
            self._queue.put(InternalQueue.QueueID.DATA.value, item)
        elif isinstance(item, ControlElement):
            self._queue.put(InternalQueue.QueueID.CONTROL.value, item)
        else:
            self._queue.put(InternalQueue.QueueID.SYSTEM.value, item)

    def __len__(self) -> int:
        return self._queue.size()

    def disable(self, queue: InternalQueue.QueueID) -> None:
        self._queue.disable(str(queue.value))

    def enable(self, queue: InternalQueue.QueueID) -> None:
        self._queue.enable(str(queue.value))

    def size(self, queue: InternalQueue.QueueID) -> int:
        return self._queue.size(str(queue.value))
