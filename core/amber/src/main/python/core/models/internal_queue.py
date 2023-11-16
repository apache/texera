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
from pympler import asizeof


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

    def _disable(self, queue: InternalQueue.QueueID) -> None:
        self._queue.disable(str(queue.value))

    def _enable(self, queue: InternalQueue.QueueID) -> None:
        self._queue.enable(str(queue.value))

    def is_control_empty(self) -> bool:
        return self.is_empty(InternalQueue.QueueID.CONTROL.value)

    def is_data_empty(self) -> bool:
        return self.is_empty(InternalQueue.QueueID.DATA.value)

    def __len__(self) -> int:
        return self.size()

    def size(self) -> int:
        return self._queue.size()

    def size_control(self) -> int:
        return self._queue.size(InternalQueue.QueueID.CONTROL.value)

    def size_data(self) -> int:
        return self._queue.size(InternalQueue.QueueID.DATA.value)

    def enable_data(self) -> None:
        self._enable(InternalQueue.QueueID.DATA)

    def disable_data(self) -> None:
        self._disable(InternalQueue.QueueID.DATA)

    def in_mem_size(self) -> int:
        return asizeof.asizeof(
            self._queue.get_sub_queue(InternalQueue.QueueID.DATA.value)
        )
