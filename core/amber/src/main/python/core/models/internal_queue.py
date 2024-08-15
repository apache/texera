from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from threading import RLock
from typing import TypeVar, Set

from core.models.internal_marker import InternalMarker
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

    class DisableType(Enum):
        DISABLE_BY_PAUSE = 1
        DISABLE_BY_BACKPRESSURE = 2

    def __init__(self):
        self._queue = LinkedBlockingMultiQueue()
        self._queue.add_sub_queue(InternalQueue.QueueID.SYSTEM.value, 0)
        self._queue.add_sub_queue(InternalQueue.QueueID.CONTROL.value, 1)
        self._queue.add_sub_queue(InternalQueue.QueueID.DATA.value, 2)
        self._queue_state: Set[InternalQueue.DisableType] = set()
        self._lock = RLock()

    def is_empty(self, key=None) -> bool:
        return self._queue.is_empty(key)

    def get(self) -> T:
        return self._queue.get()

    def put(self, item: T) -> None:
        if isinstance(item, (DataElement, InternalMarker)):
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

    def enable_data(self, disable_type: DisableType) -> bool:
        with self._lock:
            if disable_type in self._queue_state:
                self._queue_state.remove(disable_type)
            if self._queue_state:
                return False
            self._enable(InternalQueue.QueueID.DATA)
            return True

    def disable_data(self, disable_type: DisableType) -> None:
        with self._lock:
            self._queue_state.add(disable_type)
            self._disable(InternalQueue.QueueID.DATA)

    def in_mem_size(self) -> int:
        return self._queue.in_mem_size(InternalQueue.QueueID.DATA.value)

    def is_data_enabled(self) -> bool:
        return self._queue.is_enabled(InternalQueue.QueueID.DATA.value)
