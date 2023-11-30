from enum import Enum
from threading import RLock

from typing import Set, Generic, TypeVar

from core.models import InternalQueue
from core.util import IQueue


class DisableType(Enum):
    DISABLE_BY_PAUSE = 1
    DISABLE_BY_BACKPRESSURE = 2


T = TypeVar("T")


class InternalQueueManager(IQueue):
    def __init__(self, queue: InternalQueue):
        self.queue = queue
        self._queue_state: Set[DisableType] = set()
        self._lock = RLock()

    def disable_data(self, disable_type: DisableType) -> None:
        with self._lock:
            self._queue_state.add(disable_type)
            self.queue.disable_data()

    def enable_data(self, disable_type: DisableType) -> bool:
        with self._lock:
            self._queue_state.remove(disable_type)
            if self._queue_state:
                return False
            self.queue.enable_data()
            return True

    def put(self, item: T) -> None:
        self.queue.put(item)

    def get(self) -> T:
        return self.queue.get()

    def is_empty(self) -> bool:
        with self._lock:
            return self.queue.is_empty()

    def __len__(self) -> int:
        with self._lock:
            return len(self.queue)

    def is_control_empty(self) -> bool:
        with self._lock:
            return self.queue.is_control_empty()

    def is_data_empty(self) -> bool:
        with self._lock:
            return self.queue.is_data_empty()

    def is_data_enabled(self) -> bool:
        with self._lock:
            return not bool(self._queue_state)
