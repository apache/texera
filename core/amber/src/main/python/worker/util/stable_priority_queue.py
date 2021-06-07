from dataclasses import dataclass, field
from queue import PriorityQueue
from typing import Optional, Tuple, Any


@dataclass
class QueueElement:
    def __init__(self):
        self._priority: int = 0

    @property
    def priority(self):
        return self._priority


@dataclass(order=True)
class PrioritizedItem:
    priority: Tuple[int, int]
    item: QueueElement = field(compare=False)


@dataclass
class QueueControl(QueueElement):
    msg: str
    _priority: int = 0


class StablePriorityQueue(PriorityQueue):
    counter = 0

    def put(self, item: QueueElement, block: bool = ..., timeout: Optional[float] = ...) -> None:
        # OPTIMIZE:
        #    The standard implementation of queue.PriorityQueue is not stable.
        #    Here is a temporary solution to make sure the queue is stable.
        #    However, the current implementation has the following two issues:
        #       1. violating thread-safe, the counter change may not be atomic.from
        #       2. the counter keeps increment, which will have a performance issue when the number gets
        #          really big. Python handles infinite int, but it gets really slow after 2**10000000
        #
        self.counter += 1
        if not isinstance(item, QueueElement):
            raise TypeError(f"Unsupported type {type(item)} to put in the {self.__class__.__name__}.")
        super(StablePriorityQueue, self).put(PrioritizedItem((item.priority, self.counter), item), block, timeout)

    def get(self, block: bool = ..., timeout: Optional[float] = ...) -> Any:
        prioritized_item: PrioritizedItem = super(StablePriorityQueue, self).get()
        return prioritized_item.item
