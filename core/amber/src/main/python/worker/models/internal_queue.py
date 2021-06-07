from dataclasses import dataclass, field
from queue import PriorityQueue
from typing import Optional, Tuple

from worker.models.control_payload import ControlPayload
from worker.models.identity import VirtualIdentity, LinkIdentity
from worker.models.tuple import ITuple


@dataclass
class InternalQueueElement:
    pass


@dataclass(order=True)
class PrioritizedItem:
    priority: Tuple[int, int]
    item: InternalQueueElement = field(compare=False)


@dataclass
class InputTuple(InternalQueueElement):
    tuple: ITuple
    _priority: int = 1


@dataclass
class ControlElement(InternalQueueElement):
    cmd: ControlPayload
    from_: VirtualIdentity
    _priority: int = 0


@dataclass
class SenderChangeMarker(InternalQueueElement):
    link: LinkIdentity
    _priority: int = 1


@dataclass
class EndMarker(InternalQueueElement):
    _priority: int = 1
    pass


@dataclass
class EndOfAllMarker(InternalQueueElement):
    _priority: int = 1
    pass


class InternalQueue(PriorityQueue):
    counter = 0

    def put(self, item: InternalQueueElement, block: bool = ..., timeout: Optional[float] = ...) -> None:
        if not isinstance(item, InternalQueueElement):
            raise TypeError(f"Unsupported type {type(item)} to put in the InternalQueue.")

        # OPTIMIZE:
        #    The standard implementation of queue.PriorityQueue is not stable.
        #    Here is a temporary solution to make sure the queue is stable.
        #    However, the current implementation has the following two issues:
        #       1. violating thread-safe, the counter change may not be atomic.from
        #       2. the counter keeps increment, which will have a performance issue when the number gets
        #          really big. Python handles infinite int, but it gets really slow after 2**10000000
        #
        self.counter += 1

        super(InternalQueue, self).put(PrioritizedItem((item._priority, self.counter), item), block, timeout)

    def get(self, block: bool = ..., timeout: Optional[float] = ...) -> InternalQueueElement:
        prioritized_item: PrioritizedItem = super(InternalQueue, self).get()
        return prioritized_item.item
