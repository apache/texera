from threading import RLock, Condition
from typing import List, Optional, Generic, TypeVar, Dict, Tuple, Union

from core.util.customized_queue.queue_base import IQueue
from core.util.thread.atomic import AtomicInteger

T = TypeVar("T")


class Node(Generic[T]):
    def __init__(self, item: T):
        self.item = item
        self.next: Optional[Node] = None


class SubQueue(Generic[T]):
    def __init__(self, key: str, outer_self):
        self.outer_self = outer_self
        self.key: str = key
        self.priority_group: Optional[PriorityGroup] = None
        self.put_lock = RLock()
        self.not_full = Condition(self.put_lock)
        self.count = AtomicInteger()
        self.enabled: bool = True
        self.head: Node[T] = Node(None)
        self.last: Optional[Node[T]] = self.head

    def clear(self) -> None:
        raise NotImplementedError()

    def disable(self):
        self.fully_lock()
        try:
            if not self.enabled:
                return None
            self.outer_self.total_count.dec(self.count.value)
            self.enabled = False
        finally:
            self.fully_unlock()

    def enable(self) -> None:
        self.fully_lock()
        try:
            if self.enabled:
                return None
            self.enabled = True

            # potentially unlock waiting polls
            c = self.count.value
            if c > 0:
                self.outer_self.total_count.inc(c)
                self.outer_self.not_empty.notify()

        finally:
            self.fully_unlock()

    def is_enabled(self) -> bool:
        self.outer_self.take_lock.acquire()
        try:
            return self.enabled
        finally:
            self.outer_self.take_lock.release()

    def signal_not_full(self) -> None:
        self.put_lock.acquire()
        try:
            self.not_full.notify()
        finally:
            self.put_lock.release()

    def enqueue(self, node) -> None:
        self.last.next = node
        self.last = node

    def __str__(self):
        res = ""
        h = self.head
        while h.next is not None:
            res += h.next.item
            res += " -> "
            h = h.next
        return res

    def size(self) -> int:
        return self.count.value

    def is_empty(self) -> bool:
        return self.size() == 0

    def put(self, e) -> None:
        old_size = -1
        node = Node(e)
        self.put_lock.acquire()
        try:
            self.enqueue(node)
            self.count.inc()
            if self.enabled:
                old_size = self.outer_self.total_count.get_and_inc()
        finally:
            self.put_lock.release()

        if old_size == 0:
            self.outer_self._signal_not_empty()

    def remove(self, obj: T) -> bool:
        self.fully_lock()
        try:
            trail = self.head
            while trail.next is not None:
                if trail.item == obj:
                    self.unlink(trail, trail.next)
                    return True
                trail = trail.next
            return False
        finally:
            self.fully_unlock()

    def unlink(self, trail: Node, next_: Node) -> None:
        trail.item = None
        trail.next = next_.next
        if self.last == next_:
            self.last = trail
        if self.enabled:
            self.outer_self.total_count.get_and_dec()

    def fully_lock(self) -> None:
        self.put_lock.acquire()
        self.outer_self.take_lock.acquire()

    def fully_unlock(self) -> None:
        self.put_lock.release()
        self.outer_self.take_lock.release()

    def dequeue(self) -> T:
        assert self.size() > 0
        h = self.head
        first = h.next
        h.next = h
        self.head = first
        x = first.item
        first.item = None
        return x


class PriorityGroup(Generic[T]):
    def __init__(self, priority=0):
        self.priority = priority
        self.queues: List[SubQueue[T]] = list()
        self.next_idx = 0

    def add_queue(self, sub_queue: SubQueue[T]) -> None:
        self.queues.append(sub_queue)
        sub_queue.priority_group = self

    def remove_queue(self, sub_queue) -> None:
        raise NotImplementedError("Explicitly do not support remove queue.")

    def get_next_sub_queue(self) -> Optional[SubQueue[T]]:
        start_idx = self.next_idx
        queues = [q for q in self.queues]
        while True:
            child = queues[self.next_idx]
            self.next_idx += 1
            if self.next_idx == len(queues):
                self.next_idx = 0
            if child.enabled and child.size() > 0:
                return child
            if self.next_idx == start_idx:
                break
        return None

    def peek(self) -> Optional[T]:
        start_idx = self.next_idx
        while True:
            child = self.queues[self.next_idx]
            if child.enabled and child.size() > 0:
                return child.head.next.item
            else:
                self.next_idx += 1
                if self.next_idx == len(self.queues):
                    self.next_idx = 0
            if self.next_idx == start_idx:
                break
        return None


class DefaultSubQueueSelection(Generic[T]):
    def __init__(self, priority_groups: List[PriorityGroup]):
        self.priority_groups: List[PriorityGroup] = priority_groups

    def get_next(self) -> Optional[SubQueue]:
        for pg in self.priority_groups:
            sub_queue = pg.get_next_sub_queue()
            if sub_queue is not None:
                return sub_queue
        return None

    def peek(self) -> Optional[T]:
        for pg in self.priority_groups:
            deque = pg.peek()
            if deque is not None:
                return deque
        return None

    def set_priority_groups(self, priority_groups):
        self.priority_groups = priority_groups


class LinkedBlockingMultiQueue(IQueue):
    def __init__(self, subtypes: Dict[Union[type, Tuple[type]], Tuple[str, int]]):
        self.take_lock = RLock()
        self.not_empty = Condition(self.take_lock)
        self.sub_queues = dict()  # thread-safe in CPython
        self.total_count = AtomicInteger()
        self.priority_groups = list()  # thread-safe in CPython

        self.sub_queue_selection = DefaultSubQueueSelection(self.priority_groups)

        self.subtypes = dict()
        for type_, (key, _) in subtypes.items():
            if isinstance(type_, tuple):
                for t in type_:
                    self.subtypes[t] = key
            else:
                self.subtypes[type_] = key

        for _, (key, priority) in subtypes.items():
            self._add_sub_queue(key, priority)

    def put(self, item: T) -> None:
        t = type(item)
        key = self.subtypes[t]
        if key is None:
            for type_, k in self.subtypes.items():
                if isinstance(item, type_):
                    key = k
                    break
            if key is None:
                raise KeyError("this type of element is not supported: " + str(t))

        self._get_sub_queue(key).put(item)

    def get(self) -> T:
        self.take_lock.acquire()
        try:
            while self.total_count.value == 0:
                self.not_empty.wait()

            # at this point we know there is an element
            sub_queue = self.sub_queue_selection.get_next()
            ele = sub_queue.dequeue()
            sub_queue.count.dec()
            if self.total_count.get_and_dec() > 1:
                # sub queue still has element
                self.not_empty.notify()
        finally:
            self.take_lock.release()

        return ele

    def peek(self) -> Optional[T]:
        self.take_lock.acquire()
        try:
            if self.total_count.value == 0:
                return None
            else:
                return self.sub_queue_selection.peek()
        finally:
            self.take_lock.release()

    def enable(self, key):
        self._get_sub_queue(key).enable()

    def disable(self, key):
        self._get_sub_queue(key).disable()

    def size(self, key=None):
        if key is not None:
            return self._get_sub_queue(key).size()
        else:
            return self.total_count.value

    def is_empty(self, key=None) -> bool:
        return self.size(key) == 0

    def _add_sub_queue(self, key: str, priority: int) -> SubQueue:
        sub_queue = SubQueue(key, self)
        self.take_lock.acquire()

        try:
            old_queue = self.sub_queues.get(key)
            self.sub_queues[key] = sub_queue
            if old_queue is None:
                i = 0
                added = False
                for pg in self.priority_groups:
                    if pg.priority == priority:
                        pg.add_queue(sub_queue)
                        added = True
                        break
                    elif pg.priority > priority:
                        new_pg = PriorityGroup(priority)
                        new_pg.add_queue(sub_queue)
                        self.priority_groups.append(new_pg)
                        added = True
                        break

                    i += 1
                if not added:
                    new_pg = PriorityGroup(priority)
                    new_pg.add_queue(sub_queue)
                    self.priority_groups.append(new_pg)

            return old_queue
        finally:
            self.take_lock.release()

    def _get_sub_queue(self, key) -> SubQueue:
        return self.sub_queues.get(key)

    def _signal_not_empty(self) -> None:
        self.take_lock.acquire()
        try:
            self.not_empty.notify()
        finally:
            self.take_lock.release()


if __name__ == "__main__":
    lbmq = LinkedBlockingMultiQueue({str: ("control", 0), int: ("data", 2)})
    print(lbmq.size())
    assert lbmq.is_empty()
    control = lbmq._get_sub_queue("control")
    control.put("one")
    print("size:", lbmq.size())
    control.enable()
    lbmq.put(2)
    print("size:", lbmq.size())
    lbmq.put(3)
    print("size:", lbmq.size())
    lbmq.put(4)
    print("size:", lbmq.size())
    assert not control.is_empty()
    print("-----------")
    print("size:", lbmq.size())
    print(lbmq.get())
    print("size:", lbmq.size())
    print(lbmq.get())
    print("size:", lbmq.size())
    print(lbmq.get())
    print("size:", lbmq.size())
    print(lbmq.get())
