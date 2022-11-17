from __future__ import annotations

import inspect
import weakref
from threading import RLock, Condition
from typing import List, Optional, Generic, TypeVar, Dict, Tuple, Union

import six

from core.util.customized_queue.inner import inner
from core.util.customized_queue.queue_base import IQueue
from core.util.thread.atomic import AtomicInteger

T = TypeVar("T")

# helper method from `peewee` project to add metaclass
_METACLASS_ = '_metaclass_helper_'


def with_metaclass(meta, base=object):
    return meta(_METACLASS_, (base,), {})


class OuterMeta(type):
    def __new__(mcs, name, parents, dct):
        cls = super(OuterMeta, mcs).__new__(mcs, name, parents, dct)
        for klass in dct.values():
            if inspect.isclass(klass):
                print("Setting outer of '%s' to '%s'" % (klass, cls))
                klass.owner = cls

        return cls


class innerclass(object):
    """Descriptor for making inner classes.

    Adds a property 'owner' to the inner class, pointing to the outer
    owner instance.
    """

    # Use a weakref dict to memoise previous results so that
    # instance.Inner() always returns the same inner classobj.
    #
    def __init__(self, inner):
        self.inner = inner
        self.instances = weakref.WeakKeyDictionary()

    # Not thread-safe - consider adding a lock.
    #
    def __get__(self, instance, _):
        if instance is None:
            return self.inner
        if instance not in self.instances:
            self.instances[instance] = type(self.inner.__name__, (self.inner,),
                                            {'owner': instance})
        return self.instances[instance]


class LinkedBlockingMultiQueue:
    @inner
    class Node(Generic[T]):
        def __init__(self, item: T):
            self.item = item
            self.next: Optional[LinkedBlockingMultiQueue.Node] = None

    @inner
    class SubQueue(Generic[T]):
        def __init__(self, key: str):
            self.key: str = key
            self.priority_group: Optional[LinkedBlockingMultiQueue.PriorityGroup] = None
            self.put_lock = RLock()
            self.count = AtomicInteger()
            self.enabled: bool = True
            self.head: LinkedBlockingMultiQueue.Node[T] = LinkedBlockingMultiQueue.Node(
                None)
            self.last: Optional[LinkedBlockingMultiQueue.Node[T]] = self.head

        def clear(self) -> None:
            raise NotImplementedError("Intentionally do not support clear SubQueue.")

        def disable(self):
            self.fully_lock()
            try:
                if not self.enabled:
                    return None
                self.owner.total_count.dec(self.count.value)
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
                    self.owner.total_count.inc(c)
                    self.owner.not_empty.notify()

            finally:
                self.fully_unlock()

        def is_enabled(self) -> bool:
            self.owner.take_lock.acquire()
            try:
                return self.enabled
            finally:
                self.owner.take_lock.release()

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
            node = LinkedBlockingMultiQueue.Node(e)
            self.put_lock.acquire()
            try:
                self.enqueue(node)
                self.count.inc()
                if self.enabled:
                    old_size = self.owner.total_count.get_and_inc()
            finally:
                self.put_lock.release()

            if old_size == 0:
                self.owner._signal_not_empty()

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

        def unlink(self, trail: LinkedBlockingMultiQueue.Node,
                   next_: LinkedBlockingMultiQueue.Node) -> None:
            trail.item = None
            trail.next = next_.next
            if self.last == next_:
                self.last = trail
            if self.enabled:
                self.owner.total_count.get_and_dec()

        def fully_lock(self) -> None:
            self.put_lock.acquire()
            self.owner.take_lock.acquire()

        def fully_unlock(self) -> None:
            self.put_lock.release()
            self.owner.take_lock.release()

        def dequeue(self) -> T:
            assert self.size() > 0
            h = self.head
            first = h.next
            h.next = h
            self.head = first
            x = first.item
            first.item = None
            return x

    @inner
    class PriorityGroup(Generic[T]):
        def __init__(self, priority: int = 0):
            # non-negative number, the smaller number means higher priority.
            self.priority: int = priority
            self.queues: List[LinkedBlockingMultiQueue.SubQueue[T]] = list()
            self.next_idx: int = 0

        def add_queue(self, to_add: LinkedBlockingMultiQueue.SubQueue[T]) -> None:
            self.queues.append(to_add)
            to_add.priority_group = self

        def remove_queue(self, to_remove: LinkedBlockingMultiQueue.SubQueue[T]) -> None:

            for queue in self.queues:
                if queue.key == to_remove.key:
                    to_remove.put_lock.acquire()
                    try:
                        self.queues[:] = [q for q in self.queues if q != queue]
                        if self.next_idx == len(self.queues):
                            self.next_idx = 0
                        if queue.enabled:
                            self.owner.total_count.get_and_dec(to_remove.size())

                    finally:
                        to_remove.put_lock.release()

        def get_next_sub_queue(self) -> Optional[LinkedBlockingMultiQueue.SubQueue[T]]:
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

    @inner
    class DefaultSubQueueSelection(Generic[T]):
        def __init__(self,
                     priority_groups: List[LinkedBlockingMultiQueue.PriorityGroup]):
            self.priority_groups: List[
                LinkedBlockingMultiQueue.PriorityGroup[T]] = priority_groups

        def get_next(self) -> Optional[LinkedBlockingMultiQueue.SubQueue]:
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

    def __init__(self, ):
        self.take_lock = RLock()
        self.not_empty = Condition(self.take_lock)
        self.sub_queues = dict()  # thread-safe in CPython
        self.total_count = AtomicInteger()
        self.priority_groups: List[
            LinkedBlockingMultiQueue.PriorityGroup] = list()  # thread-safe in CPython
        self.sub_queue_selection = LinkedBlockingMultiQueue.DefaultSubQueueSelection(
            self.priority_groups)

    def put(self, key: str, item: T) -> None:
        """
        Put one item into the queue. Depending on the type, it will be put to the
        corresponding SubQueue.

        :param item: Any acceptable instance.
        :raises KeyError for unsupported type of item.
        :return: None
        """
        self._get_sub_queue(key).put(item)

    def get(self) -> T:
        """
        Blocking get the next available item from the queue.
        - Disabled SubQueues are considered empty and will not be fetched.
        - When multiple SubQueues are enabled and have items, it selects the SubQueue
        by the order specified by the self.sub_queue_selection strategy.

        :return: T, Any item that is available to the fetched.
        """
        self.take_lock.acquire()
        try:
            while self.total_count.value == 0:
                self.not_empty.wait()

            # at this point we know there is an element
            sub_queue = self.sub_queue_selection.get_next()
            item = sub_queue.dequeue()
            sub_queue.count.dec()
            if self.total_count.get_and_dec() > 1:
                # sub queue still has element
                self.not_empty.notify()
        finally:
            self.take_lock.release()

        return item

    def peek(self) -> Optional[T]:
        """
        Peek the next available item from the queue.
        - When no item is available, it returns None.
        - Otherwise, it acts the same as LinkedBlockingMultiQueue.get() but
        without actually taking the item out from the queue.

        :return: Optional[T], could be the available item or None.
        """
        self.take_lock.acquire()
        try:
            if self.total_count.value == 0:
                return None
            else:
                return self.sub_queue_selection.peek()
        finally:
            self.take_lock.release()

    def enable(self, key: str) -> None:
        """
        Enables a SubQueue, specified by key. This action acquires all locks.

        :param key: the key of the target SubQueue.
        :raises KeyError for unseen keys.
        :return: None
        """
        self._get_sub_queue(key).enable()

    def disable(self, key: str) -> None:
        """
        Disables a SubQueue, specified by key. This action acquires all locks.

        :param key: the key of the target SubQueue.
        :raises KeyError for unseen keys.
        :return: None
        """
        self._get_sub_queue(key).disable()

    def size(self, key: Optional[str] = None) -> int:
        """
        Get the current number of elements of the queue, or a specific SubQueue if
        key is provided. This action acquires NO locks.

        :param key: optional SubQueue key.
        :raises KeyError for unseen keys.
        :return: Integer for size.
        """
        if key is not None:
            return self._get_sub_queue(key).size()
        else:
            return self.total_count.value

    def is_empty(self, key: Optional[str] = None) -> bool:
        """
        Check if the queue is empty, or check a specific SubQueue if
        key is provided. This action acquires NO locks.

        :param key: optional SubQueue key.
        :raises KeyError for unseen keys.
        :return: Boolean representing empty or not.
        """
        return self.size(key) == 0

    def _add_sub_queue(self, key: str, priority: int) -> Optional[SubQueue]:
        """
        Create a new SubQueue if absent, with the key and priority.

        :param key: SubQueue key for future reference.
        :param priority: int value of priority, the lower number means the higher
        priority.
        :return: returns None if the key is new, or returns the previous SubQueue
        mapped by the key if key is repeated.
        """
        sub_queue = self.SubQueue(key)
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
                        new_pg = LinkedBlockingMultiQueue.PriorityGroup(priority)
                        new_pg.add_queue(sub_queue)
                        self.priority_groups.append(new_pg)
                        added = True
                        break

                    i += 1
                if not added:
                    new_pg = LinkedBlockingMultiQueue.PriorityGroup(priority)
                    new_pg.add_queue(sub_queue)
                    self.priority_groups.append(new_pg)

            return old_queue
        finally:
            self.take_lock.release()

    def _get_sub_queue(self, key: str) -> SubQueue:
        """
        Get the SubQueue specified by the key.

        :param key: SubQueue identification.
        :raises KeyError for unseen keys.
        :return: the SubQueue.
        """
        return self.sub_queues[key]

    def _signal_not_empty(self) -> None:
        """
        Notifies a (the next) consumer that the queue is not empty.
        Should only be invoked by the producer.

        :return: None
        """
        self.take_lock.acquire()
        try:
            self.not_empty.notify()
        finally:
            self.take_lock.release()


if __name__ == "__main__":
    # class A:
    #     @innerclass
    #     class B:
    #         def __repr__(self):
    #             return str(self.owner)
    #         def f(self):
    #             print(self.owner)
    #
    #     def __init__(self):
    #         self.b = A.B()
    #
    #
    # a = A()
    # print(a.b.__repr__())
    # a.b.f()
    class Outer(object):
        @inner
        class Inner(object):
            def print(self):
                print(self.owner)
        def __init__(self):
            self.i = None

        def add_i(self):
            self.i = self.Inner()

            # print(self.i.__repr__())
        def print(self):
            print(self.i)
            self.i.print()

    o1 = Outer()
    o1.add_i()
    o1.Inner().print()
    o1.i.print()
    # o1.print()
    # print(o1)
    # # i1 = o1.Inner()
    # # print(i1)
    # print(o1.i)
    # print(o1.i.owner)
