import queue
import threading
from typing import T

from overrides import overrides

from core.util.customized_queue.queue_base import IQueue


class DoubleBlockingQueue(IQueue):
    """
    Supports for multi producers + single consumer.
    """

    def __init__(self, *sub_types: type):
        self._input_queue = queue.Queue()
        self._main_queue = queue.Queue()
        self._sub_queue = queue.Queue()
        self._sub_types = sub_types
        self._sub_enabled = True
        self._consumer_id = None

    def disable_sub(self) -> None:
        """
        Invoked by the consumer only, to disable sub queue elements from emitting.
        """
        self._enforce_single_consumer()
        self._sub_enabled = False

    def enable_sub(self) -> None:
        """
        Invoked by the consumer only, to enable sub queue to emit elements.
        """
        self._enforce_single_consumer()
        self._sub_enabled = True

    @overrides
    def empty(self) -> bool:
        """
        Invoked by the consumer only, checks if the queue is empty.
        :return: True if the main queue is empty, and the enabled sub queue is empty
            as well.
        """
        self._enforce_single_consumer()
        if self._sub_enabled:
            return self.main_empty() and self.sub_empty()
        else:
            return self.main_empty()

    @overrides
    def get(self) -> T:
        """
        Invoked by the consumer only, blocking get the next item, could be either from
        the main queue or the enabled sub queue.
        :return: any type
        """
        self._enforce_single_consumer()
        while True:
            if not self._main_queue.empty():
                return self._main_queue.get()
            elif self._sub_enabled and not self._sub_queue.empty():
                return self._sub_queue.get()
            else:
                self._distribute_next()

    @overrides
    def put(self, item: T) -> None:
        """
        Enqueue an item.
        :param item: any type
        """
        self._input_queue.put(item)

    def main_empty(self) -> bool:
        """
        Invoked by the consumer only, checks if the main queue is empty.
        :return: True if the main queue is empty.
        """
        return self.main_size() == 0

    def sub_empty(self) -> bool:
        """
        Invoked by the consumer only, checks if the sub queue is empty.
        :return: True if the sub queue is empty.
        """
        return self.sub_size() == 0

    def main_size(self):
        """
        Invoked by the consumer only, returns the main queue's size.
        :return: size, int
        """
        self._enforce_single_consumer()
        self._distribute_all()
        return self._main_queue.qsize()

    def sub_size(self):
        """
        Invoked by the consumer only, returns the sub queue's size.
        :return: size, int
        """
        self._enforce_single_consumer()
        self._distribute_all()
        return self._sub_queue.qsize()

    def _distribute_all(self) -> None:
        """
        Redistribute the items in the input queue into either main queue or the sub
        queue accordingly.
        """
        while not self._input_queue.empty():
            self._distribute_next()

    def _distribute_next(self) -> None:
        """
        Redistribute the next item from input queue into either main queue or the sub
        queue accordingly.
        :return:
        """
        ele = self._input_queue.get()
        if isinstance(ele, self._sub_types):
            self._sub_queue.put(ele)
        else:
            self._main_queue.put(ele)

    def _enforce_single_consumer(self) -> None:
        """
        Raises an AssertionError if multiple consumers are detected.
        :return:
        """
        try:
            # new unique identifier in python 3.8.
            get_id = threading.get_native_id
        except Exception:
            # fall back to old ident method for python prior to 3.8.
            get_id = threading.get_ident
        if self._consumer_id is None:
            self._consumer_id = get_id()
        else:
            assert self._consumer_id == get_id(), (
                "DoubleBlockingQueue can only have one consumer! "
                f"{self._consumer_id} vs {get_id()}"
            )
