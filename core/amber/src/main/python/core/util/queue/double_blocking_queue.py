import queue
from typing import T

from overrides import overrides

from core.util.queue.queue_base import IQueue


class DoubleBlockingQueue(IQueue):

    def __init__(self, *sub_types: type):
        super().__init__()
        self._input_queue = queue.Queue()
        self._main_queue = queue.Queue()
        self._sub_queue = queue.Queue()
        self._sub_types = sub_types
        self._sub_enabled = True

    def disable_sub(self) -> None:
        self._sub_enabled = False

    @overrides
    def empty(self) -> bool:

        if self._sub_enabled:
            return self._main_empty() and self._sub_empty()
        else:
            return self._main_empty()

    def enable_sub(self) -> None:
        self._sub_enabled = True

    @overrides
    def get(self):
        while True:
            if self._sub_enabled:
                if self._main_queue.empty():
                    if not self._sub_queue.empty():
                        return self._sub_queue.get()
                    else:
                        return self._input_queue.get()
                else:
                    return self._main_queue.get()
            else:
                if self._main_queue.empty():
                    self._distribute()
                else:
                    return self._main_queue.get()

    @overrides
    def put(self, item: T) -> None:
        self._input_queue.put(item)

    def _main_empty(self) -> bool:
        self._distribute()
        return self._main_queue.qsize() == 0

    def _sub_empty(self) -> bool:
        return self._sub_queue.qsize() == 0

    def _distribute(self) -> None:
        while not self._input_queue.empty():
            ele = self._input_queue.get()
            if isinstance(ele, self._sub_types):
                self._sub_queue.put(ele)
            else:
                self._main_queue.put(ele)
                break
