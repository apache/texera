from queue import Queue

from worker.stoppable_thread import StoppableThread


class StoppableQueueBlockingThread(StoppableThread):
    THREAD_STOP = "__THREAD__STOP__MARK__"

    def __init__(self, name: str, queue: Queue):
        super().__init__(name=name)
        self._internal_queue = queue

    def stop(self):
        self._internal_queue.put(StoppableQueueBlockingThread.THREAD_STOP)
        super().stop()
