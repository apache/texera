from queue import Queue

from loguru import logger

from util.stoppable_thread import StoppableThread


class StoppableQueueBlockingThread(StoppableThread):
    """
    An implementation of StoppableThread, assuming the Thread.run() would be blocked
    by a blocking Queue.get(block=True, timeout=None).

    For example:
    ```
        def run(self) -> None:
            while True:
                entry = queue.get() # here is a blocking Queue.get()
                # do something with the entry
    ```

    According to https://docs.python.org/3/library/queue.html#queue.Queue.get, which
    quoted as: "Prior to 3.0 on POSIX systems, and for all versions on Windows, if
    block is true and timeout is None, this operation goes into an uninterruptible
    wait on an underlying lock."

    Currently, there is no other workaround for interrupting a waiting thread, safely.

    This implementation adds a special marker called
    `StoppableQueueBlockingThread.THREAD_STOP` into the queue, and when the marker is
    consumed, it should break the Thread.run().

    """
    THREAD_STOP = "__THREAD__STOP__MARKER__"

    def __init__(self, name: str, queue: Queue):
        super().__init__(name=name)
        self._internal_queue = queue

    @logger.catch
    def run(self):
        self.before_loop()
        try:
            while self.running():
                self.main_loop()
        except StoppableQueueBlockingThread.InterruptThread:
            # surpassed the expected interruption
            logger.debug(f"{self.name}-interrupting")
        finally:
            self.after_loop()

    def main_loop(self) -> None:
        pass

    def before_loop(self) -> None:
        pass

    def after_loop(self) -> None:
        pass

    def stop(self):
        self._internal_queue.put(StoppableQueueBlockingThread.THREAD_STOP)
        super().stop()

    def interruptible_get(self):
        next_entry = self._internal_queue.get()
        if next_entry == StoppableQueueBlockingThread.THREAD_STOP:
            raise StoppableQueueBlockingThread.InterruptThread
        return next_entry

    class InterruptThread(BaseException):
        """
        Used to interrupt a thread.
        """
