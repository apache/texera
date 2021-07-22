from loguru import logger
from overrides import overrides

from core.util.customized_queue.queue_base import IQueue
from core.util.runnable.runnable import Runnable
from core.util.stoppable.stoppable import Stoppable


class StoppableQueueBlockingRunnable(Runnable, Stoppable):
    """
    An implementation of Stoppable, assuming the Thread.run() would be blocked
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

    Currently, there is no other workaround for interrupting a waiting stoppable, safely.

    This implementation adds a special marker called
    `StoppableQueueBlockingThread.THREAD_STOP` into the queue, and when the marker is
    consumed, it should break the Thread.run().

    """
    THREAD_STOP = IQueue.QueueControl(msg="__THREAD__STOP__MARKER__")

    def __init__(self, name: str, queue: IQueue):
        super().__init__()
        self._internal_queue = queue
        self.name = name

    @logger.catch
    @overrides
    def run(self):
        self.pre_start()
        try:
            while True:
                self.receive(self.interruptible_get())
        except StoppableQueueBlockingRunnable.InterruptThread:
            # surpassed the expected interruption
            logger.debug(f"{self.name}-interrupting")
        finally:
            self.post_stop()

    def receive(self, next_entry: IQueue.QueueElement):
        pass

    def pre_start(self) -> None:
        pass

    def post_stop(self) -> None:
        pass

    @overrides
    def stop(self):
        self._internal_queue.put(StoppableQueueBlockingRunnable.THREAD_STOP)

    def interruptible_get(self):
        next_entry = self._internal_queue.get()
        if next_entry == StoppableQueueBlockingRunnable.THREAD_STOP:
            raise StoppableQueueBlockingRunnable.InterruptThread
        return next_entry

    class InterruptThread(BaseException):
        """
        Used to interrupt a stoppable.
        """
