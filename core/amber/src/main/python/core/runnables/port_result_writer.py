import queue
import threading
from typing import Dict

from core.models import Tuple
from core.storage.model.buffered_item_writer import BufferedItemWriter
from core.util import Stoppable
from core.util.runnable.runnable import Runnable
from proto.edu.uci.ics.amber.core import PortIdentity


class PortResultWriter(Runnable, Stoppable):
    def __init__(self, writer: BufferedItemWriter):
        self.writer: BufferedItemWriter = writer
        self.queue = queue.Queue()
        self.stopped = False
        self.stop_event = threading.Event()

    def put_tuple(self, amber_tuple: Tuple):
        assert not self.stopped, "Cannot put tuple after termination."
        self.queue.put(amber_tuple)

    def run(self) -> None:
        internal_stop = False
        while not internal_stop:
            queue_content = self.queue.get(block=True)
            if queue_content is None:
                internal_stop = True
            else:
                self.writer.put_one(queue_content)
        self.writer.close()
        self.stop_event.set()  # Signal that run() has fully stopped


    def stop(self) -> None:
        self.stopped = True
        self.queue.put(None)  # Signal termination
        self.stop_event.wait()  # Block until run() completes