import queue
from typing import Dict

from core.models import Tuple
from core.storage.model.buffered_item_writer import BufferedItemWriter
from core.util import Stoppable
from core.util.runnable.runnable import Runnable
from proto.edu.uci.ics.amber.core import PortIdentity


class PortResultWriter(Runnable, Stoppable):
    def __init__(self):
        self.writer_map: Dict[PortIdentity, BufferedItemWriter] = {}
        self.queue = queue.Queue()
        self.stopped = False

    def add_writer(self, location: PortIdentity, writer: BufferedItemWriter):
        self.writer_map[location] = writer

    def put_tuple(self, location: PortIdentity, amber_tuple: Tuple):
        assert not self.stopped, "Cannot put tuple after termination."
        self.queue.put((location, amber_tuple))

    def run(self) -> None:
        internal_stop = False
        while not internal_stop:
            queue_content = self.queue.get(block=True)
            if queue_content is None:
                internal_stop = True
            else:
                location, tuple_data = queue_content
                if location in self.writer_map:
                    self.writer_map[location].put_one(tuple_data)

        for writer in self.writer_map.values():
            writer.close()


    def stop(self) -> None:
        self.stopped = True
        self.queue.put(None)  # Signal termination