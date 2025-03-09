from dataclasses import dataclass

from overrides import overrides

from core.models import Tuple
from core.storage.model.buffered_item_writer import BufferedItemWriter
from core.util import StoppableQueueBlockingRunnable, IQueue
from core.util.customized_queue.queue_base import QueueElement


@dataclass
class PortStorageWriterElement(QueueElement):
    data_tuple: Tuple


class PortStorageWriter(StoppableQueueBlockingRunnable):
    def __init__(self, buffered_item_writer: BufferedItemWriter, queue: IQueue):
        super().__init__(name=self.__class__.__name__, queue=queue)
        self.buffered_item_writer: BufferedItemWriter = buffered_item_writer

    @overrides
    def receive(self, next_entry: QueueElement) -> None:
        if isinstance(next_entry, PortStorageWriterElement):
            self.buffered_item_writer.put_one(next_entry.data_tuple)
        else:
            raise TypeError(f"Unexpected entry {next_entry}")

    @overrides
    def pre_start(self) -> None:
        self.buffered_item_writer.open()

    @overrides
    def post_stop(self) -> None:
        self.buffered_item_writer.close()
