import typing

from core.models import Tuple, InternalQueue, DataFrame, Table, MarkerFrame
from core.models.internal_queue import DataElement
from core.models.marker import StartOfInputChannel, EndOfInputChannel
from core.storage.document_factory import DocumentFactory
from core.util import Stoppable
from core.util.runnable.runnable import Runnable
from proto.edu.uci.ics.amber.core import ActorVirtualIdentity, ChannelIdentity

MATERIALIZATION_READER_ACTOR_PREFIX = "MATERIALIZATION_READER_"

def get_from_actor_id_for_input_port_storage(storage_uri_str: str) -> ActorVirtualIdentity:
    """
    Constructs an ActorVirtualIdentity for input port storage.

    Args:
        storage_uri_str (str): The string representation of the storage URI.

    Returns:
        ActorVirtualIdentity: A new virtual identity created by prefixing the storage URI.
    """
    return ActorVirtualIdentity(MATERIALIZATION_READER_ACTOR_PREFIX + storage_uri_str)


class InputPortMaterializationReaderThread(Runnable, Stoppable):
    def __init__(self, uri: str, queue: InternalQueue, worker_actor_id: ActorVirtualIdentity, batch_size: int):
        """
        Args:
            uri (str): The URI of the materialized document.
            queue: An instance of IQueue where messages are enqueued.
            worker_actor_id (ActorVirtualIdentity): The target worker actor's identity.
            batch_size (int): The batch size for flushing tuples.
        """
        self.uri = uri
        self.queue = queue
        self.worker_actor_id = worker_actor_id
        self.batch_size = batch_size
        self.sequence_number = 0  # Counter to mimic AtomicLong behavior.
        self.buffer = []  # Buffer for Tuple objects.
        self.channel_id = None  # Will be assigned upon run.
        self._stopped = False
        self.materialization = None
        self.tuple_schema = None

    def run(self) -> None:
        """
        Main execution method that reads tuples from the materialized document and
        enqueues them in batches. It first emits a start marker and, when finished,
        emits an end marker.
        """
        # Setup a unique channel identity.
        from_actor_id = get_from_actor_id_for_input_port_storage(self.uri)
        self.channel_id = ChannelIdentity(from_actor_id, self.worker_actor_id, is_control=False)

        # Emit start marker.
        self.emit_marker(StartOfInputChannel())

        try:
            # Open the document and obtain an iterator over the tuples.
            self.materialization, self.tuple_schema = DocumentFactory.open_document(self.uri)
            storage_iterator = self.materialization.get()  # Iterator over Tuple objects.

            # Iterate and process tuples.
            for tup in storage_iterator:
                if self._stopped:
                    break
                self.buffer.append(tup)
                if len(self.buffer) >= self.batch_size:
                    self.flush()

            # Flush any remaining tuples.
            if self.buffer:
                self.flush()
        finally:
            # Emit end marker.
            self.emit_marker(EndOfInputChannel())

    def stop(self):
        """Sets the stop flag so the run loop may terminate."""
        self._stopped = True

    def flush(self) -> None:
        """
        Flush the current batch of tuples in the buffer, wrapping them in a DataFrame
        and sending them as a WorkflowFIFOMessage.
        """
        if not self.buffer:
            return
        data_payload = self.tuple_to_frame(self.buffer)
        queue_element = DataElement(
            tag=self.channel_id,
            payload=data_payload,
        )
        self.queue.put(queue_element)
        self.buffer.clear()

    def emit_marker(self, marker) -> None:
        """
        Emit a marker (for example, StartOfInputChannel or EndOfInputChannel)
        by first flushing the current data and then sending the marker message.
        """
        self.flush()
        marker_payload = MarkerFrame(marker)
        queue_element = DataElement(
            tag=self.channel_id,
            payload=marker_payload,
        )
        self.queue.put(queue_element)
        self.flush()

    def get_sequence_number(self) -> int:
        """
        Returns a monotonically increasing sequence number.
        """
        curr = self.sequence_number
        self.sequence_number += 1
        return curr

    def tuple_to_frame(self, tuples: typing.List[Tuple]) -> DataFrame:
        return DataFrame(
            frame=Table.from_pydict(
                {
                    name: [t[name] for t in tuples]
                    for name in self.tuple_schema.get_attr_names()
                },
                schema=self.tuple_schema.as_arrow_schema(),
            )
        )
