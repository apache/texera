from pandas import DataFrame
from pyarrow import Table

from python_rpc import RPCClient
from worker.architecture.messaginglayer.tuple_to_batch_converter import TupleToBatchConverter
from worker.models.generated.virtualidentity_pb2 import ActorVirtualIdentity
from worker.models.internal_queue import InternalQueue
from worker.models.payload import DataPayload
from worker.models.tuple import ITuple
from worker.util.stoppable_queue_blocking_thread import StoppableQueueBlockingThread


class NetworkSender(StoppableQueueBlockingThread):
    def __init__(self, shared_queue: InternalQueue, host: str, port: int):
        super().__init__(self.__class__.__name__, queue=shared_queue)
        self._rpc_client = RPCClient(host=host, port=port)
        self._batch = list()
        self._tuple_to_batch_converter = TupleToBatchConverter()

    def main_loop(self):
        next_entry = self.interruptible_get()
        self.pass_tuple_to_downstream(next_entry.tuple)

    def pass_tuple_to_downstream(self, tuple_: ITuple):
        for to, batch in self._tuple_to_batch_converter.tuple_to_batch(tuple_):
            self.send_batch(to, batch)

    def send_batch(self, to: ActorVirtualIdentity, batch: DataPayload) -> None:
        # TODO: add to information to the batch
        table = Table.from_pandas(DataFrame.from_records(batch))
        self._rpc_client.send_data(table)
