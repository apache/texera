from queue import Queue
from typing import List

from pandas import Series, DataFrame
from pyarrow import Table

from python_rpc import RPCClient
from worker.util.stoppable_queue_blocking_thread import StoppableQueueBlockingThread


class NetworkSender(StoppableQueueBlockingThread):
    def __init__(self, shared_queue: Queue, host: str, port: int):
        super().__init__(self.__class__.__name__, queue=shared_queue)
        self._rpc_client = RPCClient(host=host, port=port)
        self._batch = list()

    def main_loop(self):
        next_entry = self.interruptible_get()
        self._batch.append(next_entry.tuple)

        if len(self._batch) >= 1:
            self.send_batch(self._batch)
            self._batch.clear()

    def send_batch(self, batch: List[Series]):
        table = Table.from_pandas(DataFrame.from_records(batch))
        self._rpc_client.send_data(table)
