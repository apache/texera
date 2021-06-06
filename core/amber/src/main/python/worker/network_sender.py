from queue import Queue
from typing import List

from pandas import Series, DataFrame
from pyarrow import Table

from python_rpc import RPCClient
from util.stoppable_queue_blocking_thread import StoppableQueueBlockingThread


class NetworkSender(StoppableQueueBlockingThread):
    def __init__(self, shared_queue: Queue, host: str, port: int):
        super().__init__(self.__class__.__name__, queue=shared_queue)
        self._rpc_client = RPCClient(host=host, port=port)
        self._output_queue = shared_queue

    def run(self):
        batch = list()
        while self.running():
            next_entry = self._output_queue.get()
            if next_entry == StoppableQueueBlockingThread.THREAD_STOP:
                break

            batch.append(next_entry.tuple)

            if len(batch) >= 1:
                self.send_batch(batch)

    def send_batch(self, batch: List[Series]):
        table = Table.from_pandas(DataFrame.from_records(batch))
        self._rpc_client.send_data(table)
