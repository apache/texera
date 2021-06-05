from queue import Queue

from pyarrow import Table

from python_rpc import RPCServer
from worker.data_processor import InputTuple
from worker.stoppable_thread import StoppableThread


class NetworkReceiver(StoppableThread):
    def __init__(self, shared_queue: Queue, host: str, port: int):
        super().__init__(self.__class__.__name__)
        self._rpc_server = RPCServer(host=host, port=port)

        def handler(batch: Table):
            for index, row in batch.to_pandas().iterrows():
                shared_queue.put(InputTuple(tuple=row))

        self._rpc_server.register_data_handler(handler)

        self._shared_queue = shared_queue

    def run(self) -> None:
        self._rpc_server.serve()

    def stop(self):
        self._rpc_server.shutdown()
        super(NetworkReceiver, self).stop()
