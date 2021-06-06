from queue import Queue

from pyarrow import Table

from python_rpc import RPCServer
from worker.dp_thread import InputTuple
from worker.stoppable_queue_blocking_thread import StoppableQueueBlockingThread


class NetworkReceiver(StoppableQueueBlockingThread):
    def __init__(self, shared_queue: Queue, host: str, port: int):
        super().__init__(self.__class__.__name__, queue=shared_queue)
        self._rpc_server = RPCServer(host=host, port=port)

        def handler(batch: Table):
            for index, row in batch.to_pandas().iterrows():
                shared_queue.put(InputTuple(tuple=row))

        self._rpc_server.register_data_handler(handler)

    def register_shutdown(self, shutdown: callable) -> None:
        self._rpc_server.register("shutdown", shutdown)

    def run(self) -> None:
        self._rpc_server.serve()

    def stop(self):
        self._rpc_server.shutdown()
        self._rpc_server.wait()
        super(NetworkReceiver, self).stop()
