from queue import Queue

from loguru import logger
from pyarrow import Table

from python_rpc import RPCServer
from util import StoppableThread
from worker.threads.dp_thread import InputTuple


class NetworkReceiver(StoppableThread):
    def __init__(self, shared_queue: Queue, host: str, port: int):
        super().__init__(self.__class__.__name__)
        self._rpc_server = RPCServer(host=host, port=port)

        def handler(batch: Table):
            for index, row in batch.to_pandas().iterrows():
                shared_queue.put(InputTuple(tuple=row))

        self._rpc_server.register_data_handler(handler)

    def register_shutdown(self, shutdown: callable) -> None:
        self._rpc_server.register("shutdown", shutdown)

    @logger.catch
    def run(self) -> None:
        self._rpc_server.serve()

    def stop(self):
        self._rpc_server.shutdown()
        self._rpc_server.wait()
        super(NetworkReceiver, self).stop()
