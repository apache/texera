from threading import Thread
from typing import Iterable

from loguru import logger
from overrides import overrides

from core.models import Tuple
from core.models.internal_queue import InternalQueue
from core.runnables import DataProcessor, NetworkReceiver, NetworkSender
from core.udf.udf_operator import UDFOperator
from core.util.runnable.runnable import Runnable
from core.util.stoppable.stoppable import Stoppable


class PythonWorker(Runnable, Stoppable):
    def __init__(self, host: str, input_port: int, output_port: int, udf_operator: UDFOperator):
        self._input_queue = InternalQueue()
        self._output_queue = InternalQueue()
        schema_map = dict()
        self._network_receiver = NetworkReceiver(self._input_queue, host=host, port=input_port, schema_map=schema_map)
        self._network_sender = NetworkSender(self._output_queue, host=host, port=output_port, schema_map=schema_map)
        self._data_processor = DataProcessor(self._input_queue, self._output_queue, udf_operator)
        self._network_receiver.register_shutdown(self.stop)

    @overrides
    def run(self) -> None:
        network_receiver_thread = Thread(target=self._network_receiver.run)
        network_sender_thread = Thread(target=self._network_sender.run)
        dp_thread = Thread(target=self._data_processor.run)

        network_receiver_thread.start()
        network_sender_thread.start()
        dp_thread.start()
        dp_thread.join()
        network_sender_thread.join()
        network_receiver_thread.join()

    @overrides
    def stop(self):
        self._data_processor.stop()
        self._network_sender.stop()
        self._network_receiver.stop()

