from queue import Queue

from loguru import logger

from worker import DPThread
from worker.network_receiver import NetworkReceiver
from worker.network_sender import NetworkSender
from worker.stoppable_thread import StoppableThread
from worker.udf_operator import UDFOperator


class DataProcessor(StoppableThread):
    def __init__(self, id: int, host: str, input_port: int, output_port: int, udf_operator: UDFOperator):
        super().__init__(f"{self.__class__.__name__}-{id}")

        self._input_queue = Queue()
        self._output_queue = Queue()
        self._network_receiver = NetworkReceiver(self._input_queue, host=host, port=input_port)
        self._network_sender = NetworkSender(self._output_queue, host=host, port=output_port)

        self._dp_thread = DPThread(self._input_queue, self._output_queue, udf_operator)

        self._network_receiver.register_shutdown(self.stop)

    def run(self) -> None:
        self._network_receiver.start()
        self._network_sender.start()
        self._dp_thread.start()
        self._dp_thread.join()
        self._network_sender.join()
        self._network_receiver.join()

    def on_receive(self, msg, ):
        pass

    def stop(self):
        logger.debug(f"{self.name}-stopping")
        self._dp_thread.stop()
        self._network_sender.stop()
        self._network_receiver.stop()
