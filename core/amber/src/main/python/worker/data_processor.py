from queue import Queue

from util.stoppable_thread import StoppableThread
from worker import DPThread
from worker.threads.network_receiver import NetworkReceiver
from worker.threads.network_sender import NetworkSender
from worker.udf.udf_operator import UDFOperator


class DataProcessor(StoppableThread):
    def __init__(self, host: str, input_port: int, output_port: int, udf_operator: UDFOperator):
        super().__init__(f"{self.__class__.__name__}")

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

    def stop(self):
        self._dp_thread.stop()
        self._network_sender.stop()
        self._network_receiver.stop()
        super().stop()
