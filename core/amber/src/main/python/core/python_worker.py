from threading import Thread

from overrides import overrides

from core.models.internal_queue import InternalQueue
from core.runnables import DataProcessor, NetworkReceiver, NetworkSender
from core.util.runnable.runnable import Runnable
from core.util.stoppable.stoppable import Stoppable


class PythonWorker(Runnable, Stoppable):
    def __init__(self, host: str, input_port: int, output_port: int):
        self._input_queue = InternalQueue()
        self._output_queue = InternalQueue()
        self._network_receiver = NetworkReceiver(
            self._input_queue, host=host, port=input_port
        )
        self._network_sender = NetworkSender(
            self._output_queue, host=host, port=output_port
        )
        self._data_processor = DataProcessor(self._input_queue, self._output_queue)
        self._network_receiver.register_shutdown(self.stop)

    @overrides
    def run(self) -> None:
        network_receiver_thread = Thread(
            target=self._network_receiver.run, name="network_receiver"
        )
        network_sender_thread = Thread(
            target=self._network_sender.run, name="network_sender"
        )
        dp_thread = Thread(target=self._data_processor.run, name="dp_thread")

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
