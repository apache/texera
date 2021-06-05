from queue import Queue

from loguru import logger

from worker import DataProcessor
from worker.network_receiver import NetworkReceiver
from worker.network_sender import NetworkSender
from worker.stoppable_thread import StoppableThread


class Actor(StoppableThread):
    def __init__(self, id: int, host: str, input_port: int, output_port: int):
        super().__init__(f"{self.__class__.__name__}-{id}")
        self._input_queue = Queue()
        self._output_queue = Queue()
        self._network_receiver_thread = NetworkReceiver(self._input_queue, host=host, port=input_port)
        self._network_sender_thread = NetworkSender(self._output_queue, host=host, port=output_port)
        self._data_processor = DataProcessor(self._input_queue, self._output_queue)

    def run(self) -> None:
        self._network_receiver_thread.start()
        self._network_sender_thread.start()
        self._data_processor.start()
        while self.running():
            pass

        self._data_processor.join()
        self._network_sender_thread.join()
        self._network_receiver_thread.join()

    def stop(self):
        logger.debug(f"{self.name}-stopping")
        self._data_processor.stop()
        self._network_sender_thread.stop()
        self._network_receiver_thread.stop()
        super(Actor, self).stop()
