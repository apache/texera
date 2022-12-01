from threading import Thread

from overrides import overrides

from core.models.internal_queue import InternalQueue
from core.runnables import MainLoop, NetworkReceiver, NetworkSender
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
        self._main_loop = MainLoop(self._input_queue, self._output_queue)
        self._network_receiver.register_shutdown(self.stop)

    @overrides
    def run(self) -> None:
        network_sender_thread = Thread(
            target=self._network_sender.run, name="network_sender"
        )
        main_loop_thread = Thread(target=self._main_loop.run, name="main_loop_thread")

        network_sender_thread.start()
        main_loop_thread.start()
        main_loop_thread.join()
        network_sender_thread.join()

    @overrides
    def stop(self):
        self._main_loop.stop()
        self._network_sender.stop()
