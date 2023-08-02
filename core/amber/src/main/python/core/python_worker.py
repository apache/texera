from overrides import overrides
from threading import Thread, Event, Condition
from loguru import logger

from core.models.internal_queue import InternalQueue
from core.runnables import MainLoop, NetworkReceiver, NetworkSender
from core.util.runnable.runnable import Runnable
from core.util.stoppable.stoppable import Stoppable

import os
import sys
import signal
import psutil
import pyarrow.flight as fl


def self_clean_child_process(code: int):
    current_process = psutil.Process()
    children = current_process.children(recursive=True)
    for child in children:
        if child.is_running():
            try:
                os.kill(child.pid, signal.SIGKILL)
            except Exception as e:
                logger.warning(
                    f"Exception during process termination PID {str(child.pid)}: {e} "
                )
    sys.exit(code)


class PythonWorker(Runnable, Stoppable):
    def __init__(self, worker_id: str, host: str, output_port: int):
        self._input_queue = InternalQueue()
        self._output_queue = InternalQueue()
        # start the server
        self._network_receiver = NetworkReceiver(self._input_queue, host=host)
        # let Java knows where Python starts (do handshake)
        self._network_sender = NetworkSender(
            self._output_queue,
            host=host,
            port=output_port,
            handshake_port=self._network_receiver.proxy_server.get_port_number(),
        )
        self.original_parent_pid = None
        self.server_address = f"grpc+tcp://{host}:{output_port}"

        self._main_loop = MainLoop(worker_id, self._input_queue, self._output_queue)
        self._network_receiver.register_shutdown(self.stop)
        self.condition = Condition()

    @overrides
    def run(self) -> None:
        stop_event = Event()
        network_sender_thread = Thread(
            target=self._network_sender.run, name="network_sender"
        )
        main_loop_thread = Thread(target=self._main_loop.run, name="main_loop_thread")
        self.original_parent_pid = os.getppid()

        # loop = asyncio.new_event_loop()
        heartbeat_thread = Thread(
            target=self.heartbeat,
            name="heartbeat_thread",
            args=(5.0, stop_event, network_sender_thread, main_loop_thread),
        )

        network_sender_thread.start()
        main_loop_thread.start()
        heartbeat_thread.start()
        main_loop_thread.join()
        network_sender_thread.join()
        with self.condition:
            self.condition.notify()

        heartbeat_thread.join()

    @overrides
    def stop(self):
        self._main_loop.stop()
        self._network_sender.stop()
        self_clean_child_process(0)

    def heartbeat(self, interval, stop_event, network_sender_thread, main_loop_thread):
        client = fl.FlightClient(self.server_address)
        while network_sender_thread.is_alive() and main_loop_thread.is_alive():
            try:
                # Send a heartbeat to the server
                client.do_action(fl.Action("heartbeat", b""))
            except Exception as e:
                logger.warning("Server is down with exception: " + str(e))
                stop_event.set()
                break
            with self.condition:
                self.condition.wait(interval)

        if stop_event.is_set():
            self._network_sender.stop()
            self._main_loop.stop()
            current_process = psutil.Process()
            children = current_process.children(recursive=True)
            for child in children:
                try:
                    os.kill(child.pid, signal.SIGKILL)
                except Exception as e:
                    logger.warning(
                        f"Exception during process termination PID {str(child.pid)}: {e} "
                    )

            os.kill(os.getpid(), signal.SIGTERM)
