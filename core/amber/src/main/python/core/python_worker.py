from overrides import overrides
from threading import Thread, Event
from loguru import logger

from core.models.internal_queue import InternalQueue
from core.runnables import MainLoop, NetworkReceiver, NetworkSender
from core.util.runnable.runnable import Runnable
from core.util.stoppable.stoppable import Stoppable

import os
import sys
import signal
import psutil
import asyncio


def self_clean_child_process(code: int):
    current_process = psutil.Process()
    children = current_process.children(recursive=True)
    for child in children:
        if child.is_running():
            try:
                os.kill(child.pid, signal.SIGKILL)
            except Exception as e:
                logger.info(
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
        self.original_parent_pid = 1

        self._main_loop = MainLoop(worker_id, self._input_queue, self._output_queue)
        self._network_receiver.register_shutdown(self.stop)

    @overrides
    def run(self) -> None:
        network_sender_thread = Thread(
            target=self._network_sender.run, name="network_sender"
        )
        main_loop_thread = Thread(target=self._main_loop.run, name="main_loop_thread")
        self.original_parent_pid = os.getppid()

        loop = asyncio.new_event_loop()
        heartbeat_thread = Thread(
            target=self.start_asyncio_loop, name="heartbeat_thread", args=(loop, 5.0)
        )

        heartbeat_thread.start()
        network_sender_thread.start()
        main_loop_thread.start()
        main_loop_thread.join()
        network_sender_thread.join()
        heartbeat_thread.join()

    @overrides
    def stop(self):
        self._main_loop.stop()
        self._network_sender.stop()
        self_clean_child_process(0)

    def start_asyncio_loop(self, loop, interval):
        stop_event = Event()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(
                self._network_sender.heartbeat(interval, stop_event)
            )
        except Exception as e:
            logger.info(f"Heartbeat failed with exception: {e}")
        finally:
            if stop_event.is_set():
                parent_pid = os.getppid()
                parent_status = "NOT FOUND"
                try:
                    parent_status = psutil.Process(self.original_parent_pid).status()
                except Exception:
                    pass
                if parent_pid != self.original_parent_pid:
                    logger.info(
                        f"Parent process PID {self.original_parent_pid} runs unusually."
                        f" Parent PID changed to {parent_pid}."
                        f" Original parent process Status: {parent_status}"
                    )
                else:
                    logger.info(
                        f"Parent process PID {self.original_parent_pid} runs unusually."
                        f" Parent PID hasn't changed."
                        f" Original parent process Status: {parent_status}"
                    )
                self.stop()
