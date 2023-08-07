from overrides import overrides
from threading import Thread, Event, Condition
from loguru import logger

from core.util.runnable.runnable import Runnable
from core.util.stoppable.stoppable import Stoppable

import os
import signal
import psutil
import socket
import urllib.parse


class Heartbeat(Runnable, Stoppable):
    def __init__(self, host: str, output_port: int, condition: Condition):
        self._original_parent_pid = os.getppid()
        server_url = urllib.parse.urlparse(f"grpc+tcp://{host}:{output_port}")
        self._parsed_server_host = server_url.hostname
        self._parsed_server_port = server_url.port
        self._unexpected_stop = False
        self._heartbeat_wakeup_condition = condition

    @overrides
    def run(self) -> None:
        pass

    def start_heartbeat(
        self, interval, network_sender_thread, main_loop_thread
    ) -> None:
        while network_sender_thread.is_alive() and main_loop_thread.is_alive():
            try:
                # try to connect to the server via socket
                temp_socket = socket.create_connection(
                    (self._parsed_server_host, self._parsed_server_port), timeout=1
                )
                temp_socket.close()
            except Exception as e:
                logger.warning(f"Server is down with exception: {e}")
                self._unexpected_stop = True
                break
            with self._heartbeat_wakeup_condition:
                self._heartbeat_wakeup_condition.wait(interval)
        try:
            socket.create_connection(
                (self._parsed_server_host, self._parsed_server_port), timeout=1
            )
        except Exception as e:
            logger.warning(f"Server is down with exception: {e}")
            self._unexpected_stop = True

        if self._unexpected_stop:
            parent_pid = os.getppid()
            parent_status = "NOT FOUND"
            try:
                parent_status = psutil.Process(self._original_parent_pid).status()
            except Exception:
                pass
            if parent_pid != self._original_parent_pid:
                logger.info(
                    f"Parent process PID {self._original_parent_pid} runs unusually."
                    f" Parent PID changed to {parent_pid}."
                    f" Original parent process Status: {parent_status}"
                )
            else:
                logger.warning(
                    f"Parent process PID {self._original_parent_pid} runs unusually."
                    f" Parent PID hasn't changed."
                    f" Original parent process Status: {parent_status}"
                )
            self.stop()

    @overrides
    def stop(self):
        # clean up every process under the python worker
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
        os.kill(os.getpid(), signal.SIGTERM)
