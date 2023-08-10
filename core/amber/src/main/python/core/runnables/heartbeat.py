from overrides import overrides
from threading import Event
from loguru import logger

from core.util.runnable.runnable import Runnable
from core.util.stoppable.stoppable import Stoppable

import os
import signal
import psutil
import socket
import urllib.parse


class Heartbeat(Runnable, Stoppable):
    def __init__(
        self,
        host: str,
        output_port: int,
        interval: float,
        event: Event,
    ):
        self._original_parent_pid = os.getppid()
        server_url = urllib.parse.urlparse(f"grpc+tcp://{host}:{output_port}")
        self._parsed_server_host = server_url.hostname
        self._parsed_server_port = server_url.port
        self._interval = interval
        self._stop_event = event

    @overrides
    def run(self) -> None:
        while not self._stop_event.wait(timeout=self._interval):
            alive = self._check_heartbeat()
            if not alive:
                # double check
                still_alive = self._check_heartbeat()

                if not still_alive:
                    parent_pid = os.getppid()
                    try:
                        parent_status = psutil.Process(
                            self._original_parent_pid
                        ).status()
                    except Exception:
                        parent_status = "NOT FOUND"

                    logger.warning(
                        f"Parent process PID {self._original_parent_pid} "
                        "runs unusually."
                        + (
                            f" Parent PID changed to {parent_pid}."
                            if parent_pid != self._original_parent_pid
                            else " Parent PID hasn't changed."
                        )
                        + f" Original parent process Status: {parent_status}"
                    )
                    self.stop()
                    return

        # If JVM crashed and main loop and network sender threads stop, we need
        # to add this line:
        # self.stop()

    def _check_heartbeat(self) -> bool:
        """
        Attempt to connect to JVM on the specific port. If succeeds, it means the
        socket is still available and the JVM is still alive. Otherwise, the JVM
        might have been gone.

        :return: bool, indicating if the socket is available.
        """
        try:
            temp_socket = socket.create_connection(
                (self._parsed_server_host, self._parsed_server_port), timeout=1
            )
            temp_socket.close()
            return True
        except Exception as e:
            logger.warning(f"Server is down with exception: {e}")
            return False

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
                        f"Exception during process termination "
                        f"PID {str(child.pid)}: {e} "
                    )
        os.kill(os.getpid(), signal.SIGTERM)
