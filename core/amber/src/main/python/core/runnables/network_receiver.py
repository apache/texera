from loguru import logger
from overrides import overrides
from pyarrow.lib import Table

from core.models import (
    ControlElement,
    DataElement,
    InputDataFrame,
    EndOfUpstream,
    InternalQueue,
)
from core.proxy import ProxyServer
from core.util import Stoppable
from core.util.runnable.runnable import Runnable
from proto.edu.uci.ics.amber.engine.common import PythonControlMessage, PythonDataHeader


class NetworkReceiver(Runnable, Stoppable):
    """
    Receive and deserialize messages.
    """

    def __init__(self, shared_queue: InternalQueue, host: str, port: int):
        self._proxy_server = ProxyServer(host=host, port=port)

        # register the data handler to deserialize data messages.
        @logger.catch(reraise=True)
        def data_handler(command: bytes, table: Table):
            data_header = PythonDataHeader().parse(command)
            if not data_header.is_end:
                shared_queue.put(
                    DataElement(tag=data_header.tag, payload=InputDataFrame(table))
                )
            else:
                shared_queue.put(
                    DataElement(tag=data_header.tag, payload=EndOfUpstream())
                )

        self._proxy_server.register_data_handler(data_handler)

        # register the control handler to deserialize control messages.
        @logger.catch(reraise=True)
        def control_handler(message: bytes):
            print("received", message)
            python_control_message = PythonControlMessage().parse(message)
            shared_queue.put(
                ControlElement(
                    tag=python_control_message.tag,
                    payload=python_control_message.payload,
                )
            )

        self._proxy_server.register_control_handler(control_handler)

    def register_shutdown(self, shutdown: callable) -> None:
        self._proxy_server.register(
            name="shutdown", action=ProxyServer.ack(msg="Bye bye!")(shutdown)
        )

    @logger.catch(reraise=True)
    @overrides
    def run(self) -> None:
        logger.debug("started running!!!")
        self._proxy_server.serve()

    @logger.catch(reraise=True)
    @overrides
    def stop(self):
        self._proxy_server.shutdown()
        self._proxy_server.wait()
