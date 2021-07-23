from typing import Dict

from loguru import logger
from overrides import overrides
from pyarrow.lib import Table

from core.models import ControlElement, DataElement, DataFrame, EndOfUpstream, InternalQueue
from core.proxy import ProxyServer
from core.util import Stoppable
from core.util.runnable.runnable import Runnable
from proto.edu.uci.ics.amber.engine.common import PythonControlMessage, PythonDataHeader


class NetworkReceiver(Runnable, Stoppable):
    def __init__(self, shared_queue: InternalQueue, host: str, port: int, schema_map: Dict[str, type]):
        super().__init__()
        self._proxy_server = ProxyServer(host=host, port=port)

        def data_handler(command: bytes, table: Table):

            data_header = PythonDataHeader().parse(command)
            logger.debug(f"getting {data_header}, table {table}")
            if not data_header.end:
                input_schema = table.schema
                # record input schema
                for field in input_schema:
                    schema_map[field.name] = field
                shared_queue.put(DataElement(
                    tag=data_header.from_,
                    payload=DataFrame([row for _, row in table.to_pandas().iterrows()])
                ))
            else:
                shared_queue.put(DataElement(tag=data_header.from_, payload=EndOfUpstream()))

        self._proxy_server.register_data_handler(data_handler)

        def control_handler(message: bytes):
            python_control_message = PythonControlMessage().parse(message)
            shared_queue.put(ControlElement(tag=python_control_message.from_, payload=python_control_message.payload))

        self._proxy_server.register_control_handler(control_handler)

    def register_shutdown(self, shutdown: callable) -> None:
        self._proxy_server.register("shutdown", ProxyServer.ack(msg="Bye bye!")(shutdown))

    @logger.catch
    def run(self) -> None:
        self._proxy_server.serve()

    @overrides
    def stop(self):
        self._proxy_server.shutdown()
        self._proxy_server.wait()
