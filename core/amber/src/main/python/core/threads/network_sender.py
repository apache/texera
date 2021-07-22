import pandas
from loguru import logger
from pyarrow import Table
from pyarrow.lib import Schema, schema

from core.models import *
from core.proxy import ProxyClient
from core.util import StoppableQueueBlockingThread
from proto.edu.uci.ics.amber.engine.common import *


class NetworkSender(StoppableQueueBlockingThread):
    def __init__(self, shared_queue: InternalQueue, host: str, port: int, schema_map: dict[str, type]):
        super().__init__(self.__class__.__name__, queue=shared_queue)
        self._proxy_client = ProxyClient(host=host, port=port)
        self._batch = list()
        self.schema_map = schema_map

    def receive(self, next_entry: InternalQueueElement):
        logger.debug(f"Sender receive a new entry {next_entry}")
        if isinstance(next_entry, OutputDataElement):
            self.send_data(next_entry.to, next_entry.payload)
        elif isinstance(next_entry, ControlElement):
            self.send_control(next_entry.from_, next_entry.cmd)

        # TODO: handle else

    def send_data(self, to: ActorVirtualIdentity, data_payload: DataPayload) -> None:
        if isinstance(data_payload, DataFrame):
            df = pandas.DataFrame.from_records(data_payload.frame)
            inferred_schema: Schema = Schema.from_pandas(df)
            # create a output schema, use the original input schema if possible
            output_schema = schema([self.schema_map.get(field.name, field) for field in inferred_schema])
            data_header = PythonDataHeader(from_=to, end=10)
            logger.debug(f"prepared data_header {data_header}")
            logger.debug(f"reparsed data_header {PythonDataHeader().parse(bytes(data_header))}")
            table = Table.from_pandas(df, output_schema)
            self._proxy_client.send_data(bytes(data_header), table)

        elif isinstance(data_payload, EndOfUpstream):
            data_header = PythonDataHeader(from_=to, end=11)
            self._proxy_client.send_data(bytes(data_header), None)

        # TODO: handle else

    def send_control(self, to: ActorVirtualIdentity, cmd: ControlPayloadV2):
        python_control_message = PythonControlMessage(from_=to, payload=cmd)
        self._proxy_client.call_action("control", bytes(python_control_message))
