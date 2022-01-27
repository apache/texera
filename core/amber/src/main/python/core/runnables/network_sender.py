from typing import Dict

import pandas
from loguru import logger
from overrides import overrides
from pyarrow import Table
from pyarrow.lib import Schema, schema

from core.models import ControlElement, DataElement, OutputDataFrame, DataPayload, EndOfUpstream, InternalQueue, \
    InternalQueueElement
from core.proxy import ProxyClient
from core.util import StoppableQueueBlockingRunnable
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlPayloadV2, PythonControlMessage, \
    PythonDataHeader


class NetworkSender(StoppableQueueBlockingRunnable):
    """
    Serialize and send messages.
    """

    def __init__(self, shared_queue: InternalQueue, host: str, port: int, schema_map: Dict[str, type]):
        super().__init__(self.__class__.__name__, queue=shared_queue)
        self._proxy_client = ProxyClient(host=host, port=port)
        self._schema_map = schema_map

    @overrides(check_signature=False)
    def receive(self, next_entry: InternalQueueElement):
        if isinstance(next_entry, DataElement):
            self._send_data(next_entry.tag, next_entry.payload)
        elif isinstance(next_entry, ControlElement):
            self._send_control(next_entry.tag, next_entry.payload)
        else:
            raise TypeError(f"Unexpected entry {next_entry}")

    @logger.catch(reraise=True)
    def _send_data(self, to: ActorVirtualIdentity, data_payload: DataPayload) -> None:
        """
        Send data payload to the given target actor. This method is to be used internally only.

        :param to: The target actor's ActorVirtualIdentity
        :param data_payload: The data payload to be sent, can be either DataFrame or EndOfUpstream
        """
        if isinstance(data_payload, OutputDataFrame):
            df = pandas.DataFrame.from_records(data=[i.get_fields(data_payload.schema) for i in data_payload.frame], columns=data_payload.schema)
            inferred_schema: Schema = Schema.from_pandas(df)
            # create a output schema, use the original input schema if possible,
            # otherwise use the inferred schema
            # TODO: OPTIMIZE:
            #   use pandas to infer/cache column types, remove dependencies on pyarrow.
            #   Pyarrow related logic should be moved into proxy level.
            output_schema = schema([self._schema_map.get(field.name, field) for field in inferred_schema])
            data_header = PythonDataHeader(tag=to, is_end=False)
            table = Table.from_pandas(df=df, schema=output_schema)
            self._proxy_client.send_data(bytes(data_header), table)

        elif isinstance(data_payload, EndOfUpstream):
            data_header = PythonDataHeader(tag=to, is_end=True)
            self._proxy_client.send_data(bytes(data_header), None)

        else:
            raise TypeError(f"Unexpected payload {data_payload}")

    @logger.catch(reraise=True)
    def _send_control(self, to: ActorVirtualIdentity, control_payload: ControlPayloadV2) -> None:
        """
        Send the control payload to the given target actor. This method is to be used internally only.

        :param to: The target actor's ActorVirtualIdentity
        :param control_payload: The control payload to be sent, can be either ControlInvocation or
            ReturnInvocation.
        """
        python_control_message = PythonControlMessage(tag=to, payload=control_payload)
        self._proxy_client.call_action("control", bytes(python_control_message))
