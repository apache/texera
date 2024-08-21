from typing import Optional

from loguru import logger
from overrides import overrides

from core.models import DataPayload, InternalQueue, DataFrame, MarkerFrame
from core.models.internal_queue import InternalQueueElement, DataElement, ControlElement
from core.proxy import ProxyClient
from core.util import StoppableQueueBlockingRunnable
from proto.edu.uci.ics.amber.engine.common import (
    ActorVirtualIdentity,
    ControlPayloadV2,
    PythonControlMessage,
    PythonDataHeader,
)


class NetworkSender(StoppableQueueBlockingRunnable):
    """
    Serialize and send messages.
    """

    def __init__(
        self,
        shared_queue: InternalQueue,
        host: str,
        port: int,
        handshake_port: Optional[int] = None,
    ):
        super().__init__(self.__class__.__name__, queue=shared_queue)
        self._proxy_client = ProxyClient(
            host=host, port=port, handshake_port=handshake_port
        )

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
        Send data payload to the given target actor. This method is to be used
        internally only.

        :param to: The target actor's ActorVirtualIdentity
        :param data_payload: The data payload to be sent, can be either DataFrame or
            EndOfUpstream
        """

        if isinstance(data_payload, DataFrame):
            data_header = PythonDataHeader(tag=to, payload_type="data")
            self._proxy_client.send_data(
                bytes(data_header), data_payload.frame
            )  # returns credits

        elif isinstance(data_payload, MarkerFrame):
            data_header = PythonDataHeader(
                tag=to, payload_type=data_payload.frame.__class__.__name__
            )
            self._proxy_client.send_data(bytes(data_header), None)  # returns credits

    @logger.catch(reraise=True)
    def _send_control(
        self, to: ActorVirtualIdentity, control_payload: ControlPayloadV2
    ) -> None:
        """
        Send the control payload to the given target actor. This method is to be used
        internally only.

        :param to: The target actor's ActorVirtualIdentity
        :param control_payload: The control payload to be sent, can be either
            ControlInvocation or ReturnInvocation.
        """
        python_control_message = PythonControlMessage(tag=to, payload=control_payload)
        int.from_bytes(
            self._proxy_client.call_action("control", bytes(python_control_message)),
            byteorder="little",
        )  # returned credits
