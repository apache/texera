import threading

import pandas
import pytest

from core.models import Tuple
from core.models.internal_queue import ControlElement, DataElement, InternalQueue
from core.models.payload import OutputDataFrame, EndOfUpstream
from core.runnables.network_receiver import NetworkReceiver
from core.runnables.network_sender import NetworkSender
from core.util.arrow_utils import to_arrow_schema
from core.util.proto import set_one_of
from proto.edu.uci.ics.amber.engine.common import (
    ActorVirtualIdentity,
    ControlInvocationV2,
    ControlPayloadV2,
)


class TestNetworkReceiver:
    @pytest.fixture
    def input_queue(self):
        return InternalQueue()

    @pytest.fixture
    def output_queue(self):
        return InternalQueue()

    @pytest.fixture
    def network_receiver(self, output_queue):
        network_receiver = NetworkReceiver(output_queue, host="localhost", port=5555)
        yield network_receiver
        network_receiver._proxy_server.shutdown()

    @pytest.fixture
    def network_sender_thread(self, input_queue):
        network_sender = NetworkSender(input_queue, host="localhost", port=5555)
        network_sender_thread = threading.Thread(target=network_sender.run)
        yield network_sender_thread
        network_sender.stop()

    @pytest.fixture
    def data_payload(self):
        df_to_sent = pandas.DataFrame(
            {
                "Brand": ["Honda Civic", "Toyota Corolla", "Ford Focus", "Audi A4"],
                "Price": [22000, 25000, 27000, 35000],
            },
            columns=["Brand", "Price"],
        )
        return OutputDataFrame(
            frame=[Tuple(r) for _, r in df_to_sent.iterrows()],
            schema=to_arrow_schema({"Brand": "string", "Price": "integer"}),
        )

    @pytest.mark.timeout(2)
    def test_network_receiver_can_receive_data_messages(
        self,
        data_payload,
        output_queue,
        input_queue,
        network_receiver,
        network_sender_thread,
    ):
        network_sender_thread.start()
        worker_id = ActorVirtualIdentity(name="test")
        input_queue.put(DataElement(tag=worker_id, payload=data_payload))
        element: DataElement = output_queue.get()
        assert len(element.payload.frame) == len(data_payload.frame)
        assert element.tag == worker_id

    @pytest.mark.timeout(2)
    def test_network_receiver_can_receive_data_messages_end_of_upstream(
        self,
        data_payload,
        output_queue,
        input_queue,
        network_receiver,
        network_sender_thread,
    ):
        network_sender_thread.start()
        worker_id = ActorVirtualIdentity(name="test")
        input_queue.put(DataElement(tag=worker_id, payload=EndOfUpstream()))
        element: DataElement = output_queue.get()
        assert element.payload == EndOfUpstream()
        assert element.tag == worker_id

    @pytest.mark.timeout(2)
    def test_network_receiver_can_receive_control_messages(
        self,
        data_payload,
        output_queue,
        input_queue,
        network_receiver,
        network_sender_thread,
    ):
        worker_id = ActorVirtualIdentity(name="test")
        control_payload = set_one_of(ControlPayloadV2, ControlInvocationV2())
        input_queue.put(ControlElement(tag=worker_id, payload=control_payload))
        network_sender_thread.start()
        element: ControlElement = output_queue.get()
        assert element.payload == control_payload
        assert element.tag == worker_id
