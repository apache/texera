import threading
from time import sleep

import pandas
import pytest

from core.models.internal_queue import ControlElement, DataElement, InternalQueue
from core.models.payload import DataFrame, EndOfUpstream
from core.runnables.network_receiver import NetworkReceiver
from core.runnables.network_sender import NetworkSender
from core.util.proto.proto_utils import set_one_of
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlInvocationV2, ControlPayloadV2


class TestNetworkReceiver:
    @pytest.fixture
    def input_queue(self):
        return InternalQueue()

    @pytest.fixture
    def output_queue(self):
        return InternalQueue()

    @pytest.fixture
    def schema_map(self):
        return dict()

    @pytest.fixture
    def network_receiver_thread(self, schema_map, output_queue):
        network_receiver = NetworkReceiver(output_queue, host="localhost", port=5555, schema_map=schema_map)
        network_receiver_thread = threading.Thread(target=network_receiver.run)
        yield network_receiver_thread
        network_receiver.stop()

    @pytest.fixture
    def network_sender_thread(self, schema_map, input_queue):
        network_sender = NetworkSender(input_queue, host="localhost", port=5555, schema_map=schema_map)
        network_sender_thread = threading.Thread(target=network_sender.run)
        yield network_sender_thread
        network_sender.stop()

    @pytest.fixture
    def data_payload(self):
        df_to_sent = pandas.DataFrame({
            'Brand': ['Honda Civic', 'Toyota Corolla', 'Ford Focus', 'Audi A4'],
            'Price': [22000, 25000, 27000, 35000]
        }, columns=['Brand', 'Price'])
        return DataFrame(frame=[r for _, r in df_to_sent.iterrows()])

    @pytest.mark.timeout(0.5)
    def test_network_receiver_can_stop(self, schema_map):
        network_receiver = NetworkReceiver(InternalQueue(), host="localhost", port=5555, schema_map=schema_map)
        network_receiver_thread = threading.Thread(target=network_receiver.run)
        network_receiver_thread.start()
        sleep(0.1)
        assert network_receiver_thread.is_alive()
        network_receiver.stop()
        sleep(0.1)
        assert not network_receiver_thread.is_alive()
        network_receiver_thread.join()

    @pytest.mark.timeout(1)
    def test_network_receiver_can_receive_data_messages(self, schema_map, data_payload, output_queue, input_queue,
                                                        network_receiver_thread, network_sender_thread):
        network_receiver_thread.start()
        network_sender_thread.start()
        worker_id = ActorVirtualIdentity(name="test")
        input_queue.put(DataElement(tag=worker_id, payload=data_payload))
        element: DataElement = output_queue.get()
        assert len(element.payload.frame) == len(data_payload.frame)
        assert element.tag == worker_id

    @pytest.mark.timeout(1)
    def test_network_receiver_can_receive_data_messages_end_of_upstream(self, schema_map, data_payload,
                                                                        output_queue, input_queue,
                                                                        network_receiver_thread, network_sender_thread):
        network_receiver_thread.start()
        network_sender_thread.start()
        worker_id = ActorVirtualIdentity(name="test")
        input_queue.put(DataElement(tag=worker_id, payload=EndOfUpstream()))
        element: DataElement = output_queue.get()
        assert element.payload == EndOfUpstream()
        assert element.tag == worker_id

    @pytest.mark.timeout(1)
    def test_network_receiver_can_receive_control_messages(self, schema_map, data_payload, output_queue, input_queue,
                                                           network_receiver_thread, network_sender_thread):
        network_receiver_thread.start()
        network_sender_thread.start()
        worker_id = ActorVirtualIdentity(name="test")
        control_payload = set_one_of(ControlPayloadV2, ControlInvocationV2())
        input_queue.put(ControlElement(tag=worker_id, payload=control_payload))
        element: ControlElement = output_queue.get()
        assert element.payload == control_payload
        assert element.tag == worker_id
