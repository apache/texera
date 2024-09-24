import threading

import pytest
from pyarrow import Table

from core.models.internal_queue import InternalQueue, ControlElement, DataElement
from core.models.marker import EndOfInputChannel
from core.models.payload import MarkerFrame, DataFrame
from core.proxy import ProxyClient
from core.runnables.network_receiver import NetworkReceiver
from core.runnables.network_sender import NetworkSender
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
        network_receiver._proxy_server.graceful_shutdown()

    class MockFlightMetadataReader:
        """
        MockFlightMetadataReader is a mocked FlightMetadataReader class to ultimately
        mock a credit value to be returned from Scala server to Python client
        """

        class MockBuffer:
            def to_pybytes(self):
                dummy_credit = 31
                return dummy_credit.to_bytes(8, "little")

        def read(self):
            return self.MockBuffer()

    @pytest.fixture
    def network_sender_thread(self, input_queue):
        network_sender = NetworkSender(input_queue, host="localhost", port=5555)

        # mocking do_put, read, to_pybytes to return fake credit values
        def mock_do_put(
            self,
            FlightDescriptor_descriptor,
            Schema_schema,
            FlightCallOptions_options=None,
        ):
            """
            Mocking FlightClient.do_put that is called in ProxyClient to return
            a MockFlightMetadataReader instead of a FlightMetadataReader

            :param self: an instance of FlightClient (would be ProxyClient in this case)
            :param FlightDescriptor_descriptor: descriptor
            :param Schema_schema: schema
            :param FlightCallOptions_options: options, None by default
            :return: writer : FlightStreamWriter, reader : MockFlightMetadataReader
            """
            writer, _ = super(ProxyClient, self).do_put(
                FlightDescriptor_descriptor, Schema_schema, FlightCallOptions_options
            )
            reader = TestNetworkReceiver.MockFlightMetadataReader()
            return writer, reader

        mock_proxy_client = network_sender._proxy_client
        mock_proxy_client.do_put = mock_do_put.__get__(
            mock_proxy_client, ProxyClient
        )  # override do_put with mock_do_put

        network_sender_thread = threading.Thread(target=network_sender.run)
        yield network_sender_thread
        network_sender.stop()

    @pytest.fixture
    def data_payload(self):
        return DataFrame(
            frame=Table.from_pydict(
                {
                    "Brand": ["Honda Civic", "Toyota Corolla", "Ford Focus", "Audi A4"],
                    "Price": [22000, 25000, 27000, 35000],
                }
            )
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
        input_queue.put(
            DataElement(tag=worker_id, payload=MarkerFrame(EndOfInputChannel()))
        )
        element: DataElement = output_queue.get()
        assert isinstance(element.payload, MarkerFrame)
        assert element.payload.frame == EndOfInputChannel()
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
