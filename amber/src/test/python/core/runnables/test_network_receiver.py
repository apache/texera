# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest
import threading
from pampy import MatchError
from pyarrow import Table
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from core.architecture.handlers.actorcommand.actor_handler_base import (
    ActorCommandHandler,
)
from core.architecture.handlers.actorcommand.backpressure_handler import (
    BackpressureHandler,
)
from core.architecture.handlers.actorcommand.credit_update_handler import (
    CreditUpdateHandler,
)
from core.models.internal_queue import (
    InternalQueue,
    DCMElement,
    DataElement,
    ECMElement,
)
from core.models.payload import DataFrame, StateFrame
from core.models.state import State
from core.proxy import ProxyClient, ProxyServer
from core.runnables.network_receiver import NetworkReceiver
from core.runnables.network_sender import NetworkSender
from core.util.proto import set_one_of
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
    EmbeddedControlMessageIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    ControlInvocation,
    EmbeddedControlMessage,
    EmbeddedControlMessageType,
    EmptyRequest,
    AsyncRpcContext,
    ControlRequest,
)
from proto.org.apache.texera.amber.engine.common import (
    ActorCommand,
    Backpressure,
    CreditUpdate,
    DirectControlMessagePayloadV2,
    PythonActorMessage,
    PythonControlMessage,
    PythonDataHeader,
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
        network_receiver.stop()

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

    @pytest.mark.timeout(10)
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
        channel_id = ChannelIdentity(worker_id, worker_id, False)
        input_queue.put(DataElement(tag=channel_id, payload=data_payload))
        element: DataElement = output_queue.get()
        assert len(element.payload.frame) == len(data_payload.frame)
        assert element.tag == channel_id

    @pytest.mark.timeout(10)
    def test_network_receiver_can_receive_consecutive_state_messages(
        self,
        output_queue,
        input_queue,
        network_receiver,
        network_sender_thread,
    ):
        network_sender_thread.start()
        worker_id = ActorVirtualIdentity(name="test")
        channel_id = ChannelIdentity(worker_id, worker_id, False)

        # loop_counter rides the StateFrame envelope (its own Arrow column),
        # not the user content. Use a non-zero counter so the round-trip
        # actually exercises the second column over the sender->receiver wire.
        input_queue.put(
            DataElement(
                tag=channel_id,
                payload=StateFrame(State({"i": 1}), loop_counter=0),
            )
        )
        input_queue.put(
            DataElement(
                tag=channel_id,
                payload=StateFrame(State({"i": 2}), loop_counter=5),
            )
        )

        first_element: DataElement = output_queue.get()
        second_element: DataElement = output_queue.get()

        assert isinstance(first_element.payload, StateFrame)
        assert first_element.payload.frame == {"i": 1}
        assert first_element.payload.loop_counter == 0
        assert first_element.tag == channel_id

        assert isinstance(second_element.payload, StateFrame)
        assert second_element.payload.frame == {"i": 2}
        assert second_element.payload.loop_counter == 5
        assert second_element.tag == channel_id

    @pytest.mark.timeout(10)
    def test_network_receiver_can_receive_control_messages(
        self,
        data_payload,
        output_queue,
        input_queue,
        network_receiver,
        network_sender_thread,
    ):
        worker_id = ActorVirtualIdentity(name="test")
        control_payload = set_one_of(DirectControlMessagePayloadV2, ControlInvocation())
        channel_id = ChannelIdentity(worker_id, worker_id, False)
        input_queue.put(DCMElement(tag=channel_id, payload=control_payload))
        network_sender_thread.start()
        element: DCMElement = output_queue.get()
        assert element.payload == control_payload
        assert element.tag == channel_id

    @pytest.mark.timeout(10)
    def test_network_receiver_can_receive_ecm(
        self,
        output_queue,
        input_queue,
        network_receiver,
        network_sender_thread,
    ):
        network_sender_thread.start()
        worker_id = ActorVirtualIdentity(name="test")
        channel_id = ChannelIdentity(worker_id, worker_id, False)
        ecm_id = EmbeddedControlMessageIdentity("test_ecm")
        scope = [channel_id]
        rpc_context = AsyncRpcContext(worker_id, worker_id)
        command_mapping = {
            str(worker_id): ControlInvocation(
                "NoOperation",
                ControlRequest(empty_request=EmptyRequest()),
                rpc_context,
                12,
            )
        }
        input_queue.put(
            ECMElement(
                tag=channel_id,
                payload=EmbeddedControlMessage(
                    ecm_id,
                    EmbeddedControlMessageType.ALL_ALIGNMENT,
                    scope,
                    command_mapping,
                ),
            )
        )
        element: DataElement = output_queue.get()
        assert isinstance(element.payload, EmbeddedControlMessage)
        assert element.payload.ecm_type == EmbeddedControlMessageType.ALL_ALIGNMENT
        assert element.payload.id == ecm_id
        assert element.payload.command_mapping == command_mapping
        assert element.payload.scope == scope
        assert element.tag == channel_id

    ###################################################################
    # Socket-free tests.
    #
    # The fixtures below swap `ProxyServer` for a mock so the handlers
    # registered by `NetworkReceiver.__init__` can be invoked directly with the
    # exact bytes the Flight endpoints would hand them. That keeps the
    # dispatch/lifecycle assertions hermetic (no port binding, no server
    # thread) and lets us reach branches -- retry-on-bind-failure, unknown
    # payload types, actor-command dispatch -- that the wire tests above
    # cannot drive.
    ###################################################################

    @pytest.fixture
    def mock_server_class(self):
        with patch("core.runnables.network_receiver.ProxyServer") as server_class:
            # `register_shutdown` goes through `ProxyServer.ack`; keep the real
            # decorator so the registered action's behavior stays under test.
            server_class.ack = ProxyServer.ack
            yield server_class

    @pytest.fixture
    def offline_receiver(self, output_queue, mock_server_class):
        """A NetworkReceiver whose ProxyServer is a mock, plus the three
        handlers it registered on that server."""
        receiver = NetworkReceiver(output_queue, host="localhost", port=6666)
        server = mock_server_class.return_value
        return SimpleNamespace(
            receiver=receiver,
            server=server,
            queue=output_queue,
            data_handler=server.register_data_handler.call_args[0][0],
            control_handler=server.register_control_handler.call_args[0][0],
            actor_handler=server.register_actor_message_handler.call_args[0][0],
        )

    @staticmethod
    def data_channel():
        worker_id = ActorVirtualIdentity(name="test")
        return ChannelIdentity(worker_id, worker_id, False)

    @staticmethod
    def control_channel():
        worker_id = ActorVirtualIdentity(name="test")
        return ChannelIdentity(worker_id, worker_id, True)

    def test_init_retries_until_the_proxy_server_binds(self, output_queue):
        with patch("core.runnables.network_receiver.ProxyServer") as server_class:
            bound_server = MagicMock(name="bound_server")
            server_class.side_effect = [OSError("port already in use"), bound_server]
            receiver = NetworkReceiver(output_queue, host="localhost", port=6667)

        assert server_class.call_count == 2
        assert server_class.call_args_list[0].kwargs == {
            "host": "localhost",
            "port": 6667,
        }
        # The receiver keeps the instance from the successful attempt only.
        assert receiver.proxy_server is bound_server

    def test_run_serves_on_the_proxy_server(self, offline_receiver):
        offline_receiver.receiver.run()
        offline_receiver.server.serve.assert_called_once_with()

    def test_stop_shuts_the_server_down_then_waits_for_it(self, offline_receiver):
        offline_receiver.receiver.stop()
        offline_receiver.server.graceful_shutdown.assert_called_once_with()
        offline_receiver.server.wait.assert_called_once_with()
        called = [name for name, _, _ in offline_receiver.server.mock_calls]
        # waiting before shutting down would block forever
        assert called.index("graceful_shutdown") < called.index("wait")

    def test_register_shutdown_registers_an_acking_shutdown_action(
        self, offline_receiver
    ):
        shutdown = MagicMock(name="shutdown")
        offline_receiver.receiver.register_shutdown(shutdown)

        offline_receiver.server.register.assert_called_once()
        kwargs = offline_receiver.server.register.call_args.kwargs
        assert kwargs["name"] == "shutdown"
        shutdown.assert_not_called()
        # The registered action must invoke the callback and ack the caller.
        assert kwargs["action"]() == "Bye bye!"
        shutdown.assert_called_once_with()

    @pytest.mark.timeout(10)
    def test_data_handler_enqueues_a_data_frame_and_reports_credits(
        self, offline_receiver, data_payload
    ):
        channel_id = self.data_channel()
        header = PythonDataHeader(tag=channel_id, payload_type="Data")

        credits = offline_receiver.data_handler(bytes(header), data_payload.frame)

        assert credits == offline_receiver.queue.in_mem_size()
        assert credits > 0
        element = offline_receiver.queue.get()
        assert isinstance(element, DataElement)
        assert isinstance(element.payload, DataFrame)
        assert element.payload.frame is data_payload.frame
        assert element.tag == channel_id
        # is_control is normalized to a real bool so the tag hashes
        # consistently as an internal-queue key.
        assert element.tag.is_control is False
        assert hash(element.tag) == hash(channel_id)

    @pytest.mark.timeout(10)
    def test_data_handler_rebuilds_a_state_frame_from_its_columns(
        self, offline_receiver
    ):
        channel_id = self.data_channel()
        header = PythonDataHeader(tag=channel_id, payload_type="State")
        table = Table.from_pydict(
            {
                State.CONTENT: [State({"answer": 42}).to_json()],
                State.LOOP_COUNTER: [7],
                State.LOOP_START_ID: ["loop-start-1"],
            }
        )

        offline_receiver.data_handler(bytes(header), table)

        element = offline_receiver.queue.get()
        assert isinstance(element.payload, StateFrame)
        assert element.payload.frame == {"answer": 42}
        assert element.payload.loop_counter == 7
        assert element.payload.loop_start_id == "loop-start-1"
        assert element.tag == channel_id

    @pytest.mark.timeout(10)
    def test_data_handler_wraps_an_ecm_payload_in_an_ecm_element(
        self, offline_receiver
    ):
        channel_id = self.data_channel()
        header = PythonDataHeader(tag=channel_id, payload_type="ECM")
        ecm = EmbeddedControlMessage(
            EmbeddedControlMessageIdentity("ecm-1"),
            EmbeddedControlMessageType.NO_ALIGNMENT,
            [self.control_channel()],
            {},
        )
        table = Table.from_pydict({"payload": [bytes(ecm)]})

        offline_receiver.data_handler(bytes(header), table)

        element = offline_receiver.queue.get()
        assert isinstance(element, ECMElement)
        assert element.payload == ecm
        assert element.tag == channel_id
        # every scope channel gets the same bool normalization as the tag
        assert [c.is_control for c in element.payload.scope] == [True]
        assert all(type(c.is_control) is bool for c in element.payload.scope)

    def test_data_handler_rejects_an_unknown_payload_type(
        self, offline_receiver, data_payload
    ):
        header = PythonDataHeader(tag=self.data_channel(), payload_type="Nonsense")

        with pytest.raises(MatchError):
            offline_receiver.data_handler(bytes(header), data_payload.frame)

        assert offline_receiver.queue.is_empty()

    @pytest.mark.timeout(10)
    def test_control_handler_enqueues_a_dcm_element_without_charging_credits(
        self, offline_receiver
    ):
        channel_id = self.control_channel()
        payload = set_one_of(DirectControlMessagePayloadV2, ControlInvocation())
        message = PythonControlMessage(tag=channel_id, payload=payload)

        credits = offline_receiver.control_handler(bytes(message))

        # in_mem_size only counts data channels, so a control message
        # never consumes sender credits.
        assert credits == 0
        element = offline_receiver.queue.get()
        assert isinstance(element, DCMElement)
        assert element.tag == channel_id
        assert element.payload == payload

    def test_actor_handler_disables_data_when_backpressure_is_enabled(
        self, offline_receiver, data_payload
    ):
        offline_receiver.queue.put(
            DataElement(tag=self.data_channel(), payload=data_payload)
        )
        assert offline_receiver.queue.is_data_enabled()
        message = PythonActorMessage(
            payload=set_one_of(ActorCommand, Backpressure(enable_backpressure=True))
        )

        credits = offline_receiver.actor_handler(bytes(message))

        assert credits == offline_receiver.queue.in_mem_size()
        assert not offline_receiver.queue.is_data_enabled()
        # backpressure only gates the data queues, it enqueues nothing
        assert offline_receiver.queue.size_control() == 0

    @pytest.mark.timeout(10)
    def test_actor_handler_reenables_data_and_wakes_the_worker(
        self, offline_receiver, data_payload
    ):
        offline_receiver.queue.put(
            DataElement(tag=self.data_channel(), payload=data_payload)
        )
        offline_receiver.queue.disable_data(
            InternalQueue.DisableType.DISABLE_BY_BACKPRESSURE
        )
        message = PythonActorMessage(
            payload=set_one_of(ActorCommand, Backpressure(enable_backpressure=False))
        )

        offline_receiver.actor_handler(bytes(message))

        assert offline_receiver.queue.is_data_enabled()
        # a NoOperation control invocation is injected so the blocked main
        # loop wakes up and drains the re-enabled data queues
        assert offline_receiver.queue.size_control() == 1
        drained = [offline_receiver.queue.get() for _ in range(2)]
        control_elements = [e for e in drained if isinstance(e, DCMElement)]
        assert len(control_elements) == 1
        assert control_elements[0].tag.is_control
        assert (
            control_elements[0].payload.control_invocation.method_name == "NoOperation"
        )

    def test_actor_handler_treats_a_credit_update_as_a_no_op(self, offline_receiver):
        message = PythonActorMessage(payload=set_one_of(ActorCommand, CreditUpdate()))

        credits = offline_receiver.actor_handler(bytes(message))

        assert credits == 0
        assert offline_receiver.queue.is_empty()

    def test_look_up_returns_the_handler_registered_for_the_command_type(
        self, offline_receiver
    ):
        receiver = offline_receiver.receiver
        assert isinstance(receiver.look_up(Backpressure()), BackpressureHandler)
        assert isinstance(receiver.look_up(CreditUpdate()), CreditUpdateHandler)

    def test_look_up_raises_for_an_unregistered_command_type(self, offline_receiver):
        with pytest.raises(KeyError):
            offline_receiver.receiver.look_up(ActorCommand())

    def test_register_actor_command_handler_keys_on_cmd_and_overwrites(
        self, offline_receiver
    ):
        class RecordingCreditUpdateHandler(ActorCommandHandler):
            cmd = CreditUpdate

            def __init__(self):
                self.calls = []

            def __call__(self, command, input_queue, *args, **kwargs):
                self.calls.append(command)

        replacement = RecordingCreditUpdateHandler()
        offline_receiver.receiver.register_actor_command_handler(replacement)

        assert offline_receiver.receiver.look_up(CreditUpdate()) is replacement
        # the other registration is untouched
        assert isinstance(
            offline_receiver.receiver.look_up(Backpressure()), BackpressureHandler
        )

        message = PythonActorMessage(payload=set_one_of(ActorCommand, CreditUpdate()))
        offline_receiver.actor_handler(bytes(message))
        assert replacement.calls == [CreditUpdate()]
