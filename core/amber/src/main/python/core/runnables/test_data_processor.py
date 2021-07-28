from threading import Thread

import pandas
import pytest

from core.models import ControlElement, DataElement, DataFrame, EndOfUpstream, InternalQueue
from core.runnables import DataProcessor
from core.udf.examples import EchoOperator
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import OneToOnePartitioning, Partitioning
from proto.edu.uci.ics.amber.engine.architecture.worker import AddPartitioningV2, ControlCommandV2, ControlReturnV2, \
    UpdateInputLinkingV2, WorkerExecutionCompletedV2
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlInvocationV2, ControlPayloadV2, \
    LayerIdentity, LinkIdentity, ReturnInvocationV2


class TestDpTread:
    @pytest.fixture
    def command_sequence(self):
        return 1

    @pytest.fixture
    def mock_udf(self):
        return EchoOperator()

    @pytest.fixture
    def mock_link(self):
        return LinkIdentity(from_=LayerIdentity("from", "from", "from"), to=LayerIdentity("to", "to", "to"))

    @pytest.fixture
    def mock_tuple(self):
        return pandas.Series({"test-1": "hello", "test-2": 10})

    @pytest.fixture
    def mock_sender_actor(self):
        return ActorVirtualIdentity("sender")

    @pytest.fixture
    def mock_controller(self):
        return ActorVirtualIdentity("CONTROLLER")

    @pytest.fixture
    def mock_receiver_actor(self):
        return ActorVirtualIdentity("receiver")

    @pytest.fixture
    def mock_data_element(self, mock_tuple, mock_sender_actor):
        return DataElement(tag=mock_sender_actor, payload=DataFrame(frame=[mock_tuple]))

    @pytest.fixture
    def mock_end_of_upstream(self, mock_tuple, mock_sender_actor):
        return DataElement(tag=mock_sender_actor, payload=EndOfUpstream())

    @pytest.fixture
    def input_queue(self):
        return InternalQueue()

    @pytest.fixture
    def output_queue(self):
        return InternalQueue()

    @pytest.fixture
    def mock_update_input_linking(self, mock_controller, mock_sender_actor, mock_link, command_sequence):
        command = set_one_of(ControlCommandV2, UpdateInputLinkingV2(identifier=mock_sender_actor, input_link=mock_link))
        payload = set_one_of(ControlPayloadV2, ControlInvocationV2(command_id=command_sequence, command=command))
        return ControlElement(tag=mock_controller, payload=payload)

    @pytest.fixture
    def mock_add_partitioning(self, mock_controller, mock_receiver_actor, command_sequence):
        command = set_one_of(
            ControlCommandV2,
            AddPartitioningV2(
                tag=mock_receiver_actor,
                partitioning=set_one_of(
                    Partitioning, OneToOnePartitioning(
                        batch_size=1,
                        receivers=[mock_receiver_actor]
                    )
                )
            )
        )
        payload = set_one_of(ControlPayloadV2, ControlInvocationV2(command_id=command_sequence, command=command))
        return ControlElement(tag=mock_controller, payload=payload)

    @pytest.fixture
    def data_processor(self, input_queue, output_queue, mock_udf):
        data_processor = DataProcessor(input_queue, output_queue, mock_udf)
        yield data_processor
        data_processor.stop()

    @pytest.fixture
    def dp_thread(self, data_processor, reraise):
        def wrapper():
            with reraise:
                data_processor.run()

        dp_thread = Thread(target=wrapper, name="dp_thread")
        yield dp_thread

    def test_dp_thread_can_start(self, dp_thread):
        dp_thread.start()
        assert dp_thread.is_alive()

    def test_dp_thread_can_handle_data_messages(self, mock_link, mock_receiver_actor, mock_controller, input_queue,
                                                output_queue, mock_data_element, dp_thread, mock_update_input_linking,
                                                mock_add_partitioning, mock_end_of_upstream, command_sequence, reraise):
        dp_thread.start()

        # can update input link
        input_queue.put(mock_update_input_linking)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=command_sequence,
                    control_return=ControlReturnV2())
            )
        )

        # can add partitioning
        input_queue.put(mock_add_partitioning)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=command_sequence,
                    control_return=ControlReturnV2())
            )
        )

        # can process a DataFrame
        input_queue.put(mock_data_element)
        expected_data_element = mock_data_element
        expected_data_element.tag = mock_receiver_actor
        assert output_queue.get() == expected_data_element

        # can process an EndOfUpstream
        input_queue.put(mock_end_of_upstream)
        assert output_queue.get() == DataElement(tag=mock_receiver_actor, payload=EndOfUpstream())

        # WorkerExecutionCompletedV2 should be triggered when workflow finishes
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                control_invocation=ControlInvocationV2(
                    command_id=0,
                    command=ControlCommandV2(
                        worker_execution_completed=WorkerExecutionCompletedV2()
                    )
                )
            )
        )

        # can process ReturnInvocation
        input_queue.put(
            ControlElement(
                tag=mock_controller,
                payload=set_one_of(
                    ControlPayloadV2,
                    ReturnInvocationV2(
                        original_command_id=0,
                        control_return=ControlReturnV2()
                    )
                )
            )
        )

        reraise()
