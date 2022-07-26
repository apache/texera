from threading import Thread

import pandas
import pyarrow
import pytest
from loguru import logger

from core.models import (
    ControlElement,
    DataElement,
    InputDataFrame,
    OutputDataFrame,
    EndOfUpstream,
    InternalQueue,
    Tuple,
)
from core.runnables import DataProcessor
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import (
    OneToOnePartitioning,
    Partitioning,
)
from proto.edu.uci.ics.amber.engine.architecture.worker import (
    AddPartitioningV2,
    ControlCommandV2,
    ControlReturnV2,
    QueryStatisticsV2,
    UpdateInputLinkingV2,
    WorkerExecutionCompletedV2,
    WorkerState,
    WorkerStatistics,
    LinkCompletedV2,
)
from proto.edu.uci.ics.amber.engine.common import (
    ActorVirtualIdentity,
    ControlInvocationV2,
    ControlPayloadV2,
    LayerIdentity,
    LinkIdentity,
    ReturnInvocationV2,
)
from pytexera.udf.examples.echo_operator import EchoOperator

logger.level("PRINT", no=38)


class TestDataProcessor:
    @pytest.fixture
    def command_sequence(self):
        return 1

    @pytest.fixture
    def mock_udf(self):
        return EchoOperator()

    @pytest.fixture
    def mock_link(self):
        return LinkIdentity(
            from_=LayerIdentity("from", "from", "from"),
            to=LayerIdentity("to", "to", "to"),
        )

    @pytest.fixture
    def mock_tuple(self):
        return Tuple({"test-1": "hello", "test-2": 10})

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
        return DataElement(
            tag=mock_sender_actor,
            payload=InputDataFrame(
                frame=pyarrow.Table.from_pandas(
                    pandas.DataFrame([mock_tuple.as_dict()])
                )
            ),
        )

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
    def mock_update_input_linking(
        self, mock_controller, mock_sender_actor, mock_link, command_sequence
    ):
        command = set_one_of(
            ControlCommandV2,
            UpdateInputLinkingV2(identifier=mock_sender_actor, input_link=mock_link),
        )
        payload = set_one_of(
            ControlPayloadV2,
            ControlInvocationV2(command_id=command_sequence, command=command),
        )
        return ControlElement(tag=mock_controller, payload=payload)

    @pytest.fixture
    def mock_add_partitioning(
        self, mock_controller, mock_receiver_actor, command_sequence
    ):
        command = set_one_of(
            ControlCommandV2,
            AddPartitioningV2(
                tag=mock_receiver_actor,
                partitioning=set_one_of(
                    Partitioning,
                    OneToOnePartitioning(batch_size=1, receivers=[mock_receiver_actor]),
                ),
            ),
        )
        payload = set_one_of(
            ControlPayloadV2,
            ControlInvocationV2(command_id=command_sequence, command=command),
        )
        return ControlElement(tag=mock_controller, payload=payload)

    @pytest.fixture
    def mock_query_statistics(
        self, mock_controller, mock_sender_actor, command_sequence
    ):
        command = set_one_of(ControlCommandV2, QueryStatisticsV2())
        payload = set_one_of(
            ControlPayloadV2,
            ControlInvocationV2(command_id=command_sequence, command=command),
        )
        return ControlElement(tag=mock_controller, payload=payload)

    @pytest.fixture
    def data_processor(self, input_queue, output_queue, mock_udf, mock_link):
        data_processor = DataProcessor(input_queue, output_queue)
        # mock the operator binding
        data_processor._operator = mock_udf
        data_processor.context.batch_to_tuple_converter.update_all_upstream_link_ids(
            {mock_link}
        )
        data_processor._operator.output_schema = {
            "test-1": "string",
            "test-2": "integer",
        }
        yield data_processor
        data_processor.stop()

    @pytest.fixture
    def dp_thread(self, data_processor, reraise):
        def wrapper():
            with reraise:
                data_processor.run()

        dp_thread = Thread(target=wrapper, name="dp_thread")
        yield dp_thread

    @pytest.mark.timeout(2)
    def test_dp_thread_can_start(self, dp_thread):
        dp_thread.start()
        assert dp_thread.is_alive()

    @pytest.mark.timeout(2)
    def test_dp_thread_can_process_messages(
        self,
        mock_link,
        mock_receiver_actor,
        mock_controller,
        input_queue,
        output_queue,
        mock_data_element,
        dp_thread,
        mock_update_input_linking,
        mock_add_partitioning,
        mock_end_of_upstream,
        mock_query_statistics,
        mock_tuple,
        command_sequence,
        reraise,
    ):
        dp_thread.start()

        # can process UpdateInputLinking
        input_queue.put(mock_update_input_linking)

        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=command_sequence,
                    control_return=ControlReturnV2(),
                )
            ),
        )

        # can process AddPartitioning
        input_queue.put(mock_add_partitioning)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=command_sequence,
                    control_return=ControlReturnV2(),
                )
            ),
        )

        # can process a InputDataFrame
        input_queue.put(mock_data_element)

        output_data_element: DataElement = output_queue.get()
        assert output_data_element.tag == mock_receiver_actor
        assert isinstance(output_data_element.payload, OutputDataFrame)
        data_frame: OutputDataFrame = output_data_element.payload
        assert len(data_frame.frame) == 1
        assert data_frame.frame[0] == mock_tuple

        # can process QueryStatistics
        input_queue.put(mock_query_statistics)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=1,
                    control_return=ControlReturnV2(
                        worker_statistics=WorkerStatistics(
                            worker_state=WorkerState.RUNNING,
                            input_tuple_count=1,
                            output_tuple_count=1,
                        )
                    ),
                )
            ),
        )

        # can process EndOfUpstream
        input_queue.put(mock_end_of_upstream)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                control_invocation=ControlInvocationV2(
                    command_id=0,
                    command=ControlCommandV2(
                        link_completed=LinkCompletedV2(link_id=mock_link)
                    ),
                )
            ),
        )
        assert output_queue.get() == DataElement(
            tag=mock_receiver_actor, payload=EndOfUpstream()
        )

        # WorkerExecutionCompletedV2 should be triggered when workflow finishes
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                control_invocation=ControlInvocationV2(
                    command_id=1,
                    command=ControlCommandV2(
                        worker_execution_completed=WorkerExecutionCompletedV2()
                    ),
                )
            ),
        )

        # can process ReturnInvocation
        input_queue.put(
            ControlElement(
                tag=mock_controller,
                payload=set_one_of(
                    ControlPayloadV2,
                    ReturnInvocationV2(
                        original_command_id=0, control_return=ControlReturnV2()
                    ),
                ),
            )
        )

        reraise()
