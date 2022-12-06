import time
from threading import Thread
import threading

import pandas
import pyarrow
import pytest
import warnings
from loguru import logger

from core.models import (
    InputDataFrame,
    OutputDataFrame,
    EndOfUpstream,
    InternalQueue,
    Tuple,
    Batch,
)
from core.models.internal_queue import DataElement, ControlElement
from core.runnables import MainLoop
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
from pytexera.udf.examples.count_batch_operator import CountBatchOperator



class TestMainLoop:
    @pytest.fixture
    def command_sequence(self):
        return 1

    @pytest.fixture
    def mock_udf(self):
        return EchoOperator()

    @pytest.fixture
    def mock_batch_udf(self):
        return CountBatchOperator()

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
    def mock_batch(self):
        batch_list = []
        for i in range(57):
            batch_list.append(Tuple({"test-1": "hello", "test-2": i}))
        return batch_list

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
    def mock_batch_data_elements(self, mock_batch, mock_sender_actor):

        data_elements = []
        for i in range(57):
            mock_tuple = Tuple({"test-1": "hello", "test-2": i})
            data_elements.append(
                DataElement(
                    tag=mock_sender_actor,
                    payload=InputDataFrame(
                        frame=pyarrow.Table.from_pandas(
                            pandas.DataFrame([mock_tuple.as_dict()])
                        )
                    ),
                )
            )

        return data_elements

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
    def main_loop(self, input_queue, output_queue, mock_udf, mock_link):
        main_loop = MainLoop(input_queue, output_queue)
        # mock the operator binding
        main_loop.context.operator_manager.operator = mock_udf
        main_loop.context.batch_to_tuple_converter.update_all_upstream_link_ids(
            {mock_link}
        )
        main_loop.context.operator_manager.operator.output_schema = {
            "test-1": "string",
            "test-2": "integer",
        }
        yield main_loop
        main_loop.stop()

    @pytest.fixture
    def batch_data_processor(
        self, input_queue, output_queue, mock_batch_udf, mock_link
    ):
        batch_data_processor = DataProcessor(input_queue, output_queue)
        # mock the operator binding
        batch_data_processor._operator = mock_batch_udf
        batch_data_processor.context.batch_to_tuple_converter.update_all_upstream_link_ids(
            {mock_link}
        )
        batch_data_processor._operator.output_schema = {
            "test-1": "string",
            "test-2": "integer",
        }
        yield batch_data_processor
        batch_data_processor.stop()

    @pytest.fixture
    def main_loop_thread(self, main_loop, reraise):
        def wrapper():
            with reraise:
                main_loop.run()

        main_loop_thread = Thread(target=wrapper, name="main_loop_thread")
        yield main_loop_thread

    @pytest.fixture
    def batch_dp_thread(self, batch_data_processor, reraise):
        def wrapper():
            with reraise:
                batch_data_processor.run()

        batch_dp_thread = Thread(target=wrapper, name="batch_dp_thread_2")
        yield batch_dp_thread

    def test_initial_process(
        self,
        batch_dp_thread,
        input_queue,
        mock_update_input_linking,
        output_queue,
        mock_controller,
        command_sequence,
        mock_add_partitioning,
    ):
        batch_dp_thread.start()

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

    @staticmethod
    def check_batch_rank_sum(
        input_queue,
        mock_batch_data_elements,
        output_data_elements,
        output_queue,
        mock_batch,
        mock_batch_udf,
        start,
        end,
        count,
    ):
        # Checking the rank sum of each batch to make sure the accuracy
        for i in range(start, end):
            input_queue.put(mock_batch_data_elements[i])
        rank_sum_real = 0
        rank_sum_suppose = 0
        for i in range(start, end):
            output_data_elements.append(output_queue.get())
            rank_sum_real += output_data_elements[i].payload.frame[0]["test-2"]
            rank_sum_suppose += mock_batch[i]["test-2"]
        assert mock_batch_udf.count == count
        assert rank_sum_real == rank_sum_suppose

    @pytest.mark.timeout(2)
    def test_batch_dp_thread_can_start(self, batch_dp_thread):
        batch_dp_thread.start()
        assert batch_dp_thread.is_alive()

    @pytest.mark.timeout(2)
    def test_main_loop_thread_can_start(self, main_loop_thread):
        main_loop_thread.start()
        assert main_loop_thread.is_alive()

    @pytest.mark.timeout(2)
    def test_main_loop_thread_can_process_messages(
        self,
        mock_link,
        mock_receiver_actor,
        mock_controller,
        input_queue,
        output_queue,
        mock_data_element,
        main_loop_thread,
        mock_update_input_linking,
        mock_add_partitioning,
        mock_end_of_upstream,
        mock_query_statistics,
        mock_tuple,
        command_sequence,
        reraise,
    ):
        main_loop_thread.start()

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

        assert output_queue.get() == DataElement(
            tag=mock_receiver_actor, payload=EndOfUpstream()
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

    @pytest.mark.timeout(5)
    def test_batch_dp_thread_can_process_batch_simple(
        self,
        mock_batch_udf,
        mock_link,
        mock_receiver_actor,
        mock_controller,
        input_queue,
        output_queue,
        mock_batch_data_elements,
        batch_dp_thread,
        mock_update_input_linking,
        mock_add_partitioning,
        mock_end_of_upstream,
        mock_query_statistics,
        mock_batch,
        command_sequence,
        reraise,
    ):

        self.test_initial_process(
            batch_dp_thread,
            input_queue,
            mock_update_input_linking,
            output_queue,
            mock_controller,
            command_sequence,
            mock_add_partitioning,
        )

        # can process a InputDataFrame
        output_data_elements = []
        self.check_batch_rank_sum(
            input_queue,
            mock_batch_data_elements,
            output_data_elements,
            output_queue,
            mock_batch,
            mock_batch_udf,
            0,
            10,
            1,
        )
        self.check_batch_rank_sum(
            input_queue,
            mock_batch_data_elements,
            output_data_elements,
            output_queue,
            mock_batch,
            mock_batch_udf,
            10,
            20,
            2,
        )
        self.check_batch_rank_sum(
            input_queue,
            mock_batch_data_elements,
            output_data_elements,
            output_queue,
            mock_batch,
            mock_batch_udf,
            20,
            30,
            3,
        )
        self.check_batch_rank_sum(
            input_queue,
            mock_batch_data_elements,
            output_data_elements,
            output_queue,
            mock_batch,
            mock_batch_udf,
            30,
            40,
            4,
        )
        self.check_batch_rank_sum(
            input_queue,
            mock_batch_data_elements,
            output_data_elements,
            output_queue,
            mock_batch,
            mock_batch_udf,
            40,
            50,
            5,
        )

        input_queue.put(mock_end_of_upstream)

        assert output_data_elements[0].tag == mock_receiver_actor
        assert isinstance(output_data_elements[0].payload, OutputDataFrame)
        data_frame: OutputDataFrame = output_data_elements[0].payload
        assert len(data_frame.frame) == 1

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
        # can process EndOfUpstream
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

        # can process QueryStatistics
        input_queue.put(mock_query_statistics)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=1,
                    control_return=ControlReturnV2(
                        worker_statistics=WorkerStatistics(
                            worker_state=WorkerState.COMPLETED,
                            input_tuple_count=50,
                            output_tuple_count=50,
                        )
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

    @pytest.mark.timeout(5)
    def test_batch_dp_thread_can_process_batch_median(
        self,
        mock_batch_udf,
        mock_link,
        mock_receiver_actor,
        mock_controller,
        input_queue,
        output_queue,
        mock_batch_data_elements,
        batch_dp_thread,
        mock_update_input_linking,
        mock_add_partitioning,
        mock_end_of_upstream,
        mock_query_statistics,
        mock_batch,
        command_sequence,
        reraise,
    ):
        self.test_initial_process(
            batch_dp_thread,
            input_queue,
            mock_update_input_linking,
            output_queue,
            mock_controller,
            command_sequence,
            mock_add_partitioning,
        )

        # can process a InputDataFrame

        output_data_elements = []
        self.check_batch_rank_sum(
            input_queue,
            mock_batch_data_elements,
            output_data_elements,
            output_queue,
            mock_batch,
            mock_batch_udf,
            0,
            10,
            1,
        )
        self.check_batch_rank_sum(
            input_queue,
            mock_batch_data_elements,
            output_data_elements,
            output_queue,
            mock_batch,
            mock_batch_udf,
            10,
            20,
            2,
        )
        self.check_batch_rank_sum(
            input_queue,
            mock_batch_data_elements,
            output_data_elements,
            output_queue,
            mock_batch,
            mock_batch_udf,
            20,
            30,
            3,
        )
        self.check_batch_rank_sum(
            input_queue,
            mock_batch_data_elements,
            output_data_elements,
            output_queue,
            mock_batch,
            mock_batch_udf,
            30,
            40,
            4,
        )
        self.check_batch_rank_sum(
            input_queue,
            mock_batch_data_elements,
            output_data_elements,
            output_queue,
            mock_batch,
            mock_batch_udf,
            40,
            50,
            5,
        )

        for i in range(50, 57):
            input_queue.put(mock_batch_data_elements[i])
        input_queue.put(mock_end_of_upstream)

        rank_sum_real = 0
        rank_sum_suppose = 0
        for i in range(50, 57):
            output_data_elements.append(output_queue.get())
            rank_sum_real += output_data_elements[i].payload.frame[0]["test-2"]
            rank_sum_suppose += mock_batch[i]["test-2"]
        assert rank_sum_real == rank_sum_suppose

        # check the batch count
        assert mock_batch_udf.count == 6

        assert output_data_elements[0].tag == mock_receiver_actor
        assert isinstance(output_data_elements[0].payload, OutputDataFrame)
        data_frame: OutputDataFrame = output_data_elements[0].payload
        assert len(data_frame.frame) == 1
        assert data_frame.frame[0] == Tuple(mock_batch[0])

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
        # can process EndOfUpstream
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

        # can process QueryStatistics
        input_queue.put(mock_query_statistics)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=1,
                    control_return=ControlReturnV2(
                        worker_statistics=WorkerStatistics(
                            worker_state=WorkerState.COMPLETED,
                            input_tuple_count=57,
                            output_tuple_count=57,
                        )
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

    @pytest.mark.timeout(5)
    def test_batch_dp_thread_can_process_batch_complex(
        self,
        mock_batch_udf,
        mock_link,
        mock_receiver_actor,
        mock_controller,
        input_queue,
        output_queue,
        mock_batch_data_elements,
        batch_dp_thread,
        mock_update_input_linking,
        mock_add_partitioning,
        mock_end_of_upstream,
        mock_query_statistics,
        mock_batch,
        command_sequence,
        reraise,
    ):
        self.test_initial_process(
            batch_dp_thread,
            input_queue,
            mock_update_input_linking,
            output_queue,
            mock_controller,
            command_sequence,
            mock_add_partitioning,
        )

        output_data_elements = []
        # can process a InputDataFrame
        mock_batch_udf.BATCH_SIZE = 10
        for i in range(13):
            input_queue.put(mock_batch_data_elements[i])
        for i in range(10):
            output_data_elements.append(output_queue.get())
        assert mock_batch_udf.count == 1
        # input queue 13, output queue 10, batch_buffer 3
        mock_batch_udf.BATCH_SIZE = 20
        for i in range(13, 41):
            input_queue.put(mock_batch_data_elements[i])
        for i in range(20):
            output_data_elements.append(output_queue.get())
        assert mock_batch_udf.count == 2
        # input: 41, output: 30, in buffer: 11
        mock_batch_udf.BATCH_SIZE = 5
        input_queue.put(mock_batch_data_elements[41])
        for i in range(5):
            output_data_elements.append(output_queue.get())
        assert mock_batch_udf.count == 3
        input_queue.put(mock_batch_data_elements[42])
        for i in range(5):
            output_data_elements.append(output_queue.get())
        assert mock_batch_udf.count == 4
        # input queue 43, output queue 40, batch_buffer 3
        for i in range(43, 57):
            input_queue.put(mock_batch_data_elements[i])
        for i in range(15):
            output_data_elements.append(output_queue.get())
        # input queue 57, output queue 55, batch_buffer 2
        assert mock_batch_udf.count == 7

        input_queue.put(mock_end_of_upstream)
        for i in range(2):
            output_data_elements.append(output_queue.get())

        # check the batch count
        assert mock_batch_udf.count == 8

        assert output_data_elements[0].tag == mock_receiver_actor
        assert isinstance(output_data_elements[0].payload, OutputDataFrame)
        data_frame: OutputDataFrame = output_data_elements[0].payload
        assert len(data_frame.frame) == 1
        assert data_frame.frame[0] == Tuple(mock_batch[0])

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
        # can process EndOfUpstream
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

        # can process QueryStatistics
        input_queue.put(mock_query_statistics)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=1,
                    control_return=ControlReturnV2(
                        worker_statistics=WorkerStatistics(
                            worker_state=WorkerState.COMPLETED,
                            input_tuple_count=57,
                            output_tuple_count=57,
                        )
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

    @pytest.mark.timeout(2)
    def test_batch_dp_thread_can_process_batch_edge_case_string(
        self,
        reraise,
    ):
        with pytest.raises(ValueError) as exc_info:
            CountBatchOperator("test")
            assert (
                exc_info.value.args[0]
                == "BATCH_SIZE cannot be " + str(type("test")) + "."
            )

        reraise()

    @pytest.mark.timeout(2)
    def test_batch_dp_thread_can_process_batch_edge_case_non_positive(
        self,
        reraise,
    ):
        with pytest.raises(ValueError) as exc_info:
            CountBatchOperator(-20)
            assert exc_info.value.args[0] == "BATCH_SIZE should be positive."

        reraise()

    @pytest.mark.timeout(2)
    def test_batch_dp_thread_can_process_batch_edge_case_none(
        self,
        reraise,
    ):
        with pytest.raises(ValueError) as exc_info:
            CountBatchOperator(None)
            assert exc_info.value.args[0] == "BATCH_SIZE cannot be None."

        reraise()
