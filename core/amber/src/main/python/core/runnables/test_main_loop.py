import inspect
from threading import Thread

import pandas
import pyarrow
import pytest

from core.models import (
    InputDataFrame,
    OutputDataFrame,
    EndOfUpstream,
    InternalQueue,
    Tuple,
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
    InitializeOperatorLogicV2,
    PauseWorkerV2,
    ResumeWorkerV2,
)
from proto.edu.uci.ics.amber.engine.common import (
    ActorVirtualIdentity,
    ControlInvocationV2,
    ControlPayloadV2,
    LayerIdentity,
    LinkIdentity,
    ReturnInvocationV2,
)
from pytexera.udf.examples.count_batch_operator import CountBatchOperator
from pytexera.udf.examples.echo_operator import EchoOperator


class TestMainLoop:
    @pytest.fixture
    def command_sequence(self):
        return 1

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
    def mock_raw_schema(self):
        return {"test-1": "string", "test-2": "integer"}

    @pytest.fixture
    def mock_initialize_operator_logic(
        self,
        mock_controller,
        mock_sender_actor,
        mock_link,
        command_sequence,
        mock_raw_schema,
    ):
        command = set_one_of(
            ControlCommandV2,
            InitializeOperatorLogicV2(
                code="from pytexera import *\n" + inspect.getsource(EchoOperator),
                is_source=False,
                output_schema=mock_raw_schema,
                upstream_link_ids=[mock_link],
            ),
        )
        payload = set_one_of(
            ControlPayloadV2,
            ControlInvocationV2(command_id=command_sequence, command=command),
        )
        return ControlElement(tag=mock_controller, payload=payload)

    @pytest.fixture
    def mock_initialize_batch_count_operator_logic(
        self,
        mock_controller,
        mock_sender_actor,
        mock_link,
        command_sequence,
        mock_raw_schema,
    ):
        command = set_one_of(
            ControlCommandV2,
            InitializeOperatorLogicV2(
                code="from pytexera import *\n" + inspect.getsource(CountBatchOperator),
                is_source=False,
                output_schema=mock_raw_schema,
                upstream_link_ids=[mock_link],
            ),
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
    def mock_pause(self, mock_controller, mock_sender_actor, command_sequence):
        command = set_one_of(ControlCommandV2, PauseWorkerV2())
        payload = set_one_of(
            ControlPayloadV2,
            ControlInvocationV2(command_id=command_sequence, command=command),
        )
        return ControlElement(tag=mock_controller, payload=payload)

    @pytest.fixture
    def mock_resume(self, mock_controller, mock_sender_actor, command_sequence):
        command = set_one_of(ControlCommandV2, ResumeWorkerV2())
        payload = set_one_of(
            ControlPayloadV2,
            ControlInvocationV2(command_id=command_sequence, command=command),
        )
        return ControlElement(tag=mock_controller, payload=payload)

    @pytest.fixture
    def main_loop(self, input_queue, output_queue, mock_link):
        main_loop = MainLoop(input_queue, output_queue)
        yield main_loop
        main_loop.stop()

    @pytest.fixture
    def main_loop_thread(self, main_loop, reraise):
        def wrapper():
            with reraise:
                main_loop.run()

        main_loop_thread = Thread(target=wrapper, name="main_loop_thread")
        yield main_loop_thread

    @staticmethod
    def check_batch_rank_sum(
        operator,
        input_queue,
        mock_batch_data_elements,
        output_data_elements,
        output_queue,
        mock_batch,
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
        assert operator.count == count
        assert rank_sum_real == rank_sum_suppose

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
        mock_initialize_operator_logic,
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

        # can process InitializeOperatorLogic
        input_queue.put(mock_initialize_operator_logic)
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
    def test_batch_dp_thread_can_process_batch(
        self,
        mock_controller,
        mock_link,
        input_queue,
        output_queue,
        mock_receiver_actor,
        main_loop,
        main_loop_thread,
        mock_query_statistics,
        mock_update_input_linking,
        mock_add_partitioning,
        mock_pause,
        mock_resume,
        mock_initialize_batch_count_operator_logic,
        mock_batch,
        mock_batch_data_elements,
        mock_end_of_upstream,
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

        # can process InitializeOperatorLogic
        input_queue.put(mock_initialize_batch_count_operator_logic)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=command_sequence,
                    control_return=ControlReturnV2(),
                )
            ),
        )
        operator = main_loop.context.operator_manager.operator
        output_data_elements = []

        # can process a InputDataFrame
        operator.BATCH_SIZE = 10
        for i in range(13):
            input_queue.put(mock_batch_data_elements[i])
        for i in range(10):
            output_data_elements.append(output_queue.get())

        self.send_pause(
            command_sequence, input_queue, mock_controller, mock_pause, output_queue
        )
        # input queue 13, output queue 10, batch_buffer 3
        assert operator.count == 1
        operator.BATCH_SIZE = 20
        self.send_resume(
            command_sequence, input_queue, mock_controller, mock_resume, output_queue
        )

        for i in range(13, 41):
            input_queue.put(mock_batch_data_elements[i])
        for i in range(20):
            output_data_elements.append(output_queue.get())

        self.send_pause(
            command_sequence, input_queue, mock_controller, mock_pause, output_queue
        )
        # input queue 41, output queue 30, batch_buffer 11
        assert operator.count == 2
        operator.BATCH_SIZE = 5
        self.send_resume(
            command_sequence, input_queue, mock_controller, mock_resume, output_queue
        )

        input_queue.put(mock_batch_data_elements[41])
        input_queue.put(mock_batch_data_elements[42])
        for i in range(10):
            output_data_elements.append(output_queue.get())

        self.send_pause(
            command_sequence, input_queue, mock_controller, mock_pause, output_queue
        )
        # input queue 43, output queue 40, batch_buffer 3
        assert operator.count == 4
        self.send_resume(
            command_sequence, input_queue, mock_controller, mock_resume, output_queue
        )

        for i in range(43, 57):
            input_queue.put(mock_batch_data_elements[i])
        for i in range(15):
            output_data_elements.append(output_queue.get())

        self.send_pause(
            command_sequence, input_queue, mock_controller, mock_pause, output_queue
        )
        # input queue 57, output queue 55, batch_buffer 2
        assert operator.count == 7
        self.send_resume(
            command_sequence, input_queue, mock_controller, mock_resume, output_queue
        )

        input_queue.put(mock_end_of_upstream)
        for i in range(2):
            output_data_elements.append(output_queue.get())

        # check the batch count
        assert main_loop.context.operator_manager.operator.count == 8

        assert output_data_elements[0].tag == mock_receiver_actor
        assert isinstance(output_data_elements[0].payload, OutputDataFrame)
        data_frame: OutputDataFrame = output_data_elements[0].payload
        assert len(data_frame.frame) == 1
        assert data_frame.frame[0] == Tuple(mock_batch[0])

        reraise()

    @staticmethod
    def send_pause(
        command_sequence, input_queue, mock_controller, mock_pause, output_queue
    ):
        input_queue.put(mock_pause)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=command_sequence,
                    control_return=ControlReturnV2(worker_state=WorkerState.PAUSED),
                )
            ),
        )

    @staticmethod
    def send_resume(
        command_sequence, input_queue, mock_controller, mock_resume, output_queue
    ):
        input_queue.put(mock_resume)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=command_sequence,
                    control_return=ControlReturnV2(worker_state=WorkerState.RUNNING),
                )
            ),
        )
