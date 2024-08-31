import inspect
from threading import Thread

import pandas
import pyarrow
import pytest

from core.models import (
    DataFrame,
    MarkerFrame,
    InternalQueue,
    Tuple,
)
from core.models.internal_queue import DataElement, ControlElement
from core.models.marker import EndOfUpstream
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
    AddInputChannelV2,
    WorkerExecutionCompletedV2,
    WorkerMetrics,
    WorkerState,
    WorkerStatistics,
    PortCompletedV2,
    InitializeExecutorV2,
    PauseWorkerV2,
    ResumeWorkerV2,
    AssignPortV2,
    PortTupleCountMapping,
)
from proto.edu.uci.ics.amber.engine.common import (
    ActorVirtualIdentity,
    ControlInvocationV2,
    ControlPayloadV2,
    PhysicalLink,
    ReturnInvocationV2,
    PhysicalOpIdentity,
    OperatorIdentity,
    ChannelIdentity,
    PortIdentity,
)
from pytexera.udf.examples.count_batch_operator import CountBatchOperator
from pytexera.udf.examples.echo_operator import EchoOperator


class TestMainLoop:
    @pytest.fixture
    def command_sequence(self):
        return 1

    @pytest.fixture
    def mock_link(self):
        return PhysicalLink(
            from_op_id=PhysicalOpIdentity(OperatorIdentity("from"), "from"),
            from_port_id=PortIdentity(0, internal=False),
            to_op_id=PhysicalOpIdentity(OperatorIdentity("to"), "to"),
            to_port_id=PortIdentity(0, internal=False),
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
            payload=DataFrame(
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
                    payload=DataFrame(
                        frame=pyarrow.Table.from_pandas(
                            pandas.DataFrame([mock_tuple.as_dict()])
                        )
                    ),
                )
            )

        return data_elements

    @pytest.fixture
    def mock_end_of_upstream(self, mock_tuple, mock_sender_actor):
        return DataElement(tag=mock_sender_actor, payload=MarkerFrame(EndOfUpstream()))

    @pytest.fixture
    def input_queue(self):
        return InternalQueue()

    @pytest.fixture
    def output_queue(self):
        return InternalQueue()

    @pytest.fixture
    def mock_assign_input_port(
        self, mock_raw_schema, mock_controller, mock_link, command_sequence
    ):
        command = set_one_of(
            ControlCommandV2,
            AssignPortV2(
                port_id=mock_link.to_port_id, input=True, schema=mock_raw_schema
            ),
        )
        payload = set_one_of(
            ControlPayloadV2,
            ControlInvocationV2(command_id=command_sequence, command=command),
        )
        return ControlElement(tag=mock_controller, payload=payload)

    @pytest.fixture
    def mock_assign_output_port(
        self, mock_raw_schema, mock_controller, command_sequence
    ):
        command = set_one_of(
            ControlCommandV2,
            AssignPortV2(
                port_id=PortIdentity(id=0), input=False, schema=mock_raw_schema
            ),
        )
        payload = set_one_of(
            ControlPayloadV2,
            ControlInvocationV2(command_id=command_sequence, command=command),
        )
        return ControlElement(tag=mock_controller, payload=payload)

    @pytest.fixture
    def mock_add_input_channel(
        self,
        mock_controller,
        mock_sender_actor,
        mock_receiver_actor,
        mock_link,
        command_sequence,
    ):
        command = set_one_of(
            ControlCommandV2,
            AddInputChannelV2(
                ChannelIdentity(
                    from_worker_id=mock_sender_actor,
                    to_worker_id=mock_receiver_actor,
                    is_control=False,
                ),
                port_id=mock_link.to_port_id,
            ),
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
    def mock_initialize_executor(
        self,
        mock_controller,
        mock_sender_actor,
        mock_link,
        command_sequence,
        mock_raw_schema,
    ):
        command = set_one_of(
            ControlCommandV2,
            InitializeExecutorV2(
                code="from pytexera import *\n" + inspect.getsource(EchoOperator),
                is_source=False,
            ),
        )
        payload = set_one_of(
            ControlPayloadV2,
            ControlInvocationV2(command_id=command_sequence, command=command),
        )
        return ControlElement(tag=mock_controller, payload=payload)

    @pytest.fixture
    def mock_initialize_batch_count_executor(
        self,
        mock_controller,
        mock_sender_actor,
        mock_link,
        command_sequence,
        mock_raw_schema,
    ):
        command = set_one_of(
            ControlCommandV2,
            InitializeExecutorV2(
                code="from pytexera import *\n" + inspect.getsource(CountBatchOperator),
                is_source=False,
            ),
        )
        payload = set_one_of(
            ControlPayloadV2,
            ControlInvocationV2(command_id=command_sequence, command=command),
        )
        return ControlElement(tag=mock_controller, payload=payload)

    @pytest.fixture
    def mock_add_partitioning(
        self, mock_controller, mock_receiver_actor, command_sequence, mock_link
    ):
        command = set_one_of(
            ControlCommandV2,
            AddPartitioningV2(
                tag=mock_link,
                partitioning=set_one_of(
                    Partitioning,
                    OneToOnePartitioning(
                        batch_size=1,
                        channels=[
                            ChannelIdentity(
                                from_worker_id=ActorVirtualIdentity("dummy_worker_id"),
                                to_worker_id=mock_receiver_actor,
                                is_control=False,
                            )
                        ],
                    ),
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
        main_loop = MainLoop("dummy_worker_id", input_queue, output_queue)
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
        executor,
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
        assert executor.count == count
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
        mock_assign_input_port,
        mock_assign_output_port,
        mock_add_input_channel,
        mock_add_partitioning,
        mock_initialize_executor,
        mock_end_of_upstream,
        mock_query_statistics,
        mock_tuple,
        command_sequence,
        reraise,
    ):
        main_loop_thread.start()

        # can process AssignPort
        input_queue.put(mock_assign_input_port)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=command_sequence,
                    control_return=ControlReturnV2(),
                )
            ),
        )
        input_queue.put(mock_assign_output_port)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=command_sequence,
                    control_return=ControlReturnV2(),
                )
            ),
        )

        # can process AddInputChannel
        input_queue.put(mock_add_input_channel)

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

        # can process InitializeExecutor
        input_queue.put(mock_initialize_executor)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=command_sequence,
                    control_return=ControlReturnV2(),
                )
            ),
        )

        # can process a DataFrame
        input_queue.put(mock_data_element)

        output_data_element: DataElement = output_queue.get()
        assert output_data_element.tag == mock_receiver_actor
        assert isinstance(output_data_element.payload, DataFrame)
        data_frame: DataFrame = output_data_element.payload
        assert len(data_frame.frame) == 1
        assert Tuple(data_frame.frame.to_pylist()[0]) == mock_tuple

        # can process QueryStatistics
        input_queue.put(mock_query_statistics)
        elem = output_queue.get()
        stats_invocation = elem.payload.return_invocation
        stats = stats_invocation.control_return.worker_metrics.worker_statistics
        assert elem == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=1,
                    control_return=ControlReturnV2(
                        worker_metrics=WorkerMetrics(
                            worker_state=WorkerState.RUNNING,
                            worker_statistics=WorkerStatistics(
                                input_tuple_count=[
                                    PortTupleCountMapping(PortIdentity(0), 1)
                                ],
                                output_tuple_count=[
                                    PortTupleCountMapping(PortIdentity(0), 1)
                                ],
                                data_processing_time=stats.data_processing_time,
                                control_processing_time=stats.control_processing_time,
                                idle_time=stats.idle_time,
                            ),
                        )
                    ),
                )
            ),
        )

        # can process EndOfUpstream
        input_queue.put(mock_end_of_upstream)

        # the input port should complete
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                control_invocation=ControlInvocationV2(
                    command_id=0,
                    command=ControlCommandV2(
                        port_completed=PortCompletedV2(
                            port_id=mock_link.to_port_id, input=True
                        )
                    ),
                )
            ),
        )

        # the output port should complete
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                control_invocation=ControlInvocationV2(
                    command_id=1,
                    command=ControlCommandV2(
                        port_completed=PortCompletedV2(
                            port_id=PortIdentity(id=0), input=False
                        )
                    ),
                )
            ),
        )

        # WorkerExecutionCompletedV2 should be triggered when workflow finishes
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                control_invocation=ControlInvocationV2(
                    command_id=2,
                    command=ControlCommandV2(
                        worker_execution_completed=WorkerExecutionCompletedV2()
                    ),
                )
            ),
        )

        assert output_queue.get() == DataElement(
            tag=mock_receiver_actor, payload=MarkerFrame(EndOfUpstream())
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
        mock_assign_input_port,
        mock_assign_output_port,
        mock_add_input_channel,
        mock_add_partitioning,
        mock_pause,
        mock_resume,
        mock_initialize_batch_count_executor,
        mock_batch,
        mock_batch_data_elements,
        mock_end_of_upstream,
        command_sequence,
        reraise,
    ):
        main_loop_thread.start()

        # can process AssignPort
        input_queue.put(mock_assign_input_port)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=command_sequence,
                    control_return=ControlReturnV2(),
                )
            ),
        )
        input_queue.put(mock_assign_output_port)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=command_sequence,
                    control_return=ControlReturnV2(),
                )
            ),
        )

        # can process AddInputChannel
        input_queue.put(mock_add_input_channel)
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

        # can process InitializeExecutor
        input_queue.put(mock_initialize_batch_count_executor)
        assert output_queue.get() == ControlElement(
            tag=mock_controller,
            payload=ControlPayloadV2(
                return_invocation=ReturnInvocationV2(
                    original_command_id=command_sequence,
                    control_return=ControlReturnV2(),
                )
            ),
        )
        executor = main_loop.context.executor_manager.executor
        output_data_elements = []

        # can process a DataFrame
        executor.BATCH_SIZE = 10
        for i in range(13):
            input_queue.put(mock_batch_data_elements[i])
        for i in range(10):
            output_data_elements.append(output_queue.get())

        self.send_pause(
            command_sequence, input_queue, mock_controller, mock_pause, output_queue
        )
        # input queue 13, output queue 10, batch_buffer 3
        assert executor.count == 1
        executor.BATCH_SIZE = 20
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
        assert executor.count == 2
        executor.BATCH_SIZE = 5
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
        assert executor.count == 4
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
        assert executor.count == 7
        self.send_resume(
            command_sequence, input_queue, mock_controller, mock_resume, output_queue
        )

        input_queue.put(mock_end_of_upstream)
        for i in range(2):
            output_data_elements.append(output_queue.get())

        # check the batch count
        assert main_loop.context.executor_manager.executor.count == 8

        assert output_data_elements[0].tag == mock_receiver_actor
        assert isinstance(output_data_elements[0].payload, DataFrame)
        data_frame: DataFrame = output_data_elements[0].payload
        assert len(data_frame.frame) == 1
        assert Tuple(data_frame.frame.to_pylist()[0]) == Tuple(mock_batch[0])

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
