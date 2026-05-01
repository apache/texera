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

import inspect
import pandas
import pickle
import pyarrow
import pytest
import time
from threading import Thread

from core.models import (
    DataFrame,
    InternalQueue,
    State,
    StateFrame,
    Tuple,
)
from core.models.internal_queue import (
    DataElement,
    DCMElement,
    ECMElement,
)
from core.runnables import MainLoop
from core.util import set_one_of
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    PhysicalLink,
    PhysicalOpIdentity,
    OperatorIdentity,
    ChannelIdentity,
    PortIdentity,
    OpExecWithCode,
    OpExecInitInfo,
    EmbeddedControlMessageIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    ControlRequest,
    AssignPortRequest,
    ControlInvocation,
    AddInputChannelRequest,
    InitializeExecutorRequest,
    EmptyReturn,
    ReturnInvocation,
    ControlReturn,
    WorkerMetricsResponse,
    AddPartitioningRequest,
    EmptyRequest,
    PortCompletedRequest,
    AsyncRpcContext,
    WorkerStateResponse,
    EmbeddedControlMessageType,
    EmbeddedControlMessage,
)
from proto.org.apache.texera.amber.engine.architecture.sendsemantics import (
    OneToOnePartitioning,
    Partitioning,
)
from proto.org.apache.texera.amber.engine.architecture.worker import (
    WorkerMetrics,
    WorkerState,
    WorkerStatistics,
    PortTupleMetricsMapping,
    TupleMetrics,
)
from proto.org.apache.texera.amber.engine.common import DirectControlMessagePayloadV2
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
    def mock_binary_tuple(self):
        return Tuple({"test-1": [1, 2, 3, 4], "test-2": 10})

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
    def mock_data_input_channel(self):
        return ChannelIdentity(
            ActorVirtualIdentity("sender"),
            ActorVirtualIdentity("dummy_worker_id"),
            False,
        )

    @pytest.fixture
    def mock_data_output_channel(self):
        return ChannelIdentity(
            ActorVirtualIdentity("dummy_worker_id"),
            ActorVirtualIdentity("dummy_worker_id"),
            False,
        )

    @pytest.fixture
    def mock_control_input_channel(self):
        return ChannelIdentity(
            ActorVirtualIdentity("CONTROLLER"),
            ActorVirtualIdentity("dummy_worker_id"),
            True,
        )

    @pytest.fixture
    def mock_control_output_channel(self):
        return ChannelIdentity(
            ActorVirtualIdentity("dummy_worker_id"),
            ActorVirtualIdentity("CONTROLLER"),
            True,
        )

    @pytest.fixture
    def mock_receiver_actor(self):
        return ActorVirtualIdentity("dummy_worker_id")

    @pytest.fixture
    def mock_data_element(self, mock_tuple, mock_data_input_channel):
        return DataElement(
            tag=mock_data_input_channel,
            payload=DataFrame(
                frame=pyarrow.Table.from_pandas(
                    pandas.DataFrame([mock_tuple.as_dict()])
                )
            ),
        )

    @pytest.fixture
    def mock_state_data_elements(self, mock_data_input_channel):
        elements = []
        for value in (1, 2, 3, 4):
            state = State()
            state.add("value", value)
            elements.append(
                DataElement(
                    tag=mock_data_input_channel,
                    payload=StateFrame(frame=state),
                )
            )
        return elements

    @pytest.fixture
    def state_processing_executor(self):
        # In-process executor for the state-pipeline tests. Tags processed
        # states with `processed_marker` and emits a finish-marker state
        # from `produce_state_on_finish` so EndChannel handling can be
        # observed.
        class StateProcessingExecutor:
            @staticmethod
            def process_tuple(tuple_, port):
                yield tuple_

            @staticmethod
            def process_state(state: State, port: int) -> State:
                new_state = State()
                for key, value in state.__dict__.items():
                    if key != "schema":
                        new_state.add(key, value)
                new_state.add("processed_marker", "executed")
                new_state.add("port", port)
                return new_state

            @staticmethod
            def produce_state_on_finish(port: int) -> State:
                finish_state = State()
                finish_state.add("finish_marker", "produce_state_on_finish_ran")
                return finish_state

            @staticmethod
            def on_finish(port):
                yield

            @staticmethod
            def close():
                pass

        return StateProcessingExecutor()

    @pytest.fixture
    def mock_binary_data_element(self, mock_binary_tuple, mock_data_input_channel):
        return DataElement(
            tag=mock_data_input_channel,
            payload=DataFrame(
                frame=pyarrow.Table.from_pandas(
                    pandas.DataFrame([mock_binary_tuple.as_dict()])
                )
            ),
        )

    @pytest.fixture
    def mock_batch_data_elements(self, mock_batch, mock_data_input_channel):
        data_elements = []
        for i in range(57):
            mock_tuple = Tuple({"test-1": "hello", "test-2": i})
            data_elements.append(
                DataElement(
                    tag=mock_data_input_channel,
                    payload=DataFrame(
                        frame=pyarrow.Table.from_pandas(
                            pandas.DataFrame([mock_tuple.as_dict()])
                        )
                    ),
                )
            )

        return data_elements

    @pytest.fixture
    def mock_end_of_upstream(self, mock_tuple, mock_data_input_channel):
        return ECMElement(
            tag=mock_data_input_channel,
            payload=EmbeddedControlMessage(
                EmbeddedControlMessageIdentity("EndChannel"),
                EmbeddedControlMessageType.PORT_ALIGNMENT,
                [],
                {
                    mock_data_input_channel.to_worker_id.name: ControlInvocation(
                        "EndChannel",
                        ControlRequest(empty_request=EmptyRequest()),
                        AsyncRpcContext(ActorVirtualIdentity(), ActorVirtualIdentity()),
                        -1,
                    )
                },
            ),
        )

    @pytest.fixture
    def input_queue(self):
        return InternalQueue()

    @pytest.fixture
    def output_queue(self):
        return InternalQueue()

    @pytest.fixture
    def mock_assign_input_port(
        self, mock_raw_schema, mock_control_input_channel, mock_link, command_sequence
    ):
        command = set_one_of(
            ControlRequest,
            AssignPortRequest(
                port_id=mock_link.to_port_id, input=True, schema=mock_raw_schema
            ),
        )
        payload = set_one_of(
            DirectControlMessagePayloadV2,
            ControlInvocation(
                method_name="AssignPort", command_id=command_sequence, command=command
            ),
        )
        return DCMElement(tag=mock_control_input_channel, payload=payload)

    @pytest.fixture
    def mock_assign_output_port(
        self, mock_raw_schema, mock_control_input_channel, command_sequence
    ):
        command = set_one_of(
            ControlRequest,
            AssignPortRequest(
                port_id=PortIdentity(id=0), input=False, schema=mock_raw_schema
            ),
        )
        payload = set_one_of(
            DirectControlMessagePayloadV2,
            ControlInvocation(
                method_name="AssignPort", command_id=command_sequence, command=command
            ),
        )
        return DCMElement(tag=mock_control_input_channel, payload=payload)

    @pytest.fixture
    def mock_assign_input_port_binary(
        self,
        mock_binary_raw_schema,
        mock_control_input_channel,
        mock_link,
        command_sequence,
    ):
        command = set_one_of(
            ControlRequest,
            AssignPortRequest(
                port_id=mock_link.to_port_id, input=True, schema=mock_binary_raw_schema
            ),
        )
        payload = set_one_of(
            DirectControlMessagePayloadV2,
            ControlInvocation(
                method_name="AssignPort", command_id=command_sequence, command=command
            ),
        )
        return DCMElement(tag=mock_control_input_channel, payload=payload)

    @pytest.fixture
    def mock_assign_output_port_binary(
        self, mock_binary_raw_schema, mock_control_input_channel, command_sequence
    ):
        command = set_one_of(
            ControlRequest,
            AssignPortRequest(
                port_id=PortIdentity(id=0), input=False, schema=mock_binary_raw_schema
            ),
        )
        payload = set_one_of(
            DirectControlMessagePayloadV2,
            ControlInvocation(
                method_name="AssignPort", command_id=command_sequence, command=command
            ),
        )
        return DCMElement(tag=mock_control_input_channel, payload=payload)

    @pytest.fixture
    def mock_add_input_channel(
        self,
        mock_control_input_channel,
        mock_sender_actor,
        mock_receiver_actor,
        mock_link,
        command_sequence,
    ):
        command = set_one_of(
            ControlRequest,
            AddInputChannelRequest(
                ChannelIdentity(
                    from_worker_id=mock_sender_actor,
                    to_worker_id=mock_receiver_actor,
                    is_control=False,
                ),
                port_id=mock_link.to_port_id,
            ),
        )
        payload = set_one_of(
            DirectControlMessagePayloadV2,
            ControlInvocation(
                method_name="AddInputChannel",
                command_id=command_sequence,
                command=command,
            ),
        )
        return DCMElement(tag=mock_control_input_channel, payload=payload)

    @pytest.fixture
    def mock_raw_schema(self):
        return {"test-1": "STRING", "test-2": "INTEGER"}

    @pytest.fixture
    def mock_binary_raw_schema(self):
        return {"test-1": "BINARY", "test-2": "INTEGER"}

    @pytest.fixture
    def mock_initialize_executor(
        self,
        mock_control_input_channel,
        mock_sender_actor,
        mock_link,
        command_sequence,
        mock_raw_schema,
    ):
        operator_code = "from pytexera import *\n" + inspect.getsource(EchoOperator)
        command = set_one_of(
            ControlRequest,
            InitializeExecutorRequest(
                op_exec_init_info=set_one_of(
                    OpExecInitInfo, OpExecWithCode(operator_code, "python")
                ),
                is_source=False,
            ),
        )
        payload = set_one_of(
            DirectControlMessagePayloadV2,
            ControlInvocation(
                method_name="InitializeExecutor",
                command_id=command_sequence,
                command=command,
            ),
        )
        return DCMElement(tag=mock_control_input_channel, payload=payload)

    @pytest.fixture
    def mock_initialize_batch_count_executor(
        self,
        mock_control_input_channel,
        mock_sender_actor,
        mock_link,
        command_sequence,
        mock_raw_schema,
    ):
        operator_code = "from pytexera import *\n" + inspect.getsource(
            CountBatchOperator
        )
        command = set_one_of(
            ControlRequest,
            InitializeExecutorRequest(
                op_exec_init_info=set_one_of(
                    OpExecInitInfo, OpExecWithCode(operator_code, "python")
                ),
                is_source=False,
            ),
        )
        payload = set_one_of(
            DirectControlMessagePayloadV2,
            ControlInvocation(
                method_name="InitializeExecutor",
                command_id=command_sequence,
                command=command,
            ),
        )
        return DCMElement(tag=mock_control_input_channel, payload=payload)

    @pytest.fixture
    def mock_add_partitioning(
        self,
        mock_control_input_channel,
        mock_receiver_actor,
        command_sequence,
        mock_link,
    ):
        command = set_one_of(
            ControlRequest,
            AddPartitioningRequest(
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
            DirectControlMessagePayloadV2,
            ControlInvocation(
                method_name="AddPartitioning",
                command_id=command_sequence,
                command=command,
            ),
        )
        return DCMElement(tag=mock_control_input_channel, payload=payload)

    @pytest.fixture
    def mock_query_statistics(
        self, mock_control_input_channel, mock_sender_actor, command_sequence
    ):
        command = set_one_of(ControlRequest, EmptyRequest())
        payload = set_one_of(
            DirectControlMessagePayloadV2,
            ControlInvocation(
                method_name="QueryStatistics",
                command_id=command_sequence,
                command=command,
            ),
        )
        return DCMElement(tag=mock_control_input_channel, payload=payload)

    @pytest.fixture
    def mock_pause(
        self, mock_control_input_channel, mock_sender_actor, command_sequence
    ):
        command = set_one_of(ControlRequest, EmptyRequest())
        payload = set_one_of(
            DirectControlMessagePayloadV2,
            ControlInvocation(
                method_name="PauseWorker", command_id=command_sequence, command=command
            ),
        )
        return DCMElement(tag=mock_control_input_channel, payload=payload)

    @pytest.fixture
    def mock_resume(
        self, mock_control_input_channel, mock_sender_actor, command_sequence
    ):
        command = set_one_of(ControlRequest, EmptyRequest())
        payload = set_one_of(
            DirectControlMessagePayloadV2,
            ControlInvocation(
                method_name="ResumeWorker", command_id=command_sequence, command=command
            ),
        )
        return DCMElement(tag=mock_control_input_channel, payload=payload)

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
        mock_data_input_channel,
        mock_data_output_channel,
        mock_control_input_channel,
        mock_control_output_channel,
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
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )
        input_queue.put(mock_assign_output_port)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        # can process AddInputChannel
        input_queue.put(mock_add_input_channel)

        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        # can process AddPartitioning
        input_queue.put(mock_add_partitioning)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        # can process InitializeExecutor
        input_queue.put(mock_initialize_executor)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        # can process a DataFrame
        input_queue.put(mock_data_element)

        output_data_element: DataElement = output_queue.get()
        assert output_data_element.tag == mock_data_output_channel
        assert isinstance(output_data_element.payload, DataFrame)
        data_frame: DataFrame = output_data_element.payload
        assert len(data_frame.frame) == 1
        assert Tuple(data_frame.frame.to_pylist()[0]) == mock_tuple

        # can process QueryStatistics
        input_queue.put(mock_query_statistics)
        elem = output_queue.get()
        stats_invocation = elem.payload.return_invocation
        worker_metrics_response = stats_invocation.return_value.worker_metrics_response
        stats = worker_metrics_response.metrics.worker_statistics

        metrics = WorkerMetrics(
            worker_state=WorkerState.RUNNING,
            worker_statistics=WorkerStatistics(
                input_tuple_metrics=[
                    PortTupleMetricsMapping(
                        PortIdentity(0),
                        TupleMetrics(
                            1,
                            stats.input_tuple_metrics[0].tuple_metrics.size,
                        ),
                    )
                ],
                output_tuple_metrics=[
                    PortTupleMetricsMapping(
                        PortIdentity(0),
                        TupleMetrics(
                            1,
                            stats.output_tuple_metrics[0].tuple_metrics.size,
                        ),
                    )
                ],
                data_processing_time=stats.data_processing_time,
                control_processing_time=stats.control_processing_time,
                idle_time=stats.idle_time,
            ),
        )

        assert elem == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=1,
                    return_value=ControlReturn(
                        worker_metrics_response=WorkerMetricsResponse(metrics=metrics),
                    ),
                ),
            ),
        )

        input_queue.put(mock_end_of_upstream)
        output_queue.disable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)
        # the input port should complete
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                control_invocation=ControlInvocation(
                    method_name="PortCompleted",
                    command_id=0,
                    context=AsyncRpcContext(
                        sender=ActorVirtualIdentity(name="dummy_worker_id"),
                        receiver=ActorVirtualIdentity(name="CONTROLLER"),
                    ),
                    command=ControlRequest(
                        port_completed_request=PortCompletedRequest(
                            port_id=mock_link.to_port_id, input=True
                        )
                    ),
                )
            ),
        )

        # the output port should complete
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                control_invocation=ControlInvocation(
                    method_name="PortCompleted",
                    command_id=1,
                    context=AsyncRpcContext(
                        sender=ActorVirtualIdentity(name="dummy_worker_id"),
                        receiver=ActorVirtualIdentity(name="CONTROLLER"),
                    ),
                    command=ControlRequest(
                        port_completed_request=PortCompletedRequest(
                            port_id=PortIdentity(id=0), input=False
                        )
                    ),
                )
            ),
        )

        # WorkerExecutionCompletedV2 should be triggered when workflow finishes
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                control_invocation=ControlInvocation(
                    method_name="WorkerExecutionCompleted",
                    command_id=2,
                    context=AsyncRpcContext(
                        sender=ActorVirtualIdentity(name="dummy_worker_id"),
                        receiver=ActorVirtualIdentity(name="CONTROLLER"),
                    ),
                    command=ControlRequest(empty_request=EmptyRequest()),
                )
            ),
        )

        output_queue.enable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)
        assert output_queue.get() == ECMElement(
            tag=mock_data_output_channel,
            payload=EmbeddedControlMessage(
                EmbeddedControlMessageIdentity("EndChannel"),
                EmbeddedControlMessageType.PORT_ALIGNMENT,
                [],
                {
                    mock_data_output_channel.to_worker_id.name: ControlInvocation(
                        "EndChannel",
                        ControlRequest(empty_request=EmptyRequest()),
                        AsyncRpcContext(ActorVirtualIdentity(), ActorVirtualIdentity()),
                        -1,
                    )
                },
            ),
        )

        # can process ReturnInvocation
        input_queue.put(
            DCMElement(
                tag=mock_control_input_channel,
                payload=set_one_of(
                    DirectControlMessagePayloadV2,
                    ReturnInvocation(
                        command_id=0,
                        return_value=ControlReturn(empty_return=EmptyReturn()),
                    ),
                ),
            )
        )

        reraise()

    @pytest.mark.timeout(5)
    def test_batch_dp_thread_can_process_batch(
        self,
        mock_control_input_channel,
        mock_control_output_channel,
        mock_data_input_channel,
        mock_data_output_channel,
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
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )
        input_queue.put(mock_assign_output_port)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        # can process AddInputChannel
        input_queue.put(mock_add_input_channel)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        # can process AddPartitioning
        input_queue.put(mock_add_partitioning)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        # can process InitializeExecutor
        input_queue.put(mock_initialize_batch_count_executor)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
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
            command_sequence,
            input_queue,
            mock_control_output_channel,
            mock_pause,
            output_queue,
        )
        # input queue 13, output queue 10, batch_buffer 3
        assert executor.count == 1
        executor.BATCH_SIZE = 20
        self.send_resume(
            command_sequence,
            input_queue,
            mock_control_output_channel,
            mock_resume,
            output_queue,
        )

        for i in range(13, 41):
            input_queue.put(mock_batch_data_elements[i])
        for i in range(20):
            output_data_elements.append(output_queue.get())

        self.send_pause(
            command_sequence,
            input_queue,
            mock_control_output_channel,
            mock_pause,
            output_queue,
        )
        # input queue 41, output queue 30, batch_buffer 11
        assert executor.count == 2
        executor.BATCH_SIZE = 5
        self.send_resume(
            command_sequence,
            input_queue,
            mock_control_output_channel,
            mock_resume,
            output_queue,
        )

        input_queue.put(mock_batch_data_elements[41])
        input_queue.put(mock_batch_data_elements[42])
        for i in range(10):
            output_data_elements.append(output_queue.get())

        self.send_pause(
            command_sequence,
            input_queue,
            mock_control_output_channel,
            mock_pause,
            output_queue,
        )
        # input queue 43, output queue 40, batch_buffer 3
        assert executor.count == 4
        self.send_resume(
            command_sequence,
            input_queue,
            mock_control_output_channel,
            mock_resume,
            output_queue,
        )

        for i in range(43, 57):
            input_queue.put(mock_batch_data_elements[i])
        for i in range(15):
            output_data_elements.append(output_queue.get())

        self.send_pause(
            command_sequence,
            input_queue,
            mock_control_output_channel,
            mock_pause,
            output_queue,
        )
        # input queue 57, output queue 55, batch_buffer 2
        assert executor.count == 7
        self.send_resume(
            command_sequence,
            input_queue,
            mock_control_output_channel,
            mock_resume,
            output_queue,
        )

        input_queue.put(mock_end_of_upstream)
        for i in range(2):
            output_data_elements.append(output_queue.get())

        # check the batch count
        assert main_loop.context.executor_manager.executor.count == 8

        assert output_data_elements[0].tag == mock_data_output_channel
        assert isinstance(output_data_elements[0].payload, DataFrame)
        data_frame: DataFrame = output_data_elements[0].payload
        assert len(data_frame.frame) == 1
        assert Tuple(data_frame.frame.to_pylist()[0]) == Tuple(mock_batch[0])

        reraise()

    @pytest.mark.timeout(5)
    def test_main_loop_thread_can_process_single_tuple_with_binary(
        self,
        mock_link,
        mock_data_input_channel,
        mock_data_output_channel,
        mock_control_output_channel,
        mock_control_input_channel,
        input_queue,
        output_queue,
        mock_binary_tuple,
        mock_binary_data_element,
        main_loop_thread,
        mock_assign_input_port_binary,
        mock_assign_output_port_binary,
        mock_add_input_channel,
        mock_add_partitioning,
        mock_initialize_executor,
        mock_end_of_upstream,
        mock_query_statistics,
        command_sequence,
        reraise,
    ):
        main_loop_thread.start()

        # can process AssignPort
        input_queue.put(mock_assign_input_port_binary)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )
        input_queue.put(mock_assign_output_port_binary)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        # can process AddInputChannel
        input_queue.put(mock_add_input_channel)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        # can process AddPartitioning
        input_queue.put(mock_add_partitioning)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        # can process InitializeExecutor
        input_queue.put(mock_initialize_executor)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        input_queue.put(mock_binary_data_element)
        output_data_element: DataElement = output_queue.get()
        assert output_data_element.tag == mock_data_output_channel
        assert isinstance(output_data_element.payload, DataFrame)
        data_frame: DataFrame = output_data_element.payload

        assert len(data_frame.frame) == 1
        assert data_frame.frame.to_pylist()[0][
            "test-1"
        ] == b"pickle    " + pickle.dumps(mock_binary_tuple["test-1"])

        reraise()

    @staticmethod
    def send_pause(
        command_sequence,
        input_queue,
        mock_control_output_channel,
        mock_pause,
        output_queue,
    ):
        input_queue.put(mock_pause)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(
                        worker_state_response=WorkerStateResponse(WorkerState.PAUSED)
                    ),
                )
            ),
        )

    @staticmethod
    def send_resume(
        command_sequence,
        input_queue,
        mock_control_output_channel,
        mock_resume,
        output_queue,
    ):
        input_queue.put(mock_resume)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(
                        worker_state_response=WorkerStateResponse(WorkerState.RUNNING)
                    ),
                )
            ),
        )

    @pytest.mark.timeout(5)
    def test_main_loop_thread_can_align_ecm(
        self,
        mock_link,
        mock_data_input_channel,
        mock_data_output_channel,
        mock_control_output_channel,
        mock_control_input_channel,
        input_queue,
        output_queue,
        mock_binary_tuple,
        mock_binary_data_element,
        main_loop_thread,
        mock_assign_input_port_binary,
        mock_assign_output_port_binary,
        mock_add_input_channel,
        mock_add_partitioning,
        mock_initialize_executor,
        mock_end_of_upstream,
        mock_query_statistics,
        command_sequence,
        reraise,
    ):
        main_loop_thread.start()

        # can process AssignPort
        input_queue.put(mock_assign_input_port_binary)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )
        input_queue.put(mock_assign_output_port_binary)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        # can process AddInputChannel
        input_queue.put(mock_add_input_channel)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        # can process AddPartitioning
        input_queue.put(mock_add_partitioning)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        # can process InitializeExecutor
        input_queue.put(mock_initialize_executor)
        assert output_queue.get() == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(empty_return=EmptyReturn()),
                )
            ),
        )

        scope = [mock_control_input_channel, mock_data_input_channel]
        command_mapping = {
            mock_control_input_channel.to_worker_id.name: ControlInvocation(
                "NoOperation", EmptyRequest(), AsyncRpcContext(), 98
            )
        }
        test_ecm = EmbeddedControlMessage(
            "test_ecm", EmbeddedControlMessageType.ALL_ALIGNMENT, scope, command_mapping
        )
        input_queue.put(ECMElement(tag=mock_control_input_channel, payload=test_ecm))
        input_queue.put(mock_binary_data_element)
        input_queue.put(ECMElement(tag=mock_data_input_channel, payload=test_ecm))

        # The two outputs land on different channel sub-queues:
        #   - DataElement on the data channel to the downstream worker
        #   - DCMElement (NoOperation reply) on the control channel back to "sender"
        # output_queue is a priority multi-queue. With both items present,
        # the control sub-queue (priority 1) outranks the data sub-queue
        # (priority 2), so the control reply must come out first. Wait for
        # both channels to have their item before popping, so the priority
        # guarantee is what we're actually testing — see #4524.
        control_reply_channel = ChannelIdentity(
            ActorVirtualIdentity("dummy_worker_id"),
            ActorVirtualIdentity("sender"),
            is_control=True,
        )

        def channel_size(channel: ChannelIdentity) -> int:
            # Sub-queues are added lazily on first put, so the channel may not
            # exist in the LBMQ yet. Treat that as size zero.
            if channel not in output_queue._queue.sub_queues:
                return 0
            return output_queue._queue.size(channel)

        deadline = time.time() + 5.0
        while channel_size(mock_data_output_channel) == 0 or (
            channel_size(control_reply_channel) == 0
        ):
            if time.time() > deadline:
                raise AssertionError(
                    f"timed out waiting for outputs on both channels; "
                    f"data={channel_size(mock_data_output_channel)}, "
                    f"control={channel_size(control_reply_channel)}"
                )
            time.sleep(0.001)

        # Priority pulls control before data when both are queued.
        output_control_element = output_queue.get()
        assert isinstance(output_control_element, DCMElement), (
            f"expected control reply first (priority), got {type(output_control_element).__name__}"
        )
        assert output_control_element.tag == control_reply_channel
        assert output_control_element.payload.return_invocation.command_id == 98
        assert (
            output_control_element.payload.return_invocation.return_value
            == ControlReturn(empty_return=EmptyReturn())
        )

        output_data_element = output_queue.get()
        assert isinstance(output_data_element, DataElement), (
            f"expected data element second, got {type(output_data_element).__name__}"
        )
        assert output_data_element.tag == mock_data_output_channel
        assert isinstance(output_data_element.payload, DataFrame)
        data_frame: DataFrame = output_data_element.payload
        assert len(data_frame.frame) == 1
        assert data_frame.frame.to_pylist()[0][
            "test-1"
        ] == b"pickle    " + pickle.dumps(mock_binary_tuple["test-1"])
        reraise()

    @pytest.mark.timeout(2)
    def test_process_state_can_emit_multiple_states(
        self,
        main_loop,
        output_queue,
        mock_data_output_channel,
        monkeypatch,
    ):
        # Stub-level coverage of the single-switch state handshake. Each
        # call to the (stubbed) _switch_context simulates DataProc
        # consuming the queued input state and writing
        # current_output_state, mirroring what real DataProc.process_state
        # does between MainLoop's switches.
        class DummyExecutor:
            @staticmethod
            def process_state(state: State, port: int) -> State:
                output_state = State()
                output_state.add("value", state["value"] + 1)
                output_state.add("port", port)
                return output_state

        main_loop.context.executor_manager.executor = DummyExecutor()
        monkeypatch.setattr(main_loop, "_check_and_process_control", lambda: None)
        monkeypatch.setattr(
            main_loop.context.output_manager,
            "emit_state",
            lambda state: [(mock_data_output_channel.to_worker_id, StateFrame(state))],
        )

        def fake_switch_context():
            current_input_state = (
                main_loop.context.state_processing_manager.current_input_state
            )
            if current_input_state is not None:
                main_loop.context.state_processing_manager.current_output_state = (
                    DummyExecutor.process_state(current_input_state, 0)
                )

        monkeypatch.setattr(main_loop, "_switch_context", fake_switch_context)

        first_state = State()
        first_state.add("value", 1)
        second_state = State()
        second_state.add("value", 41)

        main_loop._process_state(first_state)
        main_loop._process_state(second_state)

        first_output: DataElement = output_queue.get()
        second_output: DataElement = output_queue.get()

        assert first_output.tag == mock_data_output_channel
        assert isinstance(first_output.payload, StateFrame)
        assert first_output.payload.frame["value"] == 2
        assert first_output.payload.frame["port"] == 0

        assert second_output.tag == mock_data_output_channel
        assert isinstance(second_output.payload, StateFrame)
        assert second_output.payload.frame["value"] == 42
        assert second_output.payload.frame["port"] == 0

    @pytest.mark.timeout(2)
    def test_main_loop_thread_can_process_state(
        self,
        mock_data_output_channel,
        mock_control_output_channel,
        input_queue,
        output_queue,
        main_loop,
        main_loop_thread,
        mock_assign_input_port,
        mock_assign_output_port,
        mock_add_input_channel,
        mock_add_partitioning,
        mock_initialize_executor,
        mock_state_data_elements,
        mock_end_of_upstream,
        state_processing_executor,
        command_sequence,
        reraise,
    ):
        # End-to-end coverage of the state-processing path through the real
        # MainLoop + DataProcessor threads. The single-switch state handshake
        # in MainLoop.process_input_state means each state is emitted in its
        # own cycle (no lag), and an EndChannel ECM after the last state
        # produces an additional output via produce_state_on_finish.
        main_loop_thread.start()

        for setup_msg in [
            mock_assign_input_port,
            mock_assign_output_port,
            mock_add_input_channel,
            mock_add_partitioning,
            mock_initialize_executor,
        ]:
            input_queue.put(setup_msg)
            assert output_queue.get() == DCMElement(
                tag=mock_control_output_channel,
                payload=DirectControlMessagePayloadV2(
                    return_invocation=ReturnInvocation(
                        command_id=command_sequence,
                        return_value=ControlReturn(empty_return=EmptyReturn()),
                    )
                ),
            )

        # Going through the InitializeExecutor RPC above sets up the rest of
        # the worker state (output schema, partitioning bookkeeping). Swap
        # the executor instance with the test helper here so the test can
        # assert the executor's process_state and produce_state_on_finish
        # actually ran, without depending on Python's cross-test module
        # caching for operator classes loaded via OpExecWithCode.
        main_loop.context.executor_manager.executor = state_processing_executor

        # Send four states. With the lag-free state pipeline we expect each
        # state to produce its own output in order.
        for state_element in mock_state_data_elements:
            input_queue.put(state_element)

        for expected_value in (1, 2, 3, 4):
            output_data_element: DataElement = output_queue.get()
            assert output_data_element.tag == mock_data_output_channel
            assert isinstance(output_data_element.payload, StateFrame), (
                f"expected StateFrame for value={expected_value}, got "
                f"{type(output_data_element.payload).__name__}"
            )
            output_state = output_data_element.payload.frame
            assert output_state["value"] == expected_value, (
                f"state outputs arrived out of order: expected value="
                f"{expected_value}, got value={output_state['value']}"
            )
            assert output_state["processed_marker"] == "executed"
            assert output_state["port"] == 0

        # Send EndChannel to drive _process_end_channel. The executor's
        # produce_state_on_finish writes a finish-marker state into
        # current_output_state inside DataProc's process_internal_marker;
        # MainLoop's process_input_state then emits it.
        input_queue.put(mock_end_of_upstream)

        # Drain the control reply messages so the next data
        # output_queue.get() returns the post-EndChannel data emission.
        output_queue.disable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)
        for _ in range(3):
            control_reply = output_queue.get()
            assert isinstance(control_reply, DCMElement), (
                f"expected DCMElement during EndChannel teardown, got "
                f"{type(control_reply).__name__}"
            )
        output_queue.enable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)

        end_channel_state_output: DataElement = output_queue.get()
        assert end_channel_state_output.tag == mock_data_output_channel
        assert isinstance(end_channel_state_output.payload, StateFrame), (
            f"expected StateFrame for the EndChannel-driven emission, got "
            f"{type(end_channel_state_output.payload).__name__}"
        )
        end_channel_state = end_channel_state_output.payload.frame
        assert "finish_marker" in end_channel_state.__dict__, (
            f"EndChannel emission should be the finish-marker state from "
            f"produce_state_on_finish, got {end_channel_state!r}"
        )
        assert end_channel_state["finish_marker"] == "produce_state_on_finish_ran"

        reraise()

    @pytest.mark.timeout(2)
    def test_main_loop_thread_can_process_state_after_tuple(
        self,
        mock_data_output_channel,
        mock_control_output_channel,
        input_queue,
        output_queue,
        main_loop,
        main_loop_thread,
        mock_assign_input_port,
        mock_assign_output_port,
        mock_add_input_channel,
        mock_add_partitioning,
        mock_initialize_executor,
        mock_data_element,
        mock_state_data_elements,
        state_processing_executor,
        command_sequence,
        reraise,
    ):
        # Coverage for the mixed (tuple, then state) input sequence: a
        # tuple followed by several state DataElements should still emit
        # every state's processed output in order.
        main_loop_thread.start()

        for setup_msg in [
            mock_assign_input_port,
            mock_assign_output_port,
            mock_add_input_channel,
            mock_add_partitioning,
            mock_initialize_executor,
        ]:
            input_queue.put(setup_msg)
            assert output_queue.get() == DCMElement(
                tag=mock_control_output_channel,
                payload=DirectControlMessagePayloadV2(
                    return_invocation=ReturnInvocation(
                        command_id=command_sequence,
                        return_value=ControlReturn(empty_return=EmptyReturn()),
                    )
                ),
            )

        main_loop.context.executor_manager.executor = state_processing_executor

        # Tuple first, then four states.
        input_queue.put(mock_data_element)
        warmup_output: DataElement = output_queue.get()
        assert warmup_output.tag == mock_data_output_channel
        assert isinstance(warmup_output.payload, DataFrame)

        for state_element in mock_state_data_elements:
            input_queue.put(state_element)

        for expected_value in (1, 2, 3, 4):
            output_data_element: DataElement = output_queue.get()
            assert output_data_element.tag == mock_data_output_channel
            assert isinstance(output_data_element.payload, StateFrame), (
                f"expected StateFrame for value={expected_value}, got "
                f"{type(output_data_element.payload).__name__}"
            )
            output_state = output_data_element.payload.frame
            assert output_state["value"] == expected_value, (
                f"state outputs after a tuple arrived out of order: "
                f"expected value={expected_value}, "
                f"got value={output_state['value']}"
            )
            assert output_state["processed_marker"] == "executed"

        reraise()
