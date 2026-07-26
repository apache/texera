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
import sys
import time
import uuid
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
from core.models.operator import LoopEndOperator, LoopStartOperator
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
from core.architecture.managers.context import Context
from core.architecture.managers.pause_manager import PauseType
from core.util.console_message.timestamp import current_time_in_local_timezone
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
    ConsoleMessage,
    ConsoleMessageType,
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
from pytexera.udf.udf_operator import UDFOperatorV2


class _FalseLoopEnd(LoopEndOperator):
    def condition(self):
        return False


class EmptyOnFinishOperator(UDFOperatorV2):
    # Echoes each input tuple, but its on_finish is a zero-yield generator
    # (`return` before `yield` makes the body unreachable while still marking
    # the function as a generator). This is the BatchOperator-fed-an-exact-
    # multiple-of-BATCH_SIZE shape: the EndChannel on_finish produces NOTHING,
    # so DataProcessor._set_output_tuple exhausts the iterator in a single
    # hand-off (no per-output switch dance) and sets finished_current straight
    # away. MainLoop must not lose that completion signal.
    def process_tuple(self, tuple_, port):
        yield tuple_

    def on_finish(self, port):
        return
        yield


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
            ActorVirtualIdentity("COORDINATOR"),
            ActorVirtualIdentity("dummy_worker_id"),
            True,
        )

    @pytest.fixture
    def mock_control_output_channel(self):
        return ChannelIdentity(
            ActorVirtualIdentity("dummy_worker_id"),
            ActorVirtualIdentity("COORDINATOR"),
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
            state = State({"value": value})
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
                new_state = State(
                    {key: value for key, value in state.items() if key != "schema"}
                )
                new_state["processed_marker"] = "executed"
                new_state["port"] = port
                return new_state

            @staticmethod
            def produce_state_on_finish(port: int) -> State:
                return State({"finish_marker": "produce_state_on_finish_ran"})

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
    def mock_start_channel(self, mock_data_input_channel):
        # Mirror of mock_end_of_upstream but a StartChannel ECM with
        # NO_ALIGNMENT (the alignment a real StartChannel bracket uses).
        return ECMElement(
            tag=mock_data_input_channel,
            payload=EmbeddedControlMessage(
                EmbeddedControlMessageIdentity("StartChannel"),
                EmbeddedControlMessageType.NO_ALIGNMENT,
                [],
                {
                    mock_data_input_channel.to_worker_id.name: ControlInvocation(
                        "StartChannel",
                        ControlRequest(empty_request=EmptyRequest()),
                        AsyncRpcContext(ActorVirtualIdentity(), ActorVirtualIdentity()),
                        -1,
                    )
                },
            ),
        )

    @pytest.fixture
    def mock_initialize_empty_on_finish_executor(
        self,
        mock_control_input_channel,
        mock_sender_actor,
        mock_link,
        command_sequence,
        mock_raw_schema,
    ):
        operator_code = "from pytexera import *\n" + inspect.getsource(
            EmptyOnFinishOperator
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
                        receiver=ActorVirtualIdentity(name="COORDINATOR"),
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
                        receiver=ActorVirtualIdentity(name="COORDINATOR"),
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
                        receiver=ActorVirtualIdentity(name="COORDINATOR"),
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

    @pytest.mark.timeout(2)
    def test_process_state_can_emit_consecutive_states(
        self,
        main_loop,
        output_queue,
        mock_data_output_channel,
        monkeypatch,
    ):
        class DummyExecutor:
            @staticmethod
            def process_state(state, port: int):
                return State({"value": state["value"] + 1, "port": port})

        main_loop.context.executor_manager.executor = DummyExecutor()
        monkeypatch.setattr(main_loop, "_check_and_process_control", lambda: None)
        monkeypatch.setattr(
            main_loop.context.output_manager,
            "emit_state",
            lambda state, loop_counter, *_: [
                (mock_data_output_channel.to_worker_id, StateFrame(state))
            ],
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

        first_state = State({"value": 1})
        second_state = State({"value": 41})

        main_loop._process_state_frame(StateFrame(first_state))
        main_loop._process_state_frame(StateFrame(second_state))

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
    def test_process_input_state_persists_output_state_to_storage(
        self,
        main_loop,
        mock_data_output_channel,
        monkeypatch,
    ):
        # process_input_state must invoke save_state_to_storage_if_needed
        # with the freshly emitted output state, so every state that flows
        # downstream is also durable on the upstream output port.
        class DummyExecutor:
            @staticmethod
            def process_state(state: State, port: int) -> State:
                return State({"value": state["value"] + 1, "port": port})

        saved_states: list[State] = []
        main_loop.context.executor_manager.executor = DummyExecutor()
        monkeypatch.setattr(main_loop, "_check_and_process_control", lambda: None)
        monkeypatch.setattr(
            main_loop.context.output_manager,
            "emit_state",
            lambda state, loop_counter, *_: [
                (mock_data_output_channel.to_worker_id, StateFrame(state))
            ],
        )
        monkeypatch.setattr(
            main_loop.context.output_manager,
            "save_state_to_storage_if_needed",
            lambda state, loop_counter, *_: saved_states.append(state),
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

        main_loop._process_state_frame(StateFrame(State({"value": 1})))
        main_loop._process_state_frame(StateFrame(State({"value": 41})))

        # Each input state produced one output state, so both must have
        # been persisted in order.
        assert [s["value"] for s in saved_states] == [2, 42]
        assert all(s["port"] == 0 for s in saved_states)

    @pytest.mark.timeout(2)
    def test_process_start_channel_persists_produce_state_on_start_output(
        self,
        main_loop,
        mock_data_output_channel,
        monkeypatch,
    ):
        # The state emitted by an executor's `produce_state_on_start` must
        # also be persisted via `save_state_to_storage_if_needed`, so a
        # downstream worker in a different region can replay it from the
        # iceberg state table.
        #
        # This is the integration path exercised in real workflows when
        # users override `produce_state_on_start`. `_process_start_channel`
        # → `process_input_state` → DataProcessor.process_internal_marker
        # (StartChannel) → executor.produce_state_on_start → _set_output_state
        # → MainLoop reads output state → emit + save.
        on_start_state = State({"flag": True})

        class DummyExecutor:
            @staticmethod
            def produce_state_on_start(port: int) -> State:
                # Tag with port so we can also assert the right port id
                # was forwarded.
                return State({**on_start_state, "port": port})

        saved_states: list[State] = []
        main_loop.context.executor_manager.executor = DummyExecutor()
        monkeypatch.setattr(main_loop, "_check_and_process_control", lambda: None)
        monkeypatch.setattr(
            main_loop.context.output_manager,
            "emit_state",
            lambda state, loop_counter, *_: [
                (mock_data_output_channel.to_worker_id, StateFrame(state))
            ],
        )
        monkeypatch.setattr(
            main_loop.context.output_manager,
            "save_state_to_storage_if_needed",
            lambda state, loop_counter, *_: saved_states.append(state),
        )
        # _send_ecm_to_data_channels touches output_manager state we don't
        # set up here; for this test the ECM forwarding is irrelevant -- the
        # SAVE path is what we're pinning. Stub it.
        monkeypatch.setattr(main_loop, "_send_ecm_to_data_channels", lambda *_: None)

        # Simulate the DP-thread side: when MainLoop yields, the DataProcessor
        # consumes the StartChannel marker and runs produce_state_on_start.
        def fake_switch_context():
            from core.models.internal_marker import StartChannel as _StartChannel

            tpm = main_loop.context.tuple_processing_manager
            if isinstance(tpm.current_internal_marker, _StartChannel):
                # mimic DataProcessor.process_internal_marker(StartChannel)
                produced = DummyExecutor.produce_state_on_start(port=0)
                main_loop.context.state_processing_manager.current_output_state = (
                    produced
                )
                tpm.current_internal_marker = None  # consumed

        monkeypatch.setattr(main_loop, "_switch_context", fake_switch_context)

        # Drive the path: this is exactly what `_process_ecm` calls when a
        # StartChannel ECM arrives and the start_channel handler has set
        # the marker.
        from core.models.internal_marker import StartChannel

        main_loop.context.tuple_processing_manager.current_internal_marker = (
            StartChannel()
        )
        main_loop._process_start_channel()

        # The state produced by produce_state_on_start must be persisted to
        # iceberg via save_state_to_storage_if_needed. Without this, a
        # downstream worker in a different region cannot observe the state.
        assert len(saved_states) == 1, (
            f"produce_state_on_start emitted a state but it was not persisted "
            f"to storage. saved_states={saved_states}"
        )
        assert saved_states[0]["flag"] is True
        # loop_counter is no longer part of the user State; it rides on the
        # StateFrame envelope / its own materialized column.
        assert "loop_counter" not in saved_states[0]
        assert saved_states[0]["port"] == 0

    @pytest.mark.timeout(2)
    def test_process_input_state_does_not_save_when_no_output(
        self,
        main_loop,
        monkeypatch,
    ):
        # When the executor returns no output state (process_state returned
        # None), save_state_to_storage_if_needed must not be called -- no
        # state means nothing to materialize.
        save_calls: list[State] = []
        monkeypatch.setattr(main_loop, "_check_and_process_control", lambda: None)
        monkeypatch.setattr(
            main_loop.context.output_manager,
            "emit_state",
            lambda state, loop_counter, *_: [],
        )
        monkeypatch.setattr(
            main_loop.context.output_manager,
            "save_state_to_storage_if_needed",
            lambda state, loop_counter, *_: save_calls.append(state),
        )
        # Pretend DataProc consumed the input but produced no output.
        monkeypatch.setattr(main_loop, "_switch_context", lambda: None)

        main_loop._process_state_frame(StateFrame(State({"value": 1})))

        assert save_calls == []

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
        assert "finish_marker" in end_channel_state, (
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

    @staticmethod
    def _expected_port_completed_dcm(
        mock_control_output_channel, command_id, port_id, is_input
    ):
        return DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                control_invocation=ControlInvocation(
                    method_name="PortCompleted",
                    command_id=command_id,
                    context=AsyncRpcContext(
                        sender=ActorVirtualIdentity(name="dummy_worker_id"),
                        receiver=ActorVirtualIdentity(name="COORDINATOR"),
                    ),
                    command=ControlRequest(
                        port_completed_request=PortCompletedRequest(
                            port_id=port_id, input=is_input
                        )
                    ),
                )
            ),
        )

    @staticmethod
    def _expected_worker_completed_dcm(mock_control_output_channel):
        return DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                control_invocation=ControlInvocation(
                    method_name="WorkerExecutionCompleted",
                    command_id=2,
                    context=AsyncRpcContext(
                        sender=ActorVirtualIdentity(name="dummy_worker_id"),
                        receiver=ActorVirtualIdentity(name="COORDINATOR"),
                    ),
                    command=ControlRequest(empty_request=EmptyRequest()),
                )
            ),
        )

    @staticmethod
    def _forwarded_ecm(mock_data_output_channel, method_name, alignment):
        return ECMElement(
            tag=mock_data_output_channel,
            payload=EmbeddedControlMessage(
                EmbeddedControlMessageIdentity(method_name),
                alignment,
                [],
                {
                    mock_data_output_channel.to_worker_id.name: ControlInvocation(
                        method_name,
                        ControlRequest(empty_request=EmptyRequest()),
                        AsyncRpcContext(ActorVirtualIdentity(), ActorVirtualIdentity()),
                        -1,
                    )
                },
            ),
        )

    @staticmethod
    def _drain_until(output_queue, done, timeout=15.0):
        # Non-blocking drain of the output queue against a deadline. A
        # regression that deadlocks the MainLoop/DataProcessor handshake never
        # satisfies `done`, so we return the partial batch at the deadline and
        # let the caller pytest.fail() -- the whole pytest process is never
        # hung because the worker runs on a daemon thread.
        deadline = time.time() + timeout
        collected = []
        while time.time() < deadline:
            while output_queue.size() > 0:
                collected.append(output_queue.get())
            if done(collected):
                return collected
            time.sleep(0.005)
        return collected

    @pytest.mark.timeout(30)
    def test_zero_tuple_channel_completes_worker(
        self,
        mock_link,
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
        mock_start_channel,
        mock_end_of_upstream,
        command_sequence,
        reraise,
    ):
        # A worker whose input port receives a StartChannel->EndChannel bracket
        # with ZERO DataElements (the untaken branch of an If read through an
        # InputPortMaterializationReaderRunnable, or a filter that drops
        # everything on a materialized edge) must still process both ECMs and
        # reach COMPLETED. Two sub-bugs made this hang/crash on the old
        # loop-feb branch:
        #   (1) DEADLOCK: MainLoop._process_ecm re-read current_internal_marker
        #       AFTER a _switch_context(), so the DataProcessor could pop the
        #       marker mid-switch and MainLoop skipped _process_end_channel ->
        #       both threads park forever.
        #   (2) STATE GRAPH: a zero-tuple worker never enters RUNNING (only
        #       _process_data_element does that), so completion is a direct
        #       READY -> COMPLETED transition, which the transition graph must
        #       permit.
        # Run on a daemon thread and detect completion by deadline-polling the
        # state manager so a regression deadlock fails cleanly instead of
        # hanging the whole pytest process.
        main_loop_thread.daemon = True
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

        # The worker is READY here and never enters RUNNING (no data element).
        assert main_loop.context.state_manager.confirm_state(WorkerState.READY)

        # Zero tuples between StartChannel and EndChannel.
        input_queue.put(mock_start_channel)
        input_queue.put(mock_end_of_upstream)

        expected_worker_completed = self._expected_worker_completed_dcm(
            mock_control_output_channel
        )
        collected = self._drain_until(
            output_queue,
            lambda items: expected_worker_completed in items,
        )

        if not main_loop.context.state_manager.confirm_state(WorkerState.COMPLETED):
            pytest.fail(
                "zero-tuple worker did not reach COMPLETED within the deadline "
                "-- likely the _process_ecm marker-after-switch deadlock or a "
                "missing READY->COMPLETED transition. "
                f"state={main_loop.context.state_manager.get_current_state()}, "
                f"collected={collected}"
            )

        # Both the input and output ports complete, and the worker signals
        # WorkerExecutionCompleted -- all on the coordinator control channel.
        expected_input_port_completed = self._expected_port_completed_dcm(
            mock_control_output_channel, 0, mock_link.to_port_id, True
        )
        expected_output_port_completed = self._expected_port_completed_dcm(
            mock_control_output_channel, 1, PortIdentity(id=0), False
        )
        assert expected_input_port_completed in collected
        assert expected_output_port_completed in collected
        assert expected_worker_completed in collected

        # Both ECMs are forwarded downstream on the data output channel.
        assert (
            self._forwarded_ecm(
                mock_data_output_channel,
                "StartChannel",
                EmbeddedControlMessageType.NO_ALIGNMENT,
            )
            in collected
        )
        assert (
            self._forwarded_ecm(
                mock_data_output_channel,
                "EndChannel",
                EmbeddedControlMessageType.PORT_ALIGNMENT,
            )
            in collected
        )

        reraise()

    @pytest.mark.timeout(30)
    def test_empty_on_finish_after_tuples_completes_worker(
        self,
        mock_link,
        mock_tuple,
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
        mock_initialize_empty_on_finish_executor,
        mock_data_element,
        mock_end_of_upstream,
        command_sequence,
        monkeypatch,
        reraise,
    ):
        # Sibling case: after processing real tuples, an EndChannel whose
        # on_finish yields NOTHING must also complete cleanly. The empty
        # on_finish is exhausted inside a single hand-off (DataProcessor
        # ._set_output_tuple runs no per-output switch dance, it just sets
        # finished_current), and MainLoop must not lose the completion signal.

        # Guard the udf-v1 executor-module-contamination landmine: force a
        # unique module name so cross-test importlib caching can't hand us a
        # stale operator class. (main's ExecutorManager already uses a
        # process-wide unique counter, so this is belt-and-suspenders.)
        unique_name = f"udf_empty_on_finish_{uuid.uuid4().hex}"
        monkeypatch.setattr(
            main_loop.context.executor_manager,
            "gen_module_file_name",
            lambda: (unique_name, f"{unique_name}.py"),
        )

        main_loop_thread.daemon = True
        main_loop_thread.start()

        for setup_msg in [
            mock_assign_input_port,
            mock_assign_output_port,
            mock_add_input_channel,
            mock_add_partitioning,
            mock_initialize_empty_on_finish_executor,
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

        # The loaded executor must be our zero-yield-on_finish operator, not a
        # stale cached class from another test.
        assert (
            type(main_loop.context.executor_manager.executor).__name__
            == "EmptyOnFinishOperator"
        )

        # One real tuple: the operator echoes it and the worker enters RUNNING.
        input_queue.put(mock_data_element)
        echoed: DataElement = output_queue.get()
        assert echoed.tag == mock_data_output_channel
        assert isinstance(echoed.payload, DataFrame)
        assert Tuple(echoed.payload.frame.to_pylist()[0]) == mock_tuple

        # EndChannel with an empty on_finish must still complete the worker.
        input_queue.put(mock_end_of_upstream)

        expected_worker_completed = self._expected_worker_completed_dcm(
            mock_control_output_channel
        )
        collected = self._drain_until(
            output_queue,
            lambda items: expected_worker_completed in items,
        )

        if not main_loop.context.state_manager.confirm_state(WorkerState.COMPLETED):
            pytest.fail(
                "worker with an empty on_finish did not reach COMPLETED within "
                "the deadline -- the single-hand-off completion signal was lost. "
                f"state={main_loop.context.state_manager.get_current_state()}, "
                f"collected={collected}"
            )

        expected_input_port_completed = self._expected_port_completed_dcm(
            mock_control_output_channel, 0, mock_link.to_port_id, True
        )
        expected_output_port_completed = self._expected_port_completed_dcm(
            mock_control_output_channel, 1, PortIdentity(id=0), False
        )
        assert expected_input_port_completed in collected
        assert expected_output_port_completed in collected
        assert expected_worker_completed in collected

        # The EndChannel ECM is forwarded downstream on the data output channel.
        assert (
            self._forwarded_ecm(
                mock_data_output_channel,
                "EndChannel",
                EmbeddedControlMessageType.PORT_ALIGNMENT,
            )
            in collected
        )

        reraise()

    @pytest.mark.timeout(2)
    def test_console_message_rpc_fires_before_exception_pause(
        self, main_loop, monkeypatch
    ):
        # Pin the coordinator-facing contract: when DataProcessor raises
        # during an executor call, the stack-trace ConsoleMessage must
        # reach the coordinator *before* the worker enters EXCEPTION_PAUSE
        # — otherwise the UI sees a paused worker with no error to show
        # until the user resumes. The DataProcessor side queues the
        # message before the switch (covered by
        # test_data_processor.TestExecutorSession); this test pins the
        # MainLoop side: post-switch hook flushes RPCs first, pauses last.
        events = []

        monkeypatch.setattr(
            main_loop,
            "_send_console_message",
            lambda msg: events.append(("rpc", msg)),
        )
        monkeypatch.setattr(
            main_loop.context.pause_manager,
            "pause",
            lambda pause_type, change_state=True: events.append(("pause", pause_type)),
        )

        try:
            raise RuntimeError("boom-from-executor")
        except RuntimeError:
            exc_info = sys.exc_info()
        main_loop.context.exception_manager.set_exception_info(exc_info)
        main_loop.context.console_message_manager.put_message(
            ConsoleMessage(
                worker_id="dummy_worker_id",
                timestamp=current_time_in_local_timezone(),
                msg_type=ConsoleMessageType.ERROR,
                source="test:_capture_exc_info:0",
                title="RuntimeError: boom-from-executor",
                message="RuntimeError: boom-from-executor",
            )
        )

        main_loop._post_switch_context_checks()

        kinds = [e[0] for e in events]
        assert kinds == ["rpc", "pause"], (
            "console message must reach coordinator before pause; "
            f"observed order: {kinds}"
        )
        assert events[0][1].msg_type == ConsoleMessageType.ERROR
        assert "boom-from-executor" in events[0][1].title
        assert events[1][1] is PauseType.EXCEPTION_PAUSE

    @pytest.mark.timeout(2)
    def test_complete_reports_loopend_condition_error_instead_of_crashing(
        self, main_loop, monkeypatch
    ):
        # Reviewer feedback (#discussion_r3400851492): complete() evaluates a
        # LoopEnd's user-supplied condition() on the main loop thread, before
        # close()/COMPLETED and outside DataProcessor's guarded executor
        # session. A typo or undefined name in the condition would otherwise
        # propagate through run()'s @logger.catch(reraise=True) and kill the
        # worker thread silently. The guard must report it like a UDF error
        # (record on the exception manager + ERROR console message +
        # EXCEPTION_PAUSE) and skip both the loop-back edge and completion.
        class _BoomLoopEnd(LoopEndOperator):
            def __init__(self):
                super().__init__()
                self.closed = False

            def condition(self):
                raise ValueError("name 'i' is not defined")

            def close(self):
                self.closed = True

        executor = _BoomLoopEnd()
        main_loop.context.executor_manager.executor = executor

        console_msgs = []
        pauses = []
        jumped = []
        monkeypatch.setattr(
            main_loop, "_send_console_message", lambda msg: console_msgs.append(msg)
        )
        monkeypatch.setattr(
            main_loop.context.pause_manager,
            "pause",
            lambda pause_type, change_state=True: pauses.append(pause_type),
        )
        monkeypatch.setattr(
            main_loop, "_jump_to_loop_start", lambda *args: jumped.append(True)
        )

        # Must not raise: a bad condition is reported, not propagated.
        main_loop.complete()

        assert jumped == [], "must not take the loop-back edge on a failed condition"
        assert not executor.closed, "must return before completing the worker"
        assert main_loop.context.exception_manager.has_exception()
        assert pauses == [PauseType.EXCEPTION_PAUSE]
        error_msgs = [m for m in console_msgs if m.msg_type == ConsoleMessageType.ERROR]
        assert len(error_msgs) == 1
        assert "ValueError" in error_msgs[0].title
        assert "name 'i' is not defined" in error_msgs[0].title

    @pytest.mark.timeout(2)
    def test_complete_reports_loopback_write_error_instead_of_crashing(
        self, main_loop, monkeypatch
    ):
        # Reviewer feedback (#discussion_r3561096471): the back-edge state
        # write in _jump_to_loop_start runs after the jump DCM, on the main
        # loop thread, outside DataProcessor's guarded executor session. A
        # put_one/close failure must be reported the same way as a condition
        # error (exception manager + ERROR console message + EXCEPTION_PAUSE)
        # and skip completion, not propagate and kill the worker thread.
        class _JumpingLoopEnd(LoopEndOperator):
            def __init__(self):
                super().__init__()
                self.closed = False

            def condition(self):
                return True

            def close(self):
                self.closed = True

        executor = _JumpingLoopEnd()
        executor.state = State({"i": 1})
        main_loop.context.executor_manager.executor = executor
        main_loop._loop_start_id = "loop-start-1"
        main_loop.context.loop_start_state_uris = {"loop-start-1": "vfs:///x/state"}

        console_msgs = []
        pauses = []
        monkeypatch.setattr(
            main_loop, "_send_console_message", lambda msg: console_msgs.append(msg)
        )
        monkeypatch.setattr(
            main_loop.context.pause_manager,
            "pause",
            lambda pause_type, change_state=True: pauses.append(pause_type),
        )

        class _BoomWriter:
            def put_one(self, item):
                raise OSError("iceberg commit failed")

            def close(self):
                pass

        class _Doc:
            def writer(self, name):
                return _BoomWriter()

        monkeypatch.setattr(
            "core.runnables.main_loop.DocumentFactory.create_document",
            lambda uri, schema: _Doc(),
        )

        # Must not raise: a failed back-edge write is reported, not propagated.
        main_loop.complete()

        assert not executor.closed, "must return before completing the worker"
        assert main_loop.context.exception_manager.has_exception()
        assert pauses == [PauseType.EXCEPTION_PAUSE]
        error_msgs = [m for m in console_msgs if m.msg_type == ConsoleMessageType.ERROR]
        assert len(error_msgs) == 1
        assert "iceberg commit failed" in error_msgs[0].title

    @pytest.mark.timeout(2)
    def test_emit_and_save_state_reports_error_instead_of_killing_thread(
        self, main_loop, monkeypatch
    ):
        # State serialization (state.to_tuple -> to_json) runs on the main loop
        # thread inside _emit_and_save_state, outside DataProcessor's guarded
        # executor session. A non-JSON-serializable loop variable (e.g. a numpy
        # array) makes save_state_to_storage_if_needed raise; without a guard it
        # propagates through run()'s @logger.catch(reraise=True) and kills the
        # thread, hanging the workflow with no operator-facing error. It must be
        # reported like a UDF error (exception manager + ERROR console message +
        # EXCEPTION_PAUSE) instead.
        console_msgs = []
        pauses = []
        monkeypatch.setattr(
            main_loop, "_send_console_message", lambda msg: console_msgs.append(msg)
        )
        monkeypatch.setattr(
            main_loop.context.pause_manager,
            "pause",
            lambda pause_type, change_state=True: pauses.append(pause_type),
        )
        monkeypatch.setattr(
            main_loop.context.output_manager, "emit_state", lambda *a, **k: []
        )

        def _boom(*args, **kwargs):
            raise TypeError("State value of type ndarray is not JSON serializable")

        monkeypatch.setattr(
            main_loop.context.output_manager,
            "save_state_to_storage_if_needed",
            _boom,
        )

        # Must not raise: the serialization error is reported, not propagated.
        main_loop._emit_and_save_state(State({"weights": 1}), 0, "")

        assert main_loop.context.exception_manager.has_exception()
        assert pauses == [PauseType.EXCEPTION_PAUSE]
        error_msgs = [m for m in console_msgs if m.msg_type == ConsoleMessageType.ERROR]
        assert len(error_msgs) == 1
        assert "not JSON serializable" in error_msgs[0].title

    @pytest.mark.timeout(2)
    def test_end_channel_holds_region_when_state_emit_fails(
        self, main_loop, monkeypatch
    ):
        # When a state-emission error is reported during _process_end_channel,
        # the worker must NOT go on to send port_completed / complete(): those
        # RPCs would let the coordinator mark the region complete despite the
        # reported error (port-based region completion). The guard holds the
        # region so the reported error is not a false success.
        completed = []
        port_completed_calls = []

        def _boom_process_input_state(*args, **kwargs):
            # Simulate a reported state-emit failure on the main loop thread.
            try:
                raise TypeError("not JSON serializable")
            except TypeError as err:
                main_loop.context.report_exception(err)
                main_loop._check_exception()

        monkeypatch.setattr(main_loop, "process_input_state", _boom_process_input_state)
        monkeypatch.setattr(main_loop, "process_input_tuple", lambda: None)
        monkeypatch.setattr(main_loop, "complete", lambda: completed.append(True))

        class _Coordinator:
            def port_completed(self, request):
                port_completed_calls.append(request)

        monkeypatch.setattr(
            main_loop._async_rpc_client, "coordinator_stub", lambda: _Coordinator()
        )
        monkeypatch.setattr(
            main_loop.context.pause_manager,
            "pause",
            lambda pause_type, change_state=True: None,
        )
        monkeypatch.setattr(main_loop, "_send_console_message", lambda msg: None)

        main_loop._process_end_channel()

        assert port_completed_calls == [], (
            "must not complete ports after a reported error"
        )
        assert completed == [], "must not complete the worker after a reported error"

    # -- Loop counter is runtime-owned (relocated from test_loop_operators) ---
    #
    # loop_counter is not part of State; it rides on the StateFrame envelope and
    # the runtime (_process_state_frame) owns the +1/-1. On the nested
    # pass-through branches the operator must be skipped entirely.

    def _capture_state_emit(self, main_loop, monkeypatch):
        """Stub emit/save/switch/reset; return (emitted, switched, reset_calls).

        Each `emitted` entry is (state, loop_counter, loop_start_id) so tests
        can assert the loop metadata the runtime attaches to the StateFrame
        envelope. The emit stub mirrors the real 3-arg
        `OutputManager.emit_state` signature, so a signature drift between the
        runtime and the manager fails here instead of being masked.
        `reset_calls` records each `output_manager.reset_output_storage()` call
        (stubbed so the real iceberg-truncation never runs in the unit test);
        the inner-LoopEnd pass-through is expected to fire it once, the consume
        path never.
        """
        emitted = []
        switched = []
        reset_calls = []
        monkeypatch.setattr(main_loop, "_check_and_process_control", lambda: None)
        monkeypatch.setattr(main_loop, "_switch_context", lambda: switched.append(True))
        monkeypatch.setattr(
            main_loop.context.output_manager,
            "reset_output_storage",
            lambda: reset_calls.append(True),
        )
        monkeypatch.setattr(
            main_loop.context.output_manager,
            "emit_state",
            lambda state, loop_counter, loop_start_id="": (
                emitted.append((state, loop_counter, loop_start_id)) or []
            ),
        )
        monkeypatch.setattr(
            main_loop.context.output_manager,
            "save_state_to_storage_if_needed",
            lambda state, loop_counter, loop_start_id="": None,
        )
        return emitted, switched, reset_calls

    def test_loopstart_reentry_increments_counter_and_skips_operator(
        self, main_loop, monkeypatch
    ):
        # A state arriving with a loop_start_id stamped on its envelope is an
        # outer loop's state passing through this inner LoopStart. The runtime
        # forwards it with loop_counter + 1 (keeping the outer id) and must
        # NOT invoke the operator.
        class StubLoopStart(LoopStartOperator):
            def process_table(self, table, port):
                yield

        main_loop.context.executor_manager.executor = StubLoopStart()
        emitted, switched, reset_calls = self._capture_state_emit(
            main_loop, monkeypatch
        )

        main_loop._process_state_frame(
            StateFrame(
                State({"i": 5}),
                loop_counter=1,
                loop_start_id="outer-loop",
            )
        )

        assert switched == [], "nested pass-through must not invoke the operator"
        assert len(emitted) == 1
        emitted_state, emitted_counter, emitted_id = emitted[0]
        assert emitted_counter == 2  # 1 + 1
        assert emitted_state["i"] == 5
        assert "loop_counter" not in emitted_state  # never leaks into State
        # the outer loop's id rides through unchanged
        assert emitted_id == "outer-loop"
        assert reset_calls == [], "a LoopStart never resets output storage"

    def test_loopend_passthrough_decrements_resets_output_and_skips_operator(
        self, main_loop, monkeypatch
    ):
        # loop_counter > 0 at a LoopEnd means the state belongs to an outer
        # loop: the runtime decrements and forwards, skipping the operator.
        # Reviewer feedback (#discussion_r3400851478): it also resets this
        # (inner) LoopEnd's output storage -- the outer loop advancing is the
        # signal to drop the previous outer iteration's rows; see the
        # reset_output_storage call site in _process_state_frame for the
        # full story.
        main_loop.context.executor_manager.executor = _FalseLoopEnd()
        emitted, switched, reset_calls = self._capture_state_emit(
            main_loop, monkeypatch
        )

        main_loop._process_state_frame(
            StateFrame(
                State({"outer_var": "v"}),
                loop_counter=2,
                loop_start_id="outer-loop",
            )
        )

        assert switched == [], "pass-through must not invoke the operator"
        assert reset_calls == [True], "pass-through resets the inner LoopEnd output"
        assert len(emitted) == 1
        emitted_state, emitted_counter, emitted_id = emitted[0]
        assert emitted_counter == 1  # 2 - 1
        assert emitted_state["outer_var"] == "v"
        # the outer loop's id rides through unchanged
        assert emitted_id == "outer-loop"

    def test_loopend_consume_invokes_operator_at_counter_zero(
        self, main_loop, monkeypatch
    ):
        # loop_counter == 0 is the matching loop: the runtime runs the operator
        # (consume) via the context switch. The operator returns None, so no
        # state is emitted; the loop-back is driven by complete() separately.
        # Reviewer feedback (#discussion_r3285892237): the envelope's loop
        # metadata (loop_counter / loop_start_id) is internal runtime data --
        # the runtime captures it onto its own instance state, and the
        # user-facing State handed to the operator carries only the inner
        # State's keys, never the envelope names.
        main_loop.context.executor_manager.executor = _FalseLoopEnd()
        emitted, switched, reset_calls = self._capture_state_emit(
            main_loop, monkeypatch
        )
        # No output from the operator -> no emit work after consume.
        monkeypatch.setattr(
            main_loop.context.state_processing_manager,
            "get_output_state",
            lambda: None,
        )

        main_loop._process_state_frame(
            StateFrame(
                State({"i": 42, "acc": [1, 2, 3]}),
                loop_counter=0,
                loop_start_id="outer-loop",
            )
        )

        assert switched == [True], "consume branch must invoke the operator"
        assert emitted == [], "operator returned None -> nothing emitted"
        assert reset_calls == [], "consume / single loop must not reset output"
        # The runtime captured the envelope metadata onto its own instance
        # state...
        assert main_loop._loop_start_id == "outer-loop"
        # ...but never wrote it into the user-facing State the operator sees.
        # (The consume branch sets `current_input_state` BEFORE the stubbed
        # context switch, so this is exactly what the operator would receive.)
        passed_to_operator = (
            main_loop.context.state_processing_manager.current_input_state
        )
        assert isinstance(passed_to_operator, State)
        assert set(passed_to_operator.keys()) == {"i", "acc"}
        assert "loop_start_id" not in passed_to_operator
        assert "loop_counter" not in passed_to_operator

    # ------------------------------------------------------------------ #
    # _jump_to_loop_start
    #
    # Reviewer feedback (#discussion_r3285892249) flagged the loop-back
    # path as the most fragile loop-runtime code. The id a LoopStart
    # stamps is now computed inline in process_input_state via the
    # canonical `get_logical_op_id` (pinned by that helper's own suite),
    # and the loop-back write address is not computed worker-side at all:
    # it is setup config (InitializeExecutorRequest.loopStartStateUris --
    # see the proto comment for the full story).
    # ------------------------------------------------------------------ #

    @staticmethod
    def _stub_coordinator(record):
        """A coordinator_interface stand-in that records every
        jump_to_operator_region call into ``record``."""

        class _Coordinator:
            def jump_to_operator_region(self, request):
                record.append(request)

        return _Coordinator()

    @staticmethod
    def _patch_create_document(monkeypatch, write_log):
        """Patch DocumentFactory.create_document at the symbol imported
        into main_loop. Each call appends ``(uri, schema)`` to
        ``write_log`` and returns an object whose ``writer(name)`` yields
        a mock that records ``put_one`` and ``close`` calls into the
        same list (tagged so order is observable)."""

        class _Writer:
            def __init__(self, log):
                self._log = log

            def put_one(self, item):
                self._log.append(("put_one", item))

            def close(self):
                self._log.append(("close",))

        class _Doc:
            def __init__(self, log):
                self._log = log

            def writer(self, name):
                self._log.append(("writer", name))
                return _Writer(self._log)

        def _create(uri, schema):
            write_log.append(("create_document", uri, schema))
            return _Doc(write_log)

        monkeypatch.setattr(
            "core.runnables.main_loop.DocumentFactory.create_document",
            _create,
        )

    def test_jump_to_loop_start_sends_rpc_then_writes_state_in_order(
        self, main_loop, monkeypatch
    ):
        # One shared event log for the jump RPC and the storage calls, so
        # the cross-channel ordering is pinned along with each contract.
        main_loop._loop_start_id = "outer-loop"
        # The write address is setup-injected config keyed by the captured id.
        main_loop.context.loop_start_state_uris = {
            "outer-loop": "vfs:///wf/state/outer"
        }

        events = []
        self._patch_create_document(monkeypatch, events)

        class _Coordinator:
            def jump_to_operator_region(self, request):
                events.append(("jump", request))

        class _Executor:
            state = State({"i": 7})

        main_loop._jump_to_loop_start(_Executor(), _Coordinator())

        assert len(events) == 5
        # (i) The jump RPC fires before any storage event, carrying the
        # loop_start_id we captured from the incoming StateFrame envelope
        # -- never read off user state.
        kind, request = events[0]
        assert kind == "jump"
        assert request.target_operator_id.id == "outer-loop"
        # (ii) Then the exact iceberg write contract, in order:
        # create_document with the configured URI and State.SCHEMA, open
        # writer("0"), a single put_one with the State as a depth-0 tuple
        # (the back-edge fires only after the matching LoopEnd consumed at
        # loop_counter == 0, so the next iteration starts at depth 0),
        # then close. The tuple object's internals are exercised elsewhere.
        assert events[1] == (
            "create_document",
            "vfs:///wf/state/outer",
            State.SCHEMA,
        )
        assert events[2] == ("writer", "0")
        assert events[3][0] == "put_one"
        assert events[3][1] == State({"i": 7}).to_tuple(0)
        assert events[4] == ("close",)

    def test_jump_to_loop_start_raises_when_uri_not_configured(
        self, main_loop, monkeypatch
    ):
        # A LoopEnd whose captured id has no entry in the setup-injected
        # config (misconfigured plan, or the scheduler failed to resolve
        # the LoopStart's input port) must fail loudly BEFORE the jump
        # RPC and before any storage write -- rewinding the schedule
        # without a back-edge write would hang the loop.
        main_loop._loop_start_id = "outer"
        main_loop.context.loop_start_state_uris = {}

        rpc_calls = []
        write_log = []
        self._patch_create_document(monkeypatch, write_log)

        class _Executor:
            state = State({"i": 7})

        with pytest.raises(RuntimeError, match="no loop-back state URI"):
            main_loop._jump_to_loop_start(
                _Executor(), self._stub_coordinator(rpc_calls)
            )

        assert rpc_calls == [], "must fail before the jump RPC"
        assert write_log == [], "must fail before touching storage"

    @pytest.mark.timeout(10)
    def test_two_main_loops_load_distinct_operator_classes(self):
        """
        Two worker Contexts created in the same process with DIFFERENT operator
        classes must each load exactly the class they were given.

        Regression test for executor-module contamination (#4705): executor
        modules were once named ``udf-v<per-instance-counter>``, so every
        loop's first executor was ``udf-v1`` in the process-wide
        ``sys.modules``. A loop whose worker never completes never closes its
        temp fs, so its ``udf-v1.py`` lingered on ``sys.path`` and the next
        loop re-resolved ``udf-v1`` to that older file, silently running the
        wrong operator. This test uses NO monkeypatch of
        ``gen_module_file_name`` -- module names must be process-globally
        unique on their own.

        The module-naming collision lives entirely in ``ExecutorManager``, which
        each ``MainLoop`` owns via its ``Context``. We construct ``Context``
        directly (rather than ``MainLoop``) so the regression is exercised
        without spawning the per-loop ``DataProcessor`` daemon thread that a
        full ``MainLoop`` would leave running for the rest of the test session.
        """
        echo_code = "from pytexera import *\n" + inspect.getsource(EchoOperator)
        count_code = "from pytexera import *\n" + inspect.getsource(CountBatchOperator)

        first = Context("worker-first", InternalQueue())
        second = Context("worker-second", InternalQueue())
        try:
            # The first loop loads EchoOperator and is intentionally left "unfinished"
            # until after the second loop is initialized: its temp fs is not closed yet,
            # so its udf module and sys.path entry linger exactly as a crashed /
            # never-completed worker's would.
            first.executor_manager.initialize_executor(
                echo_code, is_source=False, language="python"
            )
            first_cls = type(first.executor_manager.executor).__name__
            first_module = first.executor_manager.operator_module_name
            assert first_cls == "EchoOperator"

            # The second loop asks for a DIFFERENT class. It must get that
            # class, not the first loop's EchoOperator via a udf module-name
            # collision in the shared sys.modules / sys.path.
            second.executor_manager.initialize_executor(
                count_code, is_source=False, language="python"
            )
            second_cls = type(second.executor_manager.executor).__name__
            second_module = second.executor_manager.operator_module_name
            assert second_cls == "CountBatchOperator"

            # The module names themselves must be process-globally unique -- a
            # per-instance counter would name both loops' first executor
            # "udf-v1" and reintroduce the sys.modules collision. Asserting the
            # names differ ties the guard directly to the root cause, not just
            # the (downstream) loaded class.
            assert first_module != second_module, (
                "executor module names must be process-globally unique; "
                f"both loops used {first_module!r}"
            )
        finally:
            first.executor_manager.close()
            second.executor_manager.close()
