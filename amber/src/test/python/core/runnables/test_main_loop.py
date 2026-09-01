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
    Schema,
    State,
    StateFrame,
    Table,
    Tuple,
)
from core.models.internal_queue import (
    DataElement,
    DCMElement,
    ECMElement,
)
from core.models.operator import LoopEndOperator, LoopStartOperator
from core.storage.vfs_uri_factory import VFSURIFactory
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
        # a missing/dropped version would echo through the read-back below; guard it
        assert worker_metrics_response.metrics.state_version > 0

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
            # version is the worker's logical state clock; read it from the actual
            # report rather than pinning a brittle count (covered by StateManager tests).
            state_version=worker_metrics_response.metrics.state_version,
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
        elem = output_queue.get()
        # version is the worker's logical state clock; read it from the actual
        # report rather than pinning a brittle count (covered by StateManager tests).
        state_version = elem.payload.return_invocation.return_value.worker_state_response.state_version
        # a missing/dropped version would echo through the read-back; guard it
        assert state_version > 0
        assert elem == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(
                        worker_state_response=WorkerStateResponse(
                            WorkerState.PAUSED, state_version=state_version
                        )
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
        elem = output_queue.get()
        # version is the worker's logical state clock; read it from the actual
        # report rather than pinning a brittle count (covered by StateManager tests).
        state_version = elem.payload.return_invocation.return_value.worker_state_response.state_version
        # a missing/dropped version would echo through the read-back; guard it
        assert state_version > 0
        assert elem == DCMElement(
            tag=mock_control_output_channel,
            payload=DirectControlMessagePayloadV2(
                return_invocation=ReturnInvocation(
                    command_id=command_sequence,
                    return_value=ControlReturn(
                        worker_state_response=WorkerStateResponse(
                            WorkerState.RUNNING, state_version=state_version
                        )
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
            StartChannel(0)
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
        main_loop.context.loop_start_port_uris = {"loop-start-1": "vfs:///x"}

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

    def test_loopstart_merges_unstamped_state_instead_of_forwarding_it(
        self, main_loop, monkeypatch
    ):
        # The deliberate asymmetry with the LoopEnd branch: an UNstamped
        # counter-0 frame at a LoopStart is MERGED into the loop variables,
        # not forwarded. It has to be -- the back-edge writes the next
        # iteration's variables to this LoopStart's own input-port state URI
        # with the identical "no loop" envelope (State.to_tuple(0)), so a
        # LoopStart cannot tell the loop's own state from an upstream/body
        # operator's boundary state. A LoopEnd can, because its inbound loop
        # state is always stamped.
        class StubLoopStart(LoopStartOperator):
            def process_table(self, table, port):
                yield

        executor = StubLoopStart()
        main_loop.context.executor_manager.executor = executor
        emitted, switched, reset_calls = self._capture_state_emit(
            main_loop, monkeypatch
        )
        monkeypatch.setattr(
            main_loop.context.state_processing_manager,
            "get_output_state",
            lambda: None,
        )

        main_loop._process_state_frame(
            StateFrame(State({"seed": 7}), loop_counter=0, loop_start_id="")
        )

        assert emitted == [], "an unstamped state at a LoopStart is not forwarded"
        assert switched == [True], "it reaches the operator, which merges it"
        assert reset_calls == []
        # It is handed to the operator as the current input state, so
        # LoopStartOperator.process_state merges it into self.state.
        passed = main_loop.context.state_processing_manager.current_input_state
        assert passed == State({"seed": 7})

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

    def test_loopend_consume_defers_operator_to_end_channel(
        self, main_loop, monkeypatch
    ):
        # loop_counter == 0 is the matching loop. The runtime STASHES the state
        # here and runs the operator at EndChannel instead
        # (_consume_pending_loop_state): the loop's input table is read from the
        # Loop Start's input-port materialization, and that read must not
        # overlap this worker's own materialization reader, which is still
        # streaming at consume time. Nothing observable moves: the matching
        # consume emits no state downstream either way.
        # Reviewer feedback (#discussion_r3285892237): the envelope's loop
        # metadata (loop_counter / loop_start_id) is internal runtime data --
        # the runtime captures it onto its own instance state, and the
        # user-facing State handed to the operator carries only the inner
        # State's keys, never the envelope names.
        executor = _FalseLoopEnd()
        main_loop.context.executor_manager.executor = executor
        emitted, switched, reset_calls = self._capture_state_emit(
            main_loop, monkeypatch
        )
        # No output from the operator -> no emit work after consume.
        monkeypatch.setattr(
            main_loop.context.state_processing_manager,
            "get_output_state",
            lambda: None,
        )
        # Stub the runtime's table read (the real one opens the Loop Start's
        # input-port materialization; pinned by the jump/read URI tests).
        loop_table = Table([Tuple({"v": 1})])
        reads = []

        def _read():
            reads.append(True)
            return loop_table

        monkeypatch.setattr(main_loop, "_read_loop_input_table", _read)

        incoming = State({"i": 42, "acc": [1, 2, 3]})
        main_loop._process_state_frame(
            StateFrame(incoming, loop_counter=0, loop_start_id="outer-loop")
        )

        # At consume: state stashed, operator NOT invoked, table NOT read yet
        # (no storage I/O while this worker's reader is still streaming).
        assert switched == [], "consume must not invoke the operator yet"
        assert reads == [], "the table must not be read at consume time"
        assert main_loop._pending_loop_state is incoming
        assert executor._attached_table is None
        assert emitted == [], "the matching consume emits no state downstream"
        assert reset_calls == [], "consume / single loop must not reset output"
        # The runtime captured the envelope metadata onto its own instance
        # state...
        assert main_loop._loop_start_id == "outer-loop"
        # ...but never wrote it into the user-facing State the operator sees.
        # (The consume branch sets `current_input_state` BEFORE the stubbed
        # context switch, so this is exactly what the operator would receive.)
        # ...and the state it stashed for the operator carries only the inner
        # State's keys, never the envelope names.
        assert set(main_loop._pending_loop_state.keys()) == {"i", "acc"}
        assert "loop_start_id" not in main_loop._pending_loop_state
        assert "loop_counter" not in main_loop._pending_loop_state

        # Then at EndChannel the deferred consume runs: the table is read once
        # (the reader has finished by now) and handed to the operator.
        consumed = []
        monkeypatch.setattr(
            executor, "process_state", lambda st, port: consumed.append((st, port))
        )

        main_loop._consume_pending_loop_state(executor)

        assert reads == [True], "the table is read exactly once, at EndChannel"
        assert executor._attached_table is loop_table
        assert consumed == [(incoming, 0)]
        assert main_loop._pending_loop_state is None, "stash must be cleared"

    def test_loopend_forwards_unstamped_state_without_consuming(
        self, main_loop, monkeypatch
    ):
        # A loop-body operator that emits its own boundary state
        # (produce_state_on_start/finish -- a public API on both engine sides)
        # sends it with the "no loop" envelope (counter 0, id ""). That state
        # is NOT the loop's own boundary state: taking it would clobber the
        # captured back-jump id with "" and then fail the deferred consume's
        # bookkeeping-URI lookup for LoopStart ''. A real loop state is always
        # stamped --
        # the matching LoopStart stamps its own id on every iteration's output
        # -- so an UNstamped counter-0 frame at a LoopEnd must be forwarded
        # downstream unchanged, skipping the operator, like any default
        # pass-through.
        main_loop.context.executor_manager.executor = _FalseLoopEnd()
        emitted, switched, reset_calls = self._capture_state_emit(
            main_loop, monkeypatch
        )
        # The loop's own state was already consumed and its id captured.
        main_loop._loop_start_id = "loop-start-1"

        main_loop._process_state_frame(
            StateFrame(
                State({"note": "from-body-op"}),
                loop_counter=0,
                loop_start_id="",
            )
        )

        assert switched == [], "unstamped state must not invoke the operator"
        assert reset_calls == [], "unstamped state must not reset output"
        assert emitted == [(State({"note": "from-body-op"}), 0, "")], (
            "unstamped state must forward downstream with its envelope "
            f"unchanged; emitted: {emitted}"
        )
        assert main_loop._loop_start_id == "loop-start-1", (
            "an unstamped state must not clobber the captured back-jump id"
        )
        # Forwarding an unstamped state must NOT count as taking the loop's
        # own state -- that is what the completion guard keys on.
        assert main_loop._loop_state_consumed is False

    @pytest.mark.timeout(2)
    def test_end_channel_holds_the_region_when_only_unstamped_states_arrived(
        self, main_loop, monkeypatch
    ):
        # Forwarding unstamped states is right for a body operator's own
        # boundary state, but it must not swallow the symptom of a LOST stamp:
        # a hop that blanks the envelope (the bug class #6660/#6661 fixed)
        # makes the loop's OWN state arrive unstamped, and forwarding it leaves
        # _loop_table None -> condition() False -> one iteration reported as
        # success. A Loop End that forwarded an unstamped state and never took
        # a stamped one must fail loudly instead -- and it must do so from
        # _process_end_channel, BEFORE port_completed goes out, because region
        # completion is port-based: reported from complete() the error would
        # arrive after the coordinator already considers the region done.
        executor = _FalseLoopEnd()
        main_loop.context.executor_manager.executor = executor
        self._capture_state_emit(main_loop, monkeypatch)

        completed = []
        port_completed_calls = []
        console_msgs = []
        monkeypatch.setattr(main_loop, "process_input_tuple", lambda: None)
        monkeypatch.setattr(main_loop, "complete", lambda: completed.append(True))
        monkeypatch.setattr(
            main_loop, "_send_console_message", lambda msg: console_msgs.append(msg)
        )
        monkeypatch.setattr(
            main_loop.context.pause_manager,
            "pause",
            lambda pause_type, change_state=True: None,
        )

        class _Coordinator:
            def port_completed(self, request):
                port_completed_calls.append(request)

        monkeypatch.setattr(
            main_loop._async_rpc_client, "coordinator_stub", lambda: _Coordinator()
        )

        # The loop's own state arrives with its stamp lost upstream.
        main_loop._process_state_frame(
            StateFrame(State({"i": 0}), loop_counter=0, loop_start_id="")
        )
        assert main_loop._forwarded_unstamped_state is True
        assert main_loop._loop_state_consumed is False

        monkeypatch.setattr(main_loop, "process_input_state", lambda *a, **k: None)
        main_loop._process_end_channel()

        assert main_loop.context.exception_manager.has_exception()
        error_msgs = [m for m in console_msgs if m.msg_type == ConsoleMessageType.ERROR]
        assert len(error_msgs) == 1
        assert "loop envelope was lost upstream" in error_msgs[0].title
        assert port_completed_calls == [], "no port may be reported complete"
        assert completed == [], "the worker must not complete"

    def test_complete_accepts_unstamped_state_alongside_the_loop_state(
        self, main_loop, monkeypatch
    ):
        # The legitimate shape this PR enables: a body operator's boundary
        # state is forwarded AND the loop's own stamped state is consumed. The
        # completion guard must stay silent, in either arrival order.
        for body_state_first in (True, False):
            executor = _FalseLoopEnd()
            main_loop.context.executor_manager.executor = executor
            main_loop._forwarded_unstamped_state = False
            main_loop._loop_state_consumed = False
            main_loop._loop_start_id = ""
            self._capture_state_emit(main_loop, monkeypatch)
            monkeypatch.setattr(
                main_loop.context.state_processing_manager,
                "get_output_state",
                lambda: None,
            )

            body = StateFrame(
                State({"note": "from-body-op"}), loop_counter=0, loop_start_id=""
            )
            loop = StateFrame(
                State({"i": 0}), loop_counter=0, loop_start_id="outer-loop"
            )
            for frame in (body, loop) if body_state_first else (loop, body):
                main_loop._process_state_frame(frame)

            # Must not raise, in either order.
            main_loop._check_loop_state_arrived()
            assert main_loop._forwarded_unstamped_state is True
            assert main_loop._loop_state_consumed is True
            assert main_loop._loop_start_id == "outer-loop"

            # The guard must key on the DURABLE evidence that a stamped state
            # was taken (_loop_start_id), not on the fan-in dedup flag: that
            # flag belongs to the dedup, and a caller may legitimately clear
            # it once the iteration's state has been taken. Keyed on the flag,
            # this legitimate shape would start raising the moment anything
            # re-armed it -- so simulate that and require silence.
            main_loop._loop_state_consumed = False
            main_loop._check_loop_state_arrived()

    def test_loopend_consumes_its_loop_state_once_per_iteration(
        self, main_loop, monkeypatch
    ):
        # A loop body may branch and converge on the Loop End, so its input
        # port takes fan-in. Every reader on that port replays its own
        # branch's states, so the SAME iteration's state arrives once per
        # branch. The stash must take the FIRST and drop the rest -- a later
        # arrival silently overwriting _pending_loop_state would make the
        # consumed copy depend on reader scheduling. Assert through the whole
        # deferred path, not just the stash.
        executor = _FalseLoopEnd()
        main_loop.context.executor_manager.executor = executor
        emitted, switched, reset_calls = self._capture_state_emit(
            main_loop, monkeypatch
        )
        monkeypatch.setattr(
            main_loop.context.state_processing_manager,
            "get_output_state",
            lambda: None,
        )
        monkeypatch.setattr(
            main_loop, "_read_loop_input_table", lambda: Table([Tuple({"v": 1})])
        )
        consumed = []
        monkeypatch.setattr(
            executor, "process_state", lambda st, port: consumed.append((st, port))
        )

        first = State({"i": 42})
        second = State({"i": 42})

        def deliver(state):
            main_loop._process_state_frame(
                StateFrame(state, loop_counter=0, loop_start_id="outer-loop")
            )

        deliver(first)  # branch A
        deliver(second)  # branch B replays the same iteration's state

        assert main_loop._loop_state_consumed is True
        assert main_loop._pending_loop_state is first, "the duplicate must not stash"
        assert emitted == [], "a consume emits nothing downstream, duplicate or not"
        assert reset_calls == []
        assert main_loop._loop_start_id == "outer-loop"

        main_loop._consume_pending_loop_state(executor)

        assert switched == [], "the deferred consume does not switch context"
        assert consumed == [(first, 0)], "the operator must update exactly once"
        # The consume re-arms the duplicate guard (everything on the port is
        # already in by EndChannel), making the flag per-execution by
        # construction rather than by worker recreation.
        assert main_loop._loop_state_consumed is False

    # ------------------------------------------------------------------ #
    # _jump_to_loop_start
    #
    # Reviewer feedback (#discussion_r3285892249) flagged the loop-back
    # path as the most fragile loop-runtime code. The id a LoopStart
    # stamps is now computed inline in process_input_state via the
    # canonical `get_logical_op_id` (pinned by that helper's own suite),
    # and the loop-back write address is not computed worker-side at all:
    # it is setup config (InitializeExecutorRequest.loopStartPortUris --
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

    @pytest.mark.timeout(2)
    def test_complete_flushes_prints_from_the_user_condition(
        self, main_loop, monkeypatch
    ):
        # complete() flushes console messages on entry, before condition()
        # runs, and then shuts the worker down -- so without a second flush a
        # print() inside the user's condition would be captured and never sent.
        #
        # Mirrors LoopEndOpDesc.generatePythonCode: the user's text goes
        # through eval_condition, i.e. eval() against a bare namespace dict --
        # NOT a method of a module-level class -- so the print executes in a
        # frame whose globals have no __name__. The capture must survive that
        # (replace_print looks the module name up with .get); a plain method
        # here would pass even with a capture that crashes on the generated
        # path.
        class _PrintingLoopEnd(LoopEndOperator):
            def condition(self):
                return self.eval_condition("print('hello from condition') or False")

        executor = _PrintingLoopEnd()
        # eval_condition short-circuits to False before a consume; run a
        # minimal successful update first so the user expression actually
        # evaluates (this is the state a real matching consume leaves behind).
        executor.attach_loop_table(Table([Tuple({"v": 1})]))
        executor.run_update("pass", State())
        main_loop.context.executor_manager.executor = executor

        console_msgs = []
        monkeypatch.setattr(
            main_loop, "_send_console_message", lambda msg: console_msgs.append(msg)
        )
        monkeypatch.setattr(main_loop.data_processor, "stop", lambda: None)
        monkeypatch.setattr(
            main_loop.context.state_manager, "transit_to", lambda state: None
        )
        monkeypatch.setattr(main_loop.context, "close", lambda: None)

        class _Coordinator:
            def worker_execution_completed(self, request):
                pass

        monkeypatch.setattr(
            main_loop._async_rpc_client, "coordinator_stub", lambda: _Coordinator()
        )

        main_loop.complete()

        printed = [m for m in console_msgs if "hello from condition" in m.title]
        assert printed, (
            "a print() in the user's condition must reach the console before "
            f"the worker completes; sent: {[m.title for m in console_msgs]}"
        )

    def test_deferred_consume_captures_user_prints(self, main_loop, monkeypatch):
        # The `update` runs on the main loop thread, outside
        # DataProcessor._executor_session, so its print capture has to be
        # applied explicitly here -- otherwise a print() in the user's update
        # goes to the worker's stdout and never reaches the console.
        #
        # Mirrors LoopEndOpDesc.generatePythonCode: the user's text goes
        # through run_update, i.e. exec() against a bare namespace dict, so
        # the print executes in a frame whose globals have no __name__ and the
        # capture must survive that (replace_print looks the module name up
        # with .get). A plain print() in an overridden method here would pass
        # even with a capture that crashes on the generated path.
        class _PrintingLoopEnd(LoopEndOperator):
            def condition(self):
                return self.eval_condition("False")

            def process_state(self, state, port):
                self.run_update("print('hello from update')\ni += 1", state)
                return None

        executor = _PrintingLoopEnd()
        main_loop.context.executor_manager.executor = executor
        main_loop._pending_loop_state = State({"i": 1})
        monkeypatch.setattr(
            main_loop, "_read_loop_input_table", lambda: Table([Tuple({"v": 1})])
        )

        main_loop._consume_pending_loop_state(executor)

        printed = [
            msg
            for msg in main_loop.context.console_message_manager.get_messages(
                force_flush=True
            )
            if "hello from update" in msg.title
        ]
        assert printed, "the user's print must be captured as a console message"
        assert printed[0].msg_type == ConsoleMessageType.PRINT

    def test_read_loop_input_table_opens_the_result_uri_of_the_configured_base(
        self, main_loop, monkeypatch
    ):
        # The other half of the base-URI split (the jump test pins the state
        # URI): the loop's input table is read from result_uri(base) of the
        # SAME configured base, through DocumentFactory.open_document.
        main_loop._loop_start_id = "outer-loop"
        main_loop.context.loop_start_port_uris = {"outer-loop": "vfs:///wf/port/outer"}

        opened = []
        rows = [Tuple({"v": 1}), Tuple({"v": 2})]
        schema = Schema(raw_schema={"v": "LONG"})
        casts = []
        for tup in rows:
            monkeypatch.setattr(
                tup,
                "cast_to_schema",
                lambda s, _t=tup: casts.append((_t, s)),
                raising=True,
            )

        class _Doc:
            def get(self):
                return iter(rows)

        monkeypatch.setattr(
            "core.runnables.main_loop.DocumentFactory.open_document",
            lambda uri: (opened.append(uri) or (_Doc(), schema)),
        )

        table = main_loop._read_loop_input_table()

        assert opened == [VFSURIFactory.result_uri("vfs:///wf/port/outer")]
        assert isinstance(table, Table)
        assert list(table.as_tuples()) == rows
        # Every tuple is normalized to the doc's schema, exactly like the
        # input-port reader that streams this same doc.
        assert casts == [(tup, schema) for tup in rows]

    def test_read_loop_input_table_raises_when_uri_not_configured(
        self, main_loop, monkeypatch
    ):
        # Same fail-loud contract as the back-edge write: a LoopEnd whose
        # captured id has no setup-config entry must raise rather than read
        # from a guessed location.
        main_loop._loop_start_id = "outer-loop"
        main_loop.context.loop_start_port_uris = {}
        opened = []
        monkeypatch.setattr(
            "core.runnables.main_loop.DocumentFactory.open_document",
            lambda uri: (opened.append(uri) or (None, None)),
        )

        with pytest.raises(RuntimeError, match="no loop bookkeeping URI"):
            main_loop._read_loop_input_table()

        assert opened == [], "must fail before touching storage"

    @pytest.mark.timeout(2)
    def test_end_channel_holds_the_region_when_the_deferred_consume_fails(
        self, main_loop, monkeypatch
    ):
        # The deferred consume runs the table read and the user's `update`.
        # Both can fail, and the failure must hold the region: complete() is
        # the tail of _process_end_channel, so reporting the error there would
        # arrive after port_completed had already been sent for every port and
        # would read as a false success (region completion is port-based).
        class _BoomLoopEnd(LoopEndOperator):
            def condition(self):
                return False

            def process_state(self, state, port):
                raise ValueError("name 'i' is not defined")

        executor = _BoomLoopEnd()
        main_loop.context.executor_manager.executor = executor
        main_loop._pending_loop_state = State({"i": 1})
        monkeypatch.setattr(
            main_loop, "_read_loop_input_table", lambda: Table([Tuple({"v": 1})])
        )

        completed = []
        port_completed_calls = []
        console_msgs = []
        monkeypatch.setattr(main_loop, "process_input_state", lambda *a, **k: None)
        monkeypatch.setattr(main_loop, "process_input_tuple", lambda: None)
        monkeypatch.setattr(main_loop, "complete", lambda: completed.append(True))
        monkeypatch.setattr(
            main_loop, "_send_console_message", lambda msg: console_msgs.append(msg)
        )
        monkeypatch.setattr(
            main_loop.context.pause_manager,
            "pause",
            lambda pause_type, change_state=True: None,
        )

        class _Coordinator:
            def port_completed(self, request):
                port_completed_calls.append(request)

        monkeypatch.setattr(
            main_loop._async_rpc_client, "coordinator_stub", lambda: _Coordinator()
        )

        main_loop._process_end_channel()

        assert main_loop.context.exception_manager.has_exception()
        error_msgs = [m for m in console_msgs if m.msg_type == ConsoleMessageType.ERROR]
        assert len(error_msgs) == 1
        assert "name 'i' is not defined" in error_msgs[0].title
        assert port_completed_calls == [], "no port may be reported complete"
        assert completed == [], "the worker must not complete"

    def test_jump_to_loop_start_sends_rpc_then_writes_state_in_order(
        self, main_loop, monkeypatch
    ):
        # One shared event log for the jump RPC and the storage calls, so
        # the cross-channel ordering is pinned along with each contract.
        main_loop._loop_start_id = "outer-loop"
        # The bookkeeping BASE URI is setup-injected config keyed by the
        # captured id; the write address is derived from it (state_uri).
        main_loop.context.loop_start_port_uris = {"outer-loop": "vfs:///wf/port/outer"}

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
        # create_document with the state URI DERIVED from the configured base
        # URI and State.SCHEMA, open
        # writer("0"), a single put_one with the State as a depth-0 tuple
        # (the back-edge fires only after the matching LoopEnd consumed at
        # loop_counter == 0, so the next iteration starts at depth 0),
        # then close. The tuple object's internals are exercised elsewhere.
        assert events[1] == (
            "create_document",
            VFSURIFactory.state_uri("vfs:///wf/port/outer"),
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
        main_loop.context.loop_start_port_uris = {}

        rpc_calls = []
        write_log = []
        self._patch_create_document(monkeypatch, write_log)

        class _Executor:
            state = State({"i": 7})

        with pytest.raises(RuntimeError, match="no loop bookkeeping URI"):
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

    # ------------------------------------------------------------------ #
    # Deferred loop consume: the "nothing was stashed" shape
    # ------------------------------------------------------------------ #

    @pytest.mark.timeout(5)
    def test_deferred_consume_is_a_noop_when_no_state_was_stashed(
        self, main_loop, monkeypatch
    ):
        # A Loop End may legally reach EndChannel having never taken a matching
        # loop state -- LoopEndOperator.eval_condition's `_loop_table is None`
        # guard makes that a supported shape (the loop simply does not iterate),
        # and _check_loop_state_arrived deliberately does not treat it as an
        # error. The deferred consume must therefore return without doing any
        # loop work: reading the Loop Start's input-port materialization for a
        # loop that never ran would touch storage for no reason, and clearing
        # the fan-in dedup flag would rewrite bookkeeping owned by a consume
        # that did not happen.
        executor = _FalseLoopEnd()
        main_loop.context.executor_manager.executor = executor
        assert main_loop._pending_loop_state is None

        reads = []
        loop_table = Table([Tuple({"v": 1})])

        def _read():
            reads.append(True)
            return loop_table

        monkeypatch.setattr(main_loop, "_read_loop_input_table", _read)
        consumed = []
        monkeypatch.setattr(
            executor, "process_state", lambda st, port: consumed.append((st, port))
        )
        # Arm the dedup flag so the early return is distinguishable from the
        # consume path, whose whole point is to clear it.
        main_loop._loop_state_consumed = True

        main_loop._consume_pending_loop_state(executor)

        assert reads == [], "no stashed state means no storage read"
        assert consumed == [], "the operator's update must not run"
        assert executor._attached_table is None, "no table may be attached"
        assert main_loop._loop_state_consumed is True, (
            "the early return must not rewrite the fan-in dedup flag"
        )

    @pytest.mark.timeout(5)
    def test_loopstart_stamps_its_own_logical_op_id_on_its_output_state(
        self, main_loop, monkeypatch
    ):
        # The stamp is what lets the matching Loop End find the loop to jump
        # back to (it rides the StateFrame envelope and is captured in
        # _process_state_frame). A LoopStart must therefore REPLACE whatever id
        # arrived with its own logical op id rather than forward the inbound
        # one -- forwarding "" is exactly the lost-envelope bug class
        # #6660/#6661 fixed, and _check_loop_state_arrived only notices that
        # once a whole input port has drained.
        class StubLoopStart(LoopStartOperator):
            def process_table(self, table, port):
                yield

        main_loop.context.executor_manager.executor = StubLoopStart()
        # get_logical_op_id parses the canonical worker actor name
        # "Worker:WF<wf>-<opId>-<layer>-<idx>" and raises on anything else, so
        # the fixture's "dummy_worker_id" has to be replaced here.
        main_loop.context.worker_id = "Worker:WF7-my-loop-start-main-0"
        emitted, _, _ = self._capture_state_emit(main_loop, monkeypatch)
        # Both stubs append to ONE list so the ORDER is asserted rather than
        # merely that each happened. _switch_context is what hands control to
        # the DataProcessor thread that PRODUCES the state, so reading the
        # output state first would emit the previous iteration's state (or
        # None). Recording the switch in its own separate list -- what the
        # shared _capture_state_emit helper does -- can only witness THAT the
        # switch occurred, never that it came first.
        order = []
        monkeypatch.setattr(
            main_loop, "_switch_context", lambda: order.append("switch")
        )
        monkeypatch.setattr(
            main_loop.context.state_processing_manager,
            "get_output_state",
            lambda: (order.append("read"), State({"i": 3}))[1],
        )

        # The inbound envelope carried no id -- the shape a first-entry state
        # and the back-edge write both have.
        main_loop.process_input_state(output_loop_counter=0, output_loop_start_id="")

        assert order == ["switch", "read"], (
            f"the operator must run before its output is read; got {order}"
        )
        assert len(emitted) == 1
        emitted_state, emitted_counter, emitted_id = emitted[0]
        assert emitted_state == State({"i": 3})
        # 0 is simultaneously the inbound value, process_input_state's own
        # parameter default and the expectation, so this pins the emitted
        # tuple's SHAPE only -- it cannot tell a pass-through from a hardcoded
        # 0. The pass-through itself is pinned by
        # test_body_operator_state_forwards_the_inbound_loop_counter_and_id,
        # which passes values no default can supply.
        assert emitted_counter == 0
        assert emitted_id == "my-loop-start", (
            "a LoopStart must stamp its own logical op id, not forward the "
            f"inbound one; emitted id: {emitted_id!r}"
        )
        # `reset_calls` is deliberately NOT asserted here: reset_output_storage
        # has exactly one call site, in _process_state_frame (main_loop.py:512),
        # so nothing process_input_state reaches could fire it and the
        # assertion could never fail. That reset is pinned where it happens, by
        # test_loopend_passthrough_decrements_resets_output_and_skips_operator.

    @pytest.mark.timeout(5)
    def test_body_operator_state_forwards_the_inbound_loop_counter_and_id(
        self, main_loop, monkeypatch
    ):
        # A plain loop-BODY operator is the shape that carries a NON-zero
        # counter: _process_state_frame's default tail calls
        # process_input_state(output_loop_counter=in_counter,
        # output_loop_start_id=frame.loop_start_id), so both envelope fields
        # have to reach _emit_and_save_state unchanged. Blanking the counter
        # would strip the iteration number off every body operator's boundary
        # state -- the lost-envelope class #6660/#6661 fixed -- and the values
        # used here (7, "outer-loop") are ones no parameter default can supply,
        # so this discriminates a real forward from a constant.
        assert not isinstance(
            main_loop.context.executor_manager.executor, LoopStartOperator
        ), "a body operator is not a LoopStart, so no id stamping happens here"
        emitted, _, _ = self._capture_state_emit(main_loop, monkeypatch)
        monkeypatch.setattr(
            main_loop.context.state_processing_manager,
            "get_output_state",
            lambda: State({"i": 3}),
        )

        main_loop.process_input_state(
            output_loop_counter=7, output_loop_start_id="outer-loop"
        )

        assert emitted == [(State({"i": 3}), 7, "outer-loop")], (
            f"both envelope fields must be forwarded verbatim; emitted: {emitted}"
        )

    # ------------------------------------------------------------------ #
    # _process_end_channel: the two guards that hold a worker open
    # ------------------------------------------------------------------ #

    def _register_input_port(self, main_loop, schema, port_id, sender):
        """Register one input port carrying a single data channel and return
        that channel. Uses the public InputManager API rather than a DCM
        round-trip so a test can build a multi-port worker directly."""
        channel_id = ChannelIdentity(
            ActorVirtualIdentity(sender),
            ActorVirtualIdentity("dummy_worker_id"),
            False,
        )
        main_loop.context.input_manager.add_input_port(port_id, schema, [], [])
        main_loop.context.input_manager.register_input(channel_id, port_id)
        return channel_id

    def _capture_end_channel_effects(self, main_loop, monkeypatch):
        """Stub out everything _process_end_channel does apart from the
        completion decision itself, and return
        (port_completed_calls, closed, completed)."""
        port_completed_calls = []
        closed = []
        completed = []
        monkeypatch.setattr(main_loop, "process_input_state", lambda *a, **k: None)
        monkeypatch.setattr(main_loop, "process_input_tuple", lambda: None)
        monkeypatch.setattr(main_loop, "complete", lambda: completed.append(True))
        monkeypatch.setattr(
            main_loop.context.output_manager,
            "close_port_storage_writers",
            lambda: closed.append(True),
        )

        class _Coordinator:
            def port_completed(self, request):
                port_completed_calls.append(request)

        monkeypatch.setattr(
            main_loop._async_rpc_client, "coordinator_stub", lambda: _Coordinator()
        )
        return port_completed_calls, closed, completed

    @pytest.mark.timeout(5)
    def test_end_channel_does_not_complete_while_a_second_input_port_is_open(
        self, main_loop, monkeypatch, mock_raw_schema
    ):
        # A worker with several input ports gets one EndChannel per port. The
        # first of them must report ITS port complete and stop there: closing
        # the storage writers or calling complete() while another port is still
        # streaming would truncate that port's results and let the coordinator
        # mark the region done early (region completion is port-based).
        schema = Schema(raw_schema=mock_raw_schema)
        port_0 = PortIdentity(0, internal=False)
        port_1 = PortIdentity(1, internal=False)
        self._register_input_port(main_loop, schema, port_0, "sender-0")
        channel_1 = self._register_input_port(main_loop, schema, port_1, "sender-1")
        # The port that FINISHES is deliberately the NON-zero one. With port 0
        # finishing, "report the arriving channel's port" and "always report
        # port 0" are the same program: port 0 would be the finished port, the
        # lowest port id and the expected value at once. A worker hardcoded to
        # port 0 lets the coordinator close a port that is still streaming,
        # since region completion is port-based.
        main_loop.context.input_manager.complete_current_port(channel_1)
        main_loop.context.current_input_channel_id = channel_1
        assert not main_loop.context.input_manager.all_ports_completed(), (
            "port 0 must still be open for this test to mean anything"
        )
        # The output port is what makes this test non-vacuous: with none, the
        # is_missing_output_ports() guard below returns early regardless of what
        # all_ports_completed() answered, and a broken port-completion check
        # would go unnoticed.
        main_loop.context.output_manager.add_output_port(port_0, schema)
        assert not main_loop.context.output_manager.is_missing_output_ports()

        port_completed_calls, closed, completed = self._capture_end_channel_effects(
            main_loop, monkeypatch
        )

        main_loop._process_end_channel()

        assert port_completed_calls == [
            PortCompletedRequest(port_id=port_1, input=True)
        ], (
            "only the FINISHED input port may be reported complete; got "
            f"{port_completed_calls}"
        )
        assert closed == [], "an open input port must keep the storage writers open"
        assert completed == [], "the worker must not complete with a port still open"

    @staticmethod
    def _queued_size(output_queue, channel):
        """How many elements are queued for `channel`.

        Sub-queues are created lazily on first put, so an untouched channel is
        simply absent -- reading it as 0 rather than raising lets a test assert
        "nothing was sent here" without depending on whether the sub-queue was
        ever created.
        """
        try:
            return output_queue._queue.size(channel)
        except KeyError:
            return 0

    @pytest.mark.timeout(5)
    def test_end_channel_holds_the_worker_open_when_it_has_no_output_ports(
        self, main_loop, monkeypatch, output_queue, mock_raw_schema
    ):
        # The two-phase dependee-port region shape (see
        # OutputManager.is_missing_output_ports): this worker's only input port
        # has finished, but it has no output port at all, which means it is
        # running the dependee-port phase and must stay open for the
        # non-dependee-port phase that follows. So it reports its input port
        # complete and then stops -- no storage close, no EndChannel ECM, no
        # complete().
        schema = Schema(raw_schema=mock_raw_schema)
        port_0 = PortIdentity(0, internal=False)
        channel_0 = self._register_input_port(main_loop, schema, port_0, "sender-0")
        main_loop.context.input_manager.complete_current_port(channel_0)
        main_loop.context.current_input_channel_id = channel_0
        assert main_loop.context.input_manager.all_ports_completed()
        assert main_loop.context.output_manager.is_missing_output_ports()
        # A downstream data CHANNEL with no output PORT. OutputManager keeps
        # _ports and _channels in independent dicts (output_manager.py:85-86)
        # and add_partitioning only writes _channels, so the hold-open guard
        # still fires while get_output_channel_ids() has somewhere to send.
        # Without this channel _send_ecm_to_data_channels is a no-op whatever
        # the guard does, and a premature EndChannel broadcast injected into
        # the guard -- which would close downstream ports before the
        # non-dependee-port phase runs, the exact failure the two-phase scheme
        # exists to prevent -- would go unnoticed.
        downstream = ChannelIdentity(
            ActorVirtualIdentity("dummy_worker_id"),
            ActorVirtualIdentity("downstream"),
            False,
        )
        main_loop.context.output_manager.add_partitioning(
            PhysicalLink(
                from_op_id=PhysicalOpIdentity(OperatorIdentity("from"), "from"),
                from_port_id=PortIdentity(0, internal=False),
                to_op_id=PhysicalOpIdentity(OperatorIdentity("to"), "to"),
                to_port_id=PortIdentity(0, internal=False),
            ),
            set_one_of(
                Partitioning,
                OneToOnePartitioning(batch_size=1, channels=[downstream]),
            ),
        )
        assert main_loop.context.output_manager.is_missing_output_ports(), (
            "registering a channel must not create an output port"
        )
        assert list(main_loop.context.output_manager.get_output_channel_ids()) == [
            downstream
        ], "the broadcast must have a live channel to reach"

        port_completed_calls, closed, completed = self._capture_end_channel_effects(
            main_loop, monkeypatch
        )

        main_loop._process_end_channel()

        assert port_completed_calls == [
            PortCompletedRequest(port_id=port_0, input=True)
        ], "the input port is still reported complete"
        assert closed == [], (
            "the dependee-port phase must not close the storage writers"
        )
        assert self._queued_size(output_queue, downstream) == 0, (
            "the dependee-port phase must not send EndChannel downstream"
        )
        assert completed == [], "the worker must stay open for the next phase"

    # ------------------------------------------------------------------ #
    # _process_ecm: per-worker command dispatch and scoped forwarding
    # ------------------------------------------------------------------ #

    @staticmethod
    def _no_op_invocation():
        return ControlInvocation(
            "NoOperation",
            ControlRequest(empty_request=EmptyRequest()),
            AsyncRpcContext(ActorVirtualIdentity(), ActorVirtualIdentity()),
            -1,
        )

    @pytest.mark.timeout(5)
    def test_ecm_dispatches_only_the_command_addressed_to_this_worker(
        self, main_loop, monkeypatch, mock_data_input_channel
    ):
        # An ECM's command_mapping is keyed by worker id: a message travelling
        # a scope carries commands only for the workers that must act on it,
        # and every other worker on the path still has to align and propagate
        # it. Handing the missing entry (None) to the RPC server would dispatch
        # a control invocation with no method. Both directions are asserted
        # here so the guard is pinned as a discriminator rather than as a path
        # that merely happens to be taken.
        received = []
        monkeypatch.setattr(
            main_loop._async_rpc_server,
            "receive",
            lambda channel_id, command: received.append((channel_id, command)),
        )
        main_loop.context.current_input_channel_id = mock_data_input_channel

        def deliver(ecm_id, command_mapping):
            main_loop._process_ecm(
                ECMElement(
                    tag=mock_data_input_channel,
                    payload=EmbeddedControlMessage(
                        EmbeddedControlMessageIdentity(ecm_id),
                        EmbeddedControlMessageType.NO_ALIGNMENT,
                        [],
                        command_mapping,
                    ),
                )
            )

        deliver(
            "ecm-for-somebody-else", {"some-other-worker": self._no_op_invocation()}
        )
        assert received == [], (
            "an ECM carrying no command for this worker must dispatch nothing; "
            f"dispatched: {received}"
        )

        mine = self._no_op_invocation()
        deliver("ecm-for-me", {"dummy_worker_id": mine})
        assert received == [(mock_data_input_channel, mine)], (
            "an ECM carrying a command for this worker must dispatch it; "
            f"dispatched: {received}"
        )

    @pytest.mark.timeout(5)
    def test_ecm_is_forwarded_only_to_the_output_channels_in_its_scope(
        self, main_loop, output_queue, mock_data_input_channel
    ):
        # An ECM's scope is the set of channels it is allowed to travel. A
        # worker with several downstream channels must forward the message only
        # along the ones the scope names -- sending it down a channel outside
        # the scope would inject an alignment barrier into a region the message
        # was never meant to reach. Two output channels are required for this
        # to mean anything: with one, "forward to the in-scope channel" and
        # "forward to every channel" are the same program.
        in_scope = ChannelIdentity(
            ActorVirtualIdentity("dummy_worker_id"),
            ActorVirtualIdentity("downstream-in-scope"),
            False,
        )
        out_of_scope = ChannelIdentity(
            ActorVirtualIdentity("dummy_worker_id"),
            ActorVirtualIdentity("downstream-out-of-scope"),
            False,
        )
        link = PhysicalLink(
            from_op_id=PhysicalOpIdentity(OperatorIdentity("from"), "from"),
            from_port_id=PortIdentity(0, internal=False),
            to_op_id=PhysicalOpIdentity(OperatorIdentity("to"), "to"),
            to_port_id=PortIdentity(0, internal=False),
        )
        main_loop.context.output_manager.add_partitioning(
            link,
            set_one_of(
                Partitioning,
                OneToOnePartitioning(batch_size=1, channels=[in_scope, out_of_scope]),
            ),
        )
        assert set(main_loop.context.output_manager.get_output_channel_ids()) == {
            in_scope,
            out_of_scope,
        }

        main_loop.context.current_input_channel_id = mock_data_input_channel
        # The scope is expressed with a freshly built (equal, not identical)
        # ChannelIdentity, the way a real scope arrives off the wire.
        scoped_ecm = EmbeddedControlMessage(
            EmbeddedControlMessageIdentity("scoped-ecm"),
            EmbeddedControlMessageType.NO_ALIGNMENT,
            [
                ChannelIdentity(
                    ActorVirtualIdentity("dummy_worker_id"),
                    ActorVirtualIdentity("downstream-in-scope"),
                    False,
                )
            ],
            {},
        )

        main_loop._process_ecm(
            ECMElement(tag=mock_data_input_channel, payload=scoped_ecm)
        )

        assert self._queued_size(output_queue, out_of_scope) == 0, (
            "a channel outside the ECM's scope must receive nothing"
        )
        assert self._queued_size(output_queue, in_scope) == 1, (
            "the channel named by the scope must receive the ECM"
        )
        element = output_queue.get()
        assert isinstance(element, ECMElement)
        assert element.tag == in_scope
        assert element.payload.id == EmbeddedControlMessageIdentity("scoped-ecm")

    # ------------------------------------------------------------------ #
    # Console / debugger reporting
    # ------------------------------------------------------------------ #

    @pytest.mark.timeout(5)
    def test_console_message_is_sent_to_the_coordinator_as_an_rpc(
        self, main_loop, output_queue
    ):
        # Every console report -- user prints, operator errors, debugger events
        # -- funnels through _send_console_message, and the surrounding tests
        # all stub that method off the instance, so nothing pins the RPC it
        # actually makes. Let the real method run and read the resulting
        # control message off the output queue.
        msg = ConsoleMessage(
            worker_id="dummy_worker_id",
            timestamp=current_time_in_local_timezone(),
            msg_type=ConsoleMessageType.PRINT,
            source="pytest",
            title="hello from the operator",
            message="",
        )
        main_loop.context.console_message_manager.put_message(msg)

        main_loop._check_and_report_console_messages(force_flush=True)

        coordinator_channel = ChannelIdentity(
            ActorVirtualIdentity("dummy_worker_id"),
            ActorVirtualIdentity("COORDINATOR"),
            True,
        )
        # Check the queue is non-empty BEFORE reading it: output_queue.get()
        # blocks forever, so a regression that drops the RPC entirely would
        # hang here instead of failing (and on Windows pytest-timeout's default
        # `thread` method then kills the whole session, yielding no per-test
        # signal at all).
        queued = {
            str(key): output_queue._queue.size(key)
            for key in output_queue._queue.sub_queues
        }
        assert self._queued_size(output_queue, coordinator_channel) == 1, (
            f"the console message must be queued for the coordinator; queued: {queued}"
        )

        element = output_queue.get()
        assert isinstance(element, DCMElement)
        assert element.tag == coordinator_channel, (
            "console messages go to the coordinator on the control channel"
        )
        invocation = element.payload.control_invocation
        assert invocation.method_name == "ConsoleMessageTriggered"
        assert (
            invocation.command.console_message_triggered_request.console_message == msg
        )

    @pytest.mark.timeout(5)
    def test_debug_event_is_reported_as_a_debugger_message_and_pauses_the_worker(
        self, main_loop, monkeypatch
    ):
        # pdb writes its output into a SingleBlockingIO that the worker polls
        # after every context switch. An event there has to reach the frontend
        # as a DEBUGGER console message AND pause the worker -- without the
        # pause the debugger would report a breakpoint and then run straight
        # past it. The buffered prints have to go out on the same beat: the
        # worker is about to stop, so anything still sitting in the console
        # buffer when the pause lands would stay invisible until a resume.
        console_msgs = []
        pauses = []
        monkeypatch.setattr(
            main_loop, "_send_console_message", lambda msg: console_msgs.append(msg)
        )
        # The stub RECORDS change_state instead of dropping it: that kwarg is
        # what decides whether the worker actually reports itself PAUSED
        # (pause_manager.py:59-62 gates transit_to(WorkerState.PAUSED) on it).
        # With change_state=False the input queue is disabled but the state
        # never transits, i.e. the frontend shows RUNNING while the operator
        # sits at a breakpoint. The stub's own default is True, so an omitted
        # kwarg records True and an explicit False records False -- which is
        # what makes the pair discriminating rather than decorative.
        monkeypatch.setattr(
            main_loop.context.pause_manager,
            "pause",
            lambda pause_type, change_state=True: pauses.append(
                (pause_type, change_state)
            ),
        )
        buffered_print = ConsoleMessage(
            worker_id="dummy_worker_id",
            timestamp=current_time_in_local_timezone(),
            msg_type=ConsoleMessageType.PRINT,
            source="pytest",
            title="printed just before the breakpoint",
            message="",
        )
        main_loop.context.console_message_manager.put_message(buffered_print)
        # flush() is what makes the buffered text readable; without it
        # has_debug_event() stays False and readline() would block.
        debug_out = main_loop.context.debug_manager.debugger.stdout
        debug_out.write("> <string>(3)update()")
        debug_out.flush()
        assert main_loop.context.debug_manager.has_debug_event()

        before = current_time_in_local_timezone()
        main_loop._check_and_report_debug_event()
        after = current_time_in_local_timezone()

        assert len(console_msgs) == 2, f"sent: {console_msgs}"
        assert console_msgs[1] == buffered_print, (
            "the console buffer must be flushed before the worker pauses; "
            f"sent: {console_msgs}"
        )
        reported = console_msgs[0]
        assert reported.msg_type == ConsoleMessageType.DEBUGGER
        assert reported.source == "(Pdb)"
        assert "> <string>(3)update()" in reported.title, (
            f"the pdb event is the message title; got title={reported.title!r} "
            f"message={reported.message!r}"
        )
        assert reported.message == ""
        assert reported.worker_id == "dummy_worker_id"
        # `timestamp` is a plain betterproto datetime field, so `timestamp=None`
        # constructs cleanly and a DEBUGGER message would reach the frontend
        # with no time on it. Bracketing it against the same helper the runtime
        # uses pins that a real clock reading was taken.
        assert reported.timestamp is not None, "the console message needs a time"
        assert before <= reported.timestamp <= after, (
            f"timestamp {reported.timestamp!r} outside [{before!r}, {after!r}]"
        )
        assert pauses == [(PauseType.DEBUG_PAUSE, True)], (
            "the debug pause must also transit the worker to PAUSED "
            f"(change_state); recorded: {pauses}"
        )
        # The event was consumed, so a second poll reports nothing.
        assert not main_loop.context.debug_manager.has_debug_event()
        main_loop._check_and_report_debug_event()
        assert len(console_msgs) == 2

    @pytest.mark.timeout(5)
    def test_a_failing_element_does_not_abandon_the_rest_of_the_batch(
        self, main_loop, monkeypatch, mock_raw_schema
    ):
        # The batch iterator is the only handle on the elements still to come,
        # so letting a failure on one element escape the loop would silently
        # drop every element behind it. _process_data_element's per-element
        # backstop keeps iterating instead.
        #
        # Deliberately NOT asserted: that nothing is reported to the
        # coordinator. The backstop only logs, so a runtime-level per-element
        # failure never reaches Context.report_exception and the workflow can
        # report success on a short result -- arguably a silent-wrong-result
        # defect. Pinning that half would cement it, so this test asserts only
        # that iteration continues and that nothing propagates.
        schema = Schema(raw_schema=mock_raw_schema)
        port_0 = PortIdentity(0, internal=False)
        channel = self._register_input_port(main_loop, schema, port_0, "sender")
        main_loop.context.current_input_channel_id = channel

        element = DataElement(
            tag=channel,
            payload=DataFrame(
                frame=pyarrow.Table.from_pandas(
                    pandas.DataFrame(
                        [
                            {"test-1": "a", "test-2": 0},
                            {"test-1": "b", "test-2": 1},
                        ]
                    )
                )
            ),
        )

        attempted = []

        def _boom(port_id, size):
            attempted.append(
                main_loop.context.tuple_processing_manager.current_input_tuple["test-2"]
            )
            raise RuntimeError("statistics backend unavailable")

        monkeypatch.setattr(
            main_loop.context.statistics_manager, "increase_input_statistics", _boom
        )

        # Because the coordinator report is deliberately not asserted (above),
        # the log line is the swallow's ONLY remaining trace -- so pin it.
        # `except Exception: pass` is a strictly worse regression than the
        # defect described above (a short result with no evidence anywhere
        # rather than a stack trace in the worker log) and is not a defensible
        # production change, so this is a gap rather than a bug to cement.
        # The proxy delegates every other level to the real logger so the
        # module's debug/info calls keep working.
        from core.runnables import main_loop as main_loop_module

        class _RecordingLogger:
            def __init__(self, delegate):
                self.exceptions = []
                self._delegate = delegate

            def exception(self, err):
                self.exceptions.append(err)

            def __getattr__(self, name):
                return getattr(self._delegate, name)

        recorder = _RecordingLogger(main_loop_module.logger)
        monkeypatch.setattr(main_loop_module, "logger", recorder)

        # Must not raise.
        main_loop._process_data_element(element)

        assert attempted == [0, 1], (
            "a failure on one element must not abandon the rest of the batch; "
            f"attempted: {attempted}"
        )
        assert [type(err) for err in recorder.exceptions] == [
            RuntimeError,
            RuntimeError,
        ], (
            "every swallowed per-element failure must leave a log trace; "
            f"logged: {recorder.exceptions}"
        )
        assert all(
            "statistics backend unavailable" in str(err) for err in recorder.exceptions
        ), f"the logged trace must carry the real error; logged: {recorder.exceptions}"
