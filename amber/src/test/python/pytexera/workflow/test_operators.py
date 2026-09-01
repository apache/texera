# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest
from pytexera.workflow.codec import (
    WorkflowEnvelope,
    decode_boundary,
    dumps_envelope,
    encode_boundary,
    loads_envelope,
)
from pytexera.workflow.operators import ENVELOPE_FIELD, TupleOperator
from pytexera.workflow.runtime import Heap, InputPort, Runtime


def test_workflow_tuple_field_is_runtime_neutral() -> None:
    """The tuple field name is independent of a workflow generator."""

    assert ENVELOPE_FIELD == "workflow_envelope"


def test_tuple_operator_assembles_fan_in_by_execution_key() -> None:
    runtime = Runtime(
        input_ports=(InputPort(("left",)), InputPort(("right",))),
        outgoing=("result",),
    )

    @runtime.driver
    def driver(heap: Heap) -> Heap:
        runtime.import_boundary(heap, "left", ("left_value",))
        runtime.import_boundary(heap, "right", ("right_value",))
        heap.total = heap.left_value + heap.right_value
        runtime.export_boundary(heap, "result", ("total",))
        return heap

    class Operator(TupleOperator):
        pass

    Operator.runtime = runtime
    operator = Operator()
    left = WorkflowEnvelope(
        "run",
        (encode_boundary("left", ("left_value",), (20,)),),
    )
    right = WorkflowEnvelope(
        "run",
        (encode_boundary("right", ("right_value",), (22,)),),
    )

    assert list(operator.process_tuple({ENVELOPE_FIELD: dumps_envelope(left)}, 0)) == []
    outputs = list(operator.process_tuple({ENVELOPE_FIELD: dumps_envelope(right)}, 1))

    assert len(outputs) == 1
    envelope = loads_envelope(outputs[0][ENVELOPE_FIELD])
    assert decode_boundary(envelope.boundaries[0], ("total",)) == (42,)


def test_tuple_operator_allows_one_input_to_finish_before_another() -> None:
    runtime = Runtime(
        input_ports=(InputPort(("left",)), InputPort(("right",))),
        outgoing=("result",),
    )

    @runtime.driver
    def driver(heap: Heap) -> Heap:
        runtime.import_boundary(heap, "left", ("left_value",))
        runtime.import_boundary(heap, "right", ("right_value",))
        heap.total = heap.left_value + heap.right_value
        runtime.export_boundary(heap, "result", ("total",))
        return heap

    class Operator(TupleOperator):
        pass

    Operator.runtime = runtime
    operator = Operator()
    left = WorkflowEnvelope(
        "run",
        (encode_boundary("left", ("left_value",), (20,)),),
    )
    right = WorkflowEnvelope(
        "run",
        (encode_boundary("right", ("right_value",), (22,)),),
    )

    assert list(operator.process_tuple({ENVELOPE_FIELD: dumps_envelope(left)}, 0)) == []
    assert list(operator.on_finish(0)) == []
    outputs = list(operator.process_tuple({ENVELOPE_FIELD: dumps_envelope(right)}, 1))
    assert len(outputs) == 1
    assert list(operator.on_finish(1)) == []


def test_tuple_operator_fails_after_all_ports_finish_with_incomplete_key() -> None:
    runtime = Runtime(
        input_ports=(InputPort(("left",)), InputPort(("right",))),
    )

    @runtime.driver
    def driver(heap: Heap) -> Heap:
        return heap

    class Operator(TupleOperator):
        pass

    Operator.runtime = runtime
    operator = Operator()
    left = WorkflowEnvelope(
        "run",
        (encode_boundary("left", ("left_value",), (20,)),),
    )

    assert list(operator.process_tuple({ENVELOPE_FIELD: dumps_envelope(left)}, 0)) == []
    assert list(operator.on_finish(0)) == []
    with pytest.raises(RuntimeError, match="incomplete"):
        list(operator.on_finish(1))


def test_tuple_operator_rejects_boundaries_arriving_on_the_wrong_port() -> None:
    runtime = Runtime(
        input_ports=(InputPort(("left",)), InputPort(("right",))),
    )

    @runtime.driver
    def driver(heap: Heap) -> Heap:
        return heap

    class Operator(TupleOperator):
        pass

    Operator.runtime = runtime
    right = WorkflowEnvelope(
        "run",
        (encode_boundary("right", ("right_value",), (22,)),),
    )

    with pytest.raises(ValueError, match="port 0"):
        list(Operator().process_tuple({ENVELOPE_FIELD: dumps_envelope(right)}, 0))


def test_entry_tuple_operator_executes_without_incoming_boundaries() -> None:
    runtime = Runtime(outgoing=("result",))

    @runtime.driver
    def driver(heap: Heap) -> Heap:
        heap.value = 7
        runtime.export_boundary(heap, "result", ("value",))
        return heap

    class Operator(TupleOperator):
        pass

    Operator.runtime = runtime
    output = list(Operator().process_tuple({"execution_key": "run"}, 0))

    assert len(output) == 1
    envelope = loads_envelope(output[0][ENVELOPE_FIELD])
    assert envelope.execution_key == "run"
