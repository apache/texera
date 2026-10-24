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

import pickle

import pytest
from pytexera.workflow.codec import WorkflowEnvelope, decode_boundary, encode_boundary
from pytexera.workflow.runtime import Heap, InputPort, Runtime


def test_repeated_execution_does_not_reuse_source_global_bindings() -> None:
    """One registered driver serves independent execution keys without stale names."""

    runtime = Runtime(input_ports=(InputPort(("in",)),), outgoing=("out",))
    namespace = {"runtime": runtime}
    source = (
        "@runtime.driver\n"
        "def driver(heap):\n"
        "    global value\n"
        "    runtime.import_boundary(heap, 'in', ('present', 'seed'))\n"
        "    if heap.present:\n"
        "        value = heap.seed\n"
        "    try:\n"
        "        heap.result = value\n"
        "    except NameError as error:\n"
        "        heap.result = (type(error).__name__, error.name)\n"
        "    runtime.export_boundary(heap, 'out', ('result',))\n"
        "    return heap\n"
    )
    exec(compile(source, "<activation-test>", "exec", dont_inherit=True), namespace)
    results = tuple(
        runtime.execute(
            WorkflowEnvelope(
                key,
                (encode_boundary("in", ("present", "seed"), (present, seed)),),
            )
        )
        for key, present, seed in (("first", True, 41), ("second", False, 0))
    )

    assert decode_boundary(results[0].boundaries[0], ("result",)) == (41,)
    assert decode_boundary(results[1].boundaries[0], ("result",)) == (
        ("NameError", "value"),
    )
    assert "value" not in namespace


def test_runtime_imports_and_exports_only_declared_fields() -> None:
    runtime = Runtime(input_ports=(InputPort(("input",)),), outgoing=("output",))

    @runtime.driver
    def driver(heap: Heap) -> Heap:
        runtime.import_boundary(heap, "input", ("value",))
        heap.result = heap.value + 1
        heap.unselected = "must not cross"
        runtime.export_boundary(heap, "output", ("result",))
        return heap

    inbound = WorkflowEnvelope(
        "run",
        (encode_boundary("input", ("value",), (41,)),),
    )

    outbound = runtime.execute(inbound)

    assert tuple(row.boundary_id for row in outbound.boundaries) == ("output",)
    assert decode_boundary(outbound.boundaries[0], ("result",)) == (42,)


def test_heap_exposes_ordinary_fields_as_a_python_namespace() -> None:
    heap = Heap(WorkflowEnvelope("run", ()))
    heap.summary = 41

    assert heap.summary == 41
    heap.summary = 42
    assert heap.summary == 42
    del heap.summary
    with pytest.raises(NameError, match="summary"):
        _ = heap.summary


def test_missing_heap_binding_preserves_name_error_metadata() -> None:
    """A transported absence retains Python's observable exception name."""

    heap = Heap(WorkflowEnvelope("run", ()))
    with pytest.raises(NameError) as read_error:
        _ = heap.missing
    with pytest.raises(NameError) as delete_error:
        del heap.missing
    assert read_error.value.name == "missing"
    assert delete_error.value.name == "missing"


def test_source_builtin_fallback_is_not_a_present_or_exported_value() -> None:
    """Lookup, storage presence and boundary serialization are different operations."""

    runtime = Runtime(outgoing=("edge",))
    heap = Heap(WorkflowEnvelope("run", ()))
    assert heap.len is len
    assert heap["__import__"] is __import__
    heap.len = None
    assert heap.len is None
    del heap.len
    assert heap.len([1]) == 1
    with pytest.raises(NameError, match="len"):
        del heap.len

    runtime.export_boundary(heap, "edge", ("__import__", "len"))

    assert heap._outgoing["edge"].present == ()
    assert decode_boundary(heap._outgoing["edge"], ("__import__", "len")) == ()
    assert heap._values == {}


def test_runtime_round_trips_only_fields_present_on_this_path() -> None:
    """An absent selected field stays absent instead of becoming a failure."""

    producer = Runtime(outgoing=("edge",))

    @producer.driver
    def produce(heap: Heap) -> Heap:
        heap.left = 41
        producer.export_boundary(heap, "edge", ("left", "right"))
        return heap

    exported = producer.execute(WorkflowEnvelope("run", ()))
    boundary = exported.boundaries[0]
    assert boundary.present == ("left",)

    consumer = Runtime(input_ports=(InputPort(("edge",)),))

    @consumer.driver
    def consume(heap: Heap) -> Heap:
        consumer.import_boundary(heap, "edge", ("left", "right"))
        heap.result = heap.left + 1
        with pytest.raises(NameError, match="right"):
            _ = heap.right
        return heap

    consumed = consumer.execute(exported)

    assert consumed.boundaries == ()


def test_runtime_rejects_two_boundaries_claiming_the_same_field() -> None:
    """Fan-in field ownership is explicit and independent of import order."""

    runtime = Runtime(
        input_ports=(InputPort(("left",)), InputPort(("right",))),
    )

    @runtime.driver
    def driver(heap: Heap) -> Heap:
        runtime.import_boundary(heap, "left", ("value",))
        runtime.import_boundary(heap, "right", ("value",))
        return heap

    envelope = WorkflowEnvelope(
        "run",
        (
            encode_boundary("left", ("value",), (1,)),
            encode_boundary("right", ("value",), (2,)),
        ),
    )

    with pytest.raises(RuntimeError, match="already owned"):
        runtime.execute(envelope)


def test_failed_multi_field_claim_does_not_reserve_earlier_fields() -> None:
    """A conflicting fan-in claim validates every field before committing any."""

    runtime = Runtime(input_ports=(InputPort(("left", "right", "third")),))
    heap = Heap(
        WorkflowEnvelope(
            "run",
            (
                encode_boundary("left", ("x",), (1,)),
                encode_boundary("right", ("x", "y"), (2, 3)),
                encode_boundary("third", ("y",), (4,)),
            ),
        )
    )
    runtime.import_boundary(heap, "left", ("x",))

    with pytest.raises(RuntimeError, match="already owned"):
        runtime.import_boundary(heap, "right", ("x", "y"))

    runtime.import_boundary(heap, "third", ("y",))
    assert heap.y == 4


def test_failed_decode_does_not_claim_boundary_fields() -> None:
    """Invalid bytes cannot mutate fan-in ownership before decode succeeds."""

    malformed = encode_boundary("bad", ("x",), (1,))
    object.__setattr__(malformed, "payload", b"not-a-cloudpickle-payload")
    recovery = encode_boundary("recovery", ("x",), (2,))
    heap = Heap(WorkflowEnvelope("run", (malformed, recovery)))
    runtime = Runtime(input_ports=(InputPort(("bad", "recovery")),))

    with pytest.raises(pickle.UnpicklingError):
        runtime.import_boundary(heap, "bad", ("x",))

    runtime.import_boundary(heap, "recovery", ("x",))
    assert heap.x == 2


def test_runtime_fails_closed_on_missing_export() -> None:
    runtime = Runtime(outgoing=("required",))

    @runtime.driver
    def driver(heap: Heap) -> Heap:
        return heap

    with pytest.raises(RuntimeError, match="outgoing"):
        runtime.execute(WorkflowEnvelope("run", ()))


def test_heap_rejects_missing_source_binding() -> None:
    heap = Heap(WorkflowEnvelope("run", ()))

    with pytest.raises(NameError, match="missing"):
        _ = heap.missing


def test_heap_keyed_access_is_reserved_only_for_private_source_names() -> None:
    heap = Heap(WorkflowEnvelope("run", ()))
    heap["_value"] = 1

    assert heap["_value"] == 1
    del heap["_value"]

    with pytest.raises(NameError, match="_value"):
        _ = heap["_value"]


def test_runtime_rejects_boundary_ownership_by_multiple_input_ports() -> None:
    with pytest.raises(ValueError, match="exactly one input port"):
        Runtime(
            input_ports=(InputPort(("shared",)), InputPort(("shared",))),
        )


@pytest.mark.parametrize(
    "ids", [(), ("",), (1,), (True,), (b"x",), (None,), ("b", "a"), ("a", "a")]
)
def test_invalid_input_boundary_ids(ids):
    with pytest.raises(ValueError):
        InputPort(ids)


@pytest.mark.parametrize("ports", [None, [], ("edge",)])
def test_invalid_runtime_port_contract(ports):
    with pytest.raises(TypeError, match="InputPort"):
        Runtime(input_ports=ports)


def test_driver_registration_and_execution_fail_closed():
    envelope = WorkflowEnvelope("run", ())
    with pytest.raises(RuntimeError, match="no driver"):
        Runtime().execute(envelope)
    runtime = Runtime()
    runtime.driver(lambda heap: None)
    with pytest.raises(RuntimeError, match="already has"):
        runtime.driver(lambda heap: heap)
    with pytest.raises(RuntimeError, match="return its input"):
        runtime.execute(envelope)
    invalid = Runtime()
    invalid.driver(42)
    with pytest.raises(TypeError, match="Python function"):
        invalid.execute(envelope)


def test_missing_and_undeclared_boundaries_do_not_execute():
    runtime = Runtime(input_ports=(InputPort(("in",)),), outgoing=("out",))
    calls = []
    runtime.driver(lambda heap: calls.append(heap))
    envelope = WorkflowEnvelope("run", ())
    heap = Heap(envelope)
    with pytest.raises(RuntimeError, match="missing"):
        runtime.execute(envelope)
    assert calls == []
    with pytest.raises(RuntimeError, match="missing"):
        runtime.import_boundary(heap, "in", ())
    with pytest.raises(ValueError, match="not incoming"):
        runtime.import_boundary(heap, "other", ())
    with pytest.raises(ValueError, match="not outgoing"):
        runtime.export_boundary(heap, "other", ())
    runtime.export_boundary(heap, "out", ())
    with pytest.raises(RuntimeError, match="twice"):
        runtime.export_boundary(heap, "out", ())
    with pytest.raises(TypeError, match="string"):
        heap[1] = 42
