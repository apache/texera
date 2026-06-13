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

"""Unit tests for the loop runtime: LoopStartOperator and LoopEndOperator.

These exercise the abstract base classes in operator.py that the
generated `ProcessLoopStartOperator` / `ProcessLoopEndOperator` classes
extend. The tests use minimal stub subclasses that mirror what
`LoopStartOpDesc.generatePythonCode` / `LoopEndOpDesc.generatePythonCode`
emit so the behavior covered here is the same shape that ships at
runtime.

Coverage:
  - LoopStart's first-entry state merge into self.state.
  - LoopEnd's process_table identity yield; condition is abstract.
  - The guarded exec helpers (eval_output / run_update / eval_condition)
    keep the reserved `table` / `output` names out of the persistent loop
    state, so user code cannot silently clobber loop machinery.
  - A multi-iteration loop driven to completion through the operators and the
    State to_tuple/from_tuple round-trip (TestLoopRunsToCompletion).

loop_counter and the LoopStart jump metadata (LoopStartId / LoopStartStateURI)
are owned by the worker runtime, not these operators -- they ride the
StateFrame envelope as their own columns -- so their handling is covered in
test_main_loop.py::TestLoopCounterRuntime.
"""

from typing import Iterator, Optional

import pyarrow as pa
import pytest

from core.models import State, Table, TableLike, Tuple
from core.models.operator import (
    _RESERVED_STATE_KEYS,
    LoopEndOperator,
    LoopStartOperator,
)
from core.models.table import table_from_ipc_bytes, table_to_ipc_bytes


# ---------------------------------------------------------------------------
# Stub subclasses that mirror the generated Python in
# LoopStart/LoopEnd OpDesc. Keeping them here (rather than reusing the
# real generator) lets the test pin behavior without spinning up a Scala
# runtime to produce code.
# ---------------------------------------------------------------------------


class _StubLoopStart(LoopStartOperator):
    """Mirrors `ProcessLoopStartOperator` from LoopStartOpDesc codegen.

    open() runs the user's `initialization` to seed self.state with the loop
    variables. process_table runs the user's `output` expression (via the
    guarded eval_output helper) and yields the result for downstream.
    """

    def __init__(self, initialization="i = 0", output_expr="table.iloc[i]"):
        super().__init__()
        self._initialization = initialization
        self._output_expr = output_expr

    def open(self) -> None:
        self.state = {}
        exec(self._initialization, {}, self.state)

    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        yield self.eval_output(self._output_expr, table)


class _StubLoopEnd(LoopEndOperator):
    """Mirrors `ProcessLoopEndOperator` from LoopEndOpDesc codegen.

    Consume-only: the runtime owns loop_counter and the nested pass-through, so
    the operator only runs the matching-loop path. run_update / eval_condition
    run the user's `update` / `condition` in a guarded namespace (user vars +
    table) so `table`/`output` never persist in or get clobbered out of
    self.state.
    """

    def __init__(self, update="i += 1", condition_expr="i < 3"):
        # No self.state seeding here: the real generated ProcessLoopEndOperator
        # has no __init__/open, so it relies entirely on LoopEndOperator's base
        # __init__. Mirroring that lets the tests exercise the base init.
        super().__init__()
        self._update = update
        self._condition_expr = condition_expr

    def process_state(self, state: State, port: int) -> Optional[State]:
        self.run_update(self._update, state)
        return None

    def condition(self) -> bool:
        return self.eval_condition(self._condition_expr)


# ---------------------------------------------------------------------------
# LoopStartOperator — process_state
# ---------------------------------------------------------------------------


class TestLoopStartProcessState:
    def test_first_time_state_is_merged_into_self_state_and_none_is_returned(self):
        # First entry: state from upstream (no LoopStartStateURI). The
        # base class must merge it into self.state and return None so
        # nothing flows downstream of LoopStart until the table is in.
        op = _StubLoopStart()
        op.open()
        op.state["i"] = 0  # simulate the user's initialization

        result = op.process_state(State({"upstream_key": "v"}), port=0)

        assert result is None, "first-time state must not be forwarded"
        assert op.state["upstream_key"] == "v", "state was not merged into self.state"

    # NOTE: LoopStart re-entry (+1) is owned by the worker runtime now, not the
    # operator (which only does the first-entry merge above). It and the nested
    # pass-through are covered in test_main_loop.py::TestLoopCounterRuntime.


# ---------------------------------------------------------------------------
# LoopStartOperator — produce_state_on_finish
# ---------------------------------------------------------------------------


class TestBufferedTableAccessor:
    """`TableOperator._buffered_table(port)` replaces the name-mangled
    `self._TableOperator__table_data[port]` read, so a rename of the parent
    class doesn't silently break LoopStart's table access."""

    def test_returns_buffered_tuples_as_table(self):
        op = _StubLoopStart()
        op.open()
        list(op.process_tuple(Tuple({"v": 1}), port=0))
        list(op.process_tuple(Tuple({"v": 2}), port=0))

        table = op._buffered_table(port=0)

        assert isinstance(table, Table)
        assert list(table.as_tuples()) == [Tuple({"v": 1}), Tuple({"v": 2})]

    def test_buffers_are_keyed_by_port(self):
        op = _StubLoopStart()
        op.open()
        list(op.process_tuple(Tuple({"v": 1}), port=0))
        list(op.process_tuple(Tuple({"v": 99}), port=1))

        assert list(op._buffered_table(port=0).as_tuples()) == [Tuple({"v": 1})]
        assert list(op._buffered_table(port=1).as_tuples()) == [Tuple({"v": 99})]


class TestLoopStartProduceStateOnFinish:
    def test_serializes_buffered_table_as_arrow_into_state_table_field(self):
        # produce_state_on_finish serializes the buffered table as an Apache
        # Arrow IPC stream (NOT pickle -- the receiving LoopEnd would otherwise
        # have to pickle.loads data from iceberg, a remote-code-execution
        # surface). The bytes must round-trip back to the same tuples and parse
        # as a real Arrow stream.
        op = _StubLoopStart()
        op.open()
        # Drive a couple of tuples through to populate the per-port buffer.
        list(op.process_tuple(Tuple({"v": 1}), port=0))
        list(op.process_tuple(Tuple({"v": 2}), port=0))

        produced = op.produce_state_on_finish(port=0)

        assert isinstance(produced, dict)
        assert "table" in produced
        assert isinstance(produced["table"], bytes), "table must be serialized bytes"
        # The bytes are an Arrow IPC stream (stronger than a no-pickle-prefix
        # check): if a future change swaps the encoder back to pickle, the
        # Arrow reader raises here.
        with pa.ipc.open_stream(pa.py_buffer(produced["table"])) as reader:
            reader.read_all()
        # Round-trip through the public helper must give back our two tuples.
        decoded = table_from_ipc_bytes(produced["table"])
        assert isinstance(decoded, Table)
        assert list(decoded.as_tuples()) == [Tuple({"v": 1}), Tuple({"v": 2})]

    def test_user_state_fields_survive_into_produced_state(self):
        # Any vars the user set in open() (e.g. i, accumulators) must
        # ride along in the produced state so LoopEnd can run the user's
        # `update` expression against them.
        op = _StubLoopStart(initialization="i = 0; acc = []")
        op.open()
        list(op.process_tuple(Tuple({"v": 1}), port=0))

        produced = op.produce_state_on_finish(port=0)

        assert produced["i"] == 0
        assert produced["acc"] == []
        # loop_counter is no longer seeded into the operator's state; it is
        # runtime-owned and rides on the StateFrame envelope.
        assert "loop_counter" not in produced


# ---------------------------------------------------------------------------
# LoopEndOperator — base class behaviour
# ---------------------------------------------------------------------------


class TestLoopEndBase:
    def test_process_table_yields_input_table_unchanged(self):
        # The base class finalizes process_table to a single identity
        # yield. The user only ever overrides condition() and (via
        # codegen) process_state.
        op = _StubLoopEnd()
        in_table = Table([Tuple({"x": 1}), Tuple({"x": 2})])
        out = list(op.process_table(in_table, port=0))
        assert out == [in_table]

    def test_condition_is_abstract_on_base_class(self):
        # A class that extends LoopEndOperator without supplying
        # condition() must be uninstantiable. This is what stops a
        # user from shipping a loop with an empty exit condition.
        class _Missing(LoopEndOperator):
            pass

        # Match on "abstract" rather than the method name "condition":
        # CPython's "missing abstract method" message wording has changed
        # between releases, but it has always contained the word
        # "abstract".
        with pytest.raises(TypeError, match="abstract"):
            _Missing()

    def test_condition_returns_false_before_any_state_is_consumed(self):
        # MainLoop.complete() calls condition() on every LoopEnd. One that
        # never consumed a matching state (run_update never ran) -- e.g. an
        # inner LoopEnd that only forwarded outer-loop pass-through state --
        # must return False (don't fire the back-edge) rather than raise
        # AttributeError on self._loop_table / self.state, or NameError when
        # the user's condition references undefined loop variables.
        op = _StubLoopEnd(condition_expr="i < len(table)")
        assert op.condition() is False

    def test_consumed_flag_flips_after_run_update(self):
        # Before any consume the loop hasn't run here; after run_update the
        # real condition is evaluated against the consumed state.
        op = _StubLoopEnd(update="i += 1", condition_expr="i < 3")
        assert op._consumed_state is False
        op.process_state(
            State({"i": 0, "table": table_to_ipc_bytes(Table([Tuple({"v": 1})]))}),
            port=0,
        )
        assert op._consumed_state is True
        assert op.condition() is True  # i became 1, 1 < 3


# ---------------------------------------------------------------------------
# Generated-style LoopEnd — single-loop matching branch
# ---------------------------------------------------------------------------


class TestLoopEndMatchingBranch:
    def test_loop_counter_zero_runs_user_update_and_returns_none(self):
        # The matching-loop branch (loop_counter == 0) is where the user's
        # update expression runs. process_state must return None so no
        # state flows downstream; the actual loop-back is driven by
        # main_loop.complete() reading executor.state.
        op = _StubLoopEnd(update="i += 1")
        # Simulate LoopStart's produced state arriving here. The table rides as
        # Arrow IPC bytes (see produce_state_on_finish), not pickle.
        # The content carries only user data (i) and the per-iteration table
        # scratch. loop_counter / LoopStartId / LoopStartStateURI are
        # runtime-owned and ride the StateFrame envelope, never the content.
        incoming = State(
            {
                "i": 2,
                "table": table_to_ipc_bytes(Table([Tuple({"v": 1})])),
            }
        )

        result = op.process_state(incoming, port=0)

        assert result is None, "matching-loop branch must not emit state downstream"
        assert op.state["i"] == 3, "user's update did not run on the matching branch"
        # Only user variables persist in self.state; the decoded table is kept
        # off to the side (self._loop_table) for condition(), never in the state.
        assert "table" not in op.state
        assert isinstance(op._loop_table, Table)

    def test_condition_evaluates_user_expression_against_stashed_state(self):
        op = _StubLoopEnd(update="i += 1", condition_expr="i < 3")

        # Drive process_state once so self.state is populated. The table rides
        # as Arrow IPC bytes, not pickle.
        op.process_state(
            State(
                {
                    "i": 1,
                    "table": table_to_ipc_bytes(Table([Tuple({"v": 1})])),
                }
            ),
            port=0,
        )
        assert op.condition() is True  # i became 2, 2 < 3

        # Run another iteration to push i past the threshold.
        op.process_state(
            State(
                {
                    "i": 2,
                    "table": table_to_ipc_bytes(Table([Tuple({"v": 1})])),
                }
            ),
            port=0,
        )
        assert op.condition() is False  # i became 3, 3 < 3 is False


# ---------------------------------------------------------------------------
# Nested-loop counter behaviour -- LoopStart +1, LoopEnd -1, and the
# depth-symmetric invariant -- is now owned by the worker runtime (the
# operators no longer read or mutate loop_counter), so it is covered in
# test_main_loop.py::TestLoopCounterRuntime rather than here.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Loop runs to completion -- multi-iteration composition of the real operators
# (eval_output / run_update / eval_condition), the State to_tuple/from_tuple
# round-trip that the materialized state channel performs, and the back-edge
# hand-off of the user loop variables. This is the closest verifiable proxy
# for a live single-loop run; the full-engine scheduler / region-re-execution
# path is exercised by the integration CI job, not here.
# ---------------------------------------------------------------------------


class TestLoopRunsToCompletion:
    @staticmethod
    def _drive_single_loop(rows, init, output_expr, update, condition_expr):
        """Drive one LoopStart -> LoopEnd loop to completion.

        Mimics how the engine runs each iteration: the LoopStart region is
        re-executed (a fresh operator whose open() seeds the loop variables,
        the back-edge state overriding them, and the upstream table re-read),
        the produced state crosses the materialized channel (a State
        to_tuple/from_tuple round-trip), the LoopEnd runs the user update and
        evaluates the condition, and on continuation only the user loop
        variables are handed back over the back-edge.

        Returns (iteration_count, emitted_body_rows, final_loop_vars).
        """
        back_edge = None
        iterations = 0
        emitted = []
        while True:
            iterations += 1
            assert iterations <= 100, "loop failed to terminate"

            start = _StubLoopStart(initialization=init, output_expr=output_expr)
            start.open()
            if back_edge is not None:
                start.process_state(back_edge, port=0)
            for row in rows:
                list(start.process_tuple(row, port=0))
            emitted.extend(o for o in start.on_finish(port=0) if o is not None)
            produced = start.produce_state_on_finish(port=0)

            # Cross-region hand-off: serialize + deserialize like the
            # materialized state channel does.
            forwarded = State.from_tuple(State(produced).to_tuple())

            end = _StubLoopEnd(update=update, condition_expr=condition_expr)
            end.run_update(update, forwarded)
            if not end.condition():
                return iterations, emitted, dict(end.state)

            # Only the user loop variables cross the back-edge.
            back_edge = State(end.state)

    def test_single_loop_iterates_once_per_row_then_stops(self):
        rows = [Tuple({"v": 1}), Tuple({"v": 2}), Tuple({"v": 3})]
        iterations, emitted, final_vars = self._drive_single_loop(
            rows,
            init="i = 0",
            output_expr="table.iloc[i]",
            update="i += 1",
            condition_expr="i < len(table)",
        )
        assert iterations == 3
        assert len(emitted) == 3  # one loop-body row emitted per iteration
        assert final_vars["i"] == 3

    def test_accumulator_persists_and_reserved_names_never_leak(self):
        rows = [Tuple({"v": 10}), Tuple({"v": 20}), Tuple({"v": 30})]
        iterations, _, final_vars = self._drive_single_loop(
            rows,
            init="i = 0; total = 0",
            output_expr="table.iloc[i]",
            update="total += int(table.iloc[i]['v']); i += 1",
            condition_expr="i < len(table)",
        )
        assert iterations == 3
        assert final_vars["i"] == 3
        assert final_vars["total"] == 60  # 10 + 20 + 30 carried across iterations
        # `table`/`output` are runtime-reserved; they must never persist in the
        # loop state that crosses the back-edge.
        assert "table" not in final_vars
        assert "output" not in final_vars


class TestReservedStateKeysConstant:
    """The reviewer flagged that the reserved-name set was a string
    convention rather than encoded as code. The filtering in
    ``eval_output`` / ``run_update`` / ``produce_state_on_finish`` now
    reads against a single ``_RESERVED_STATE_KEYS`` constant; this test
    pins that the constant carries exactly the names documented on the
    loop-operator class docstrings."""

    def test_contains_table_and_output(self):
        assert "table" in _RESERVED_STATE_KEYS
        assert "output" in _RESERVED_STATE_KEYS

    def test_does_not_contain_envelope_only_names(self):
        # loop_counter / LoopStartId / LoopStartStateURI live on the
        # StateFrame envelope, never in user state -- so they shouldn't
        # appear in this filter list either.
        assert "loop_counter" not in _RESERVED_STATE_KEYS
        assert "LoopStartId" not in _RESERVED_STATE_KEYS
        assert "LoopStartStateURI" not in _RESERVED_STATE_KEYS

    def test_is_frozen(self):
        # Mutability would let a future caller silently expand the
        # reserved set; the constant is meant to be the single source of
        # truth, so freeze it.
        assert isinstance(_RESERVED_STATE_KEYS, frozenset)
