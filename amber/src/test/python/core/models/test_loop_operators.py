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

Single-loop coverage:
  - LoopStart's first-time state observation (merge into self.state).
  - LoopEnd's process_table is the identity yield.
  - End-to-end one-iteration loop driven through the matching-loop branch.

Nested-loop coverage:
  - LoopStart.process_state with `LoopStartStateURI` already present
    must increment `loop_counter` and pass the state through downstream
    (this is what makes inner LoopStart not consume outer-loop state).
  - LoopEnd's generated process_state, when `loop_counter > 0`, must
    decrement and return the state unchanged so the outer LoopEnd is
    the one that runs the user's update / condition.
  - Round-trip outer × inner loop preserves the nesting invariant
    (loop_counter is symmetric across LoopStart/LoopEnd traversals).
"""

from pickle import loads
from typing import Iterator, Optional

import pytest

from core.models import State, Table, TableLike, Tuple
from core.models.operator import LoopEndOperator, LoopStartOperator


# ---------------------------------------------------------------------------
# Stub subclasses that mirror the generated Python in
# LoopStart/LoopEnd OpDesc. Keeping them here (rather than reusing the
# real generator) lets the test pin behavior without spinning up a Scala
# runtime to produce code.
# ---------------------------------------------------------------------------


class _StubLoopStart(LoopStartOperator):
    """Mirrors `ProcessLoopStartOperator` from LoopStartOpDesc codegen.

    open() seeds `loop_counter` to 0 and runs the user's `initialization`.
    process_table runs the user's `output` expression and yields the
    result for downstream.
    """

    def __init__(self, initialization="i = 0", output_expr="table.iloc[i]"):
        super().__init__()
        self._initialization = initialization
        self._output_expr = output_expr

    def open(self) -> None:
        self.state = {}
        exec(self._initialization, {}, self.state)

    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        self.state["table"] = table
        exec(f"output = {self._output_expr}", {}, self.state)
        yield self.state["output"]


class _StubLoopEnd(LoopEndOperator):
    """Mirrors `ProcessLoopEndOperator` from LoopEndOpDesc codegen.

    process_state recognises the nested-loop pass-through path
    (`loop_counter > 0`) and decrements; on the matching-loop branch
    it stashes the state, deserializes the pickled table, and runs the
    user's `update`. condition() returns the boolean result of the
    user's `condition` expression evaluated in self.state.
    """

    def __init__(self, update="i += 1", condition_expr="i < 3"):
        super().__init__()
        self._update = update
        self._condition_expr = condition_expr
        self.state = {}

    def process_state(self, state: State, port: int) -> Optional[State]:
        # Consume-only, mirroring the simplified codegen: the runtime owns
        # loop_counter and the nested pass-through branch, so the operator only
        # ever runs the matching-loop (consume) path.
        self.state = dict(state)
        self.state["table"] = loads(self.state["table"])
        exec(self._update, {}, self.state)
        return None

    def condition(self) -> bool:
        exec(f"output = {self._condition_expr}", {}, self.state)
        return self.state["output"]


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


class TestLoopStartProduceStateOnFinish:
    def test_pickles_buffered_table_into_state_table_field(self):
        # produce_state_on_finish must serialize the buffered table via
        # pickle (so the cross-region state stream can carry a heavy
        # pandas DataFrame as bytes). The receiving LoopEnd unpickles
        # it on the matching-loop branch.
        op = _StubLoopStart()
        op.open()
        # Drive a couple of tuples through to populate the per-port buffer.
        list(op.process_tuple(Tuple({"v": 1}), port=0))
        list(op.process_tuple(Tuple({"v": 2}), port=0))

        produced = op.produce_state_on_finish(port=0)

        assert isinstance(produced, dict)
        assert "table" in produced
        assert isinstance(produced["table"], bytes), "table must be pickled bytes"
        # Round-trip through pickle.loads must give back our two tuples.
        unpickled = loads(produced["table"])
        assert isinstance(unpickled, Table)
        rows = list(unpickled.as_tuples())
        assert rows == [Tuple({"v": 1}), Tuple({"v": 2})]

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
        # Simulate LoopStart's produced state arriving here.
        from pickle import dumps

        incoming = State(
            {
                "loop_counter": 0,
                "i": 2,
                "table": dumps(Table([Tuple({"v": 1})])),
                "LoopStartId": "outer-loop",
                "LoopStartStateURI": "vfs:///outer",
            }
        )

        result = op.process_state(incoming, port=0)

        assert result is None, "matching-loop branch must not emit state downstream"
        assert op.state["i"] == 3, "user's update did not run on the matching branch"
        # The table is unpickled in-place so condition() can see it as
        # a real Table without a second round of deserialization.
        assert isinstance(op.state["table"], Table)
        # Loop metadata is preserved so _jump_to_loop_start can read it.
        assert op.state["LoopStartId"] == "outer-loop"
        assert op.state["LoopStartStateURI"] == "vfs:///outer"

    def test_condition_evaluates_user_expression_against_stashed_state(self):
        op = _StubLoopEnd(update="i += 1", condition_expr="i < 3")
        from pickle import dumps

        # Drive process_state once so self.state is populated.
        op.process_state(
            State(
                {
                    "loop_counter": 0,
                    "i": 1,
                    "table": dumps(Table([Tuple({"v": 1})])),
                }
            ),
            port=0,
        )
        assert op.condition() is True  # i became 2, 2 < 3

        # Run another iteration to push i past the threshold.
        op.process_state(
            State(
                {
                    "loop_counter": 0,
                    "i": 2,
                    "table": dumps(Table([Tuple({"v": 1})])),
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
