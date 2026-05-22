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
        self.state = {"loop_counter": 0}
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
        loop_counter = int(state.get("loop_counter", 0))
        if loop_counter > 0:
            state["loop_counter"] = loop_counter - 1
            return state
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
        # loop_counter is left at its open() value (0) on first entry.
        assert op.state["loop_counter"] == 0

    def test_reentry_state_with_loop_start_uri_increments_loop_counter(self):
        # Re-entry from this LoopStart's own LoopEnd: the state carries
        # LoopStartStateURI, so the base class must INCREMENT
        # loop_counter and PASS THROUGH the state downstream. This is
        # what main_loop's _attach_loop_start_id relies on.
        op = _StubLoopStart()
        op.open()
        incoming = State({"LoopStartStateURI": "vfs:///x", "loop_counter": 0, "i": 2})

        result = op.process_state(incoming, port=0)

        assert result is not None, "re-entry state must be returned for downstream"
        assert result["loop_counter"] == 1
        # The user variable rides along.
        assert result["i"] == 2

    def test_reentry_at_nested_loop_counter_bumps_one(self):
        # Nested loop: an outer loop's re-entry state passes through this
        # inner LoopStart with a loop_counter already > 0 (because the
        # outer LoopStart bumped it on its own re-entry first). The
        # invariant is that we only ever +1, never reset.
        op = _StubLoopStart()
        op.open()
        incoming = State({"LoopStartStateURI": "vfs:///outer", "loop_counter": 5})

        result = op.process_state(incoming, port=0)

        assert result["loop_counter"] == 6


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
        assert produced["loop_counter"] == 0


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
# Nested loops — LoopEnd pass-through branch
# ---------------------------------------------------------------------------


class TestLoopEndNestedPassThrough:
    def test_loop_counter_positive_decrements_and_passes_state_through(self):
        # When the inner LoopEnd receives state with loop_counter > 0,
        # the state belongs to an OUTER loop. The inner LoopEnd must
        # decrement loop_counter and return the state for downstream
        # routing (which eventually reaches the outer LoopEnd at
        # loop_counter == 0).
        op = _StubLoopEnd(update="i += 1")
        op.state = {"sentinel": "must_not_be_overwritten"}

        incoming = State({"loop_counter": 2, "outer_var": "v"})
        result = op.process_state(incoming, port=0)

        assert result is not None, "pass-through branch must emit state downstream"
        assert result["loop_counter"] == 1
        assert result["outer_var"] == "v"
        # The pass-through branch must NOT overwrite self.state — the
        # inner LoopEnd's own matching-loop state from a previous inner
        # iteration must be preserved.
        assert op.state == {"sentinel": "must_not_be_overwritten"}

    def test_pass_through_chain_collapses_to_matching_branch_at_zero(self):
        # Walk a state with loop_counter=3 through three levels of
        # nested LoopEnds: each strips one level, and the fourth
        # (loop_counter == 0) is the matching loop that runs the
        # user's update. This pins the depth-symmetric invariant
        # nested-for-loop scheduling depends on.
        from pickle import dumps

        outer = _StubLoopEnd(update="i += 10")
        middle = _StubLoopEnd(update="i += 100")
        inner = _StubLoopEnd(update="i += 1000")
        match = _StubLoopEnd(update="i += 1")

        state = State(
            {
                "loop_counter": 3,
                "i": 0,
                "table": dumps(Table([Tuple({"v": 1})])),
            }
        )

        # Each outer→inner hop decrements once.
        state = outer.process_state(state, port=0)
        assert state["loop_counter"] == 2
        state = middle.process_state(state, port=0)
        assert state["loop_counter"] == 1
        state = inner.process_state(state, port=0)
        assert state["loop_counter"] == 0
        # At loop_counter == 0 the matching LoopEnd consumes the state
        # and runs ITS user update — not any of the pass-through ops'.
        result = match.process_state(state, port=0)
        assert result is None
        assert match.state["i"] == 1, "only the matching LoopEnd's update should fire"


# ---------------------------------------------------------------------------
# Nested loops — round trip
# ---------------------------------------------------------------------------


class TestNestedLoopRoundTrip:
    def test_outer_then_inner_loop_state_keeps_counters_symmetric(self):
        # Simulate the state flow for one outer iteration that itself
        # triggers one inner iteration:
        #
        #   outer LoopStart re-entry  → loop_counter 0 → 1
        #   inner LoopStart re-entry  → loop_counter 1 → 2
        #   inner LoopEnd             → loop_counter 2 → 1
        #   outer LoopEnd             → loop_counter 1 → 0 (matching branch)
        #
        # The matching branch on the outer LoopEnd is reached iff every
        # increment is mirrored by exactly one decrement. A bug in
        # either side would land us in the wrong branch.
        outer_start = _StubLoopStart()
        inner_start = _StubLoopStart()
        inner_end = _StubLoopEnd(update="outer_i += 100")
        outer_end = _StubLoopEnd(update="outer_i += 1")
        outer_start.open()
        inner_start.open()

        from pickle import dumps

        # outer LoopEnd jumped back to outer LoopStart with this state.
        state_in = State(
            {
                "LoopStartStateURI": "vfs:///outer",
                "loop_counter": 0,
                "outer_i": 0,
                "table": dumps(Table([Tuple({"v": 1})])),
            }
        )

        # outer LoopStart re-entry: +1
        state_after_outer_start = outer_start.process_state(state_in, port=0)
        assert state_after_outer_start["loop_counter"] == 1
        # inner LoopStart sees the same passing state and +1 again.
        state_after_inner_start = inner_start.process_state(
            state_after_outer_start, port=0
        )
        assert state_after_inner_start["loop_counter"] == 2
        # inner LoopEnd: pass-through branch (-1).
        state_after_inner_end = inner_end.process_state(state_after_inner_start, port=0)
        assert state_after_inner_end is not None
        assert state_after_inner_end["loop_counter"] == 1
        # outer LoopEnd: pass-through (-1) lands at 0, the matching branch.
        # Now process_state would have to run the matching branch path
        # because loop_counter == 0. To get there we need one more hop:
        result = outer_end.process_state(state_after_inner_end, port=0)
        # NOT yet at 0 — pass-through decrements to 0 and returns. The
        # NEXT hop is the matching branch.
        assert result is not None
        assert result["loop_counter"] == 0

        # Final landing on the matching branch consumes the state and
        # runs the outer update (outer_i += 1).
        matching = _StubLoopEnd(update="outer_i += 1")
        assert matching.process_state(result, port=0) is None
        assert matching.state["outer_i"] == 1
