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
  - The guarded eval/exec helpers (eval_output / run_update / eval_condition)
    keep the reserved `table` name out of the persistent loop state, so user
    code cannot silently clobber loop machinery. The table crosses the loop
    boundary as Arrow IPC bytes (see table_to_ipc_bytes in core.models.table);
    a user loop variable named `table` is a raised collision, not a silent
    drop (TestReservedNameCollision).
  - A multi-iteration loop driven to completion through the operators and the
    State to_tuple/from_tuple round-trip (TestLoopRunsToCompletion).
  - The exact generated-code shape -- base64 + decode_python_template + exec,
    with quote/newline-bearing expressions (TestGeneratedCodeShape).
  - Loop expressions that use generator expressions / comprehensions / lambdas
    over loop variables resolve (eval/exec namespace passed as globals), and
    exec's injected `__builtins__` never leaks into the persisted state
    (TestLoopExpressionScoping).

loop_counter and the LoopStart jump metadata (LoopStartId) are owned by the
worker runtime, not these operators -- they ride the StateFrame envelope as
their own columns (the loop-back write address is setup config, not state) --
so their handling is covered in test_main_loop.py::TestMainLoop.
"""

import base64
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
        self.run_initialization(self._initialization)

    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        yield self.eval_output(self._output_expr, table)


class _StubLoopEnd(LoopEndOperator):
    """Mirrors `ProcessLoopEndOperator` from LoopEndOpDesc codegen.

    Consume-only: the runtime owns loop_counter and the nested pass-through, so
    the operator only runs the matching-loop path. run_update / eval_condition
    run the user's `update` / `condition` in a guarded namespace (user vars +
    table) so `table` never persists in or gets clobbered out of self.state.
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


def _ipc_one_row():
    """One-row table as Arrow IPC bytes (the loop `table` payload)."""
    return table_to_ipc_bytes(Table([Tuple({"v": 1})]))


class TestLoopStartProcessState:
    def test_first_time_state_is_merged_into_self_state_and_none_is_returned(self):
        # First entry: state from upstream (no LoopStartId stamped). The
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
    # pass-through are covered in test_main_loop.py::TestMainLoop.


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
        # Arrow IPC stream, not pickle (see table_to_ipc_bytes in
        # core.models.table for why). The bytes must round-trip back to the
        # same tuples and parse as a real Arrow stream.
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
        # _loop_table stays None until a matching state is consumed; that
        # None is what condition() short-circuits on.
        assert op._loop_table is None
        assert op.condition() is False


# ---------------------------------------------------------------------------
# Generated-style LoopEnd — single-loop matching branch
# ---------------------------------------------------------------------------


class TestLoopEndMatchingBranch:
    def test_matching_branch_runs_update_and_condition_reads_result(self):
        # The matching-loop branch (loop_counter == 0) is where the user's
        # update expression runs. process_state must return None so no
        # state flows downstream; the actual loop-back is driven by
        # main_loop.complete() reading executor.state.
        op = _StubLoopEnd(update="i += 1", condition_expr="i < 3")
        # Simulate LoopStart's produced state arriving here. The table rides as
        # Arrow IPC bytes (see produce_state_on_finish), not pickle.
        # The content carries only user data (i) and the per-iteration table
        # scratch. loop_counter / LoopStartId are runtime-owned and ride the
        # StateFrame envelope, never the content.
        incoming = State(
            {
                "i": 1,
                "table": _ipc_one_row(),
            }
        )

        result = op.process_state(incoming, port=0)

        assert result is None, "matching-loop branch must not emit state downstream"
        assert op.state["i"] == 2, "user's update did not run on the matching branch"
        # Only user variables persist in self.state; the decoded table is kept
        # off to the side (self._loop_table) for condition(), never in the state.
        assert "table" not in op.state
        assert isinstance(op._loop_table, Table)
        # condition() evaluates the user expression against the stashed state.
        assert op.condition() is True  # i became 2, 2 < 3

        # Run another iteration to push i past the threshold.
        op.process_state(
            State(
                {
                    "i": 2,
                    "table": _ipc_one_row(),
                }
            ),
            port=0,
        )
        assert op.condition() is False  # i became 3, 3 < 3 is False


# ---------------------------------------------------------------------------
# Nested-loop counter behaviour -- LoopStart +1, LoopEnd -1, and the
# depth-symmetric invariant -- is now owned by the worker runtime (the
# operators no longer read or mutate loop_counter), so it is covered in
# test_main_loop.py::TestMainLoop rather than here.
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
    def test_accumulator_persists_and_reserved_names_never_leak(self):
        # The one-iteration-per-row scenario itself is exercised end-to-end
        # (byte-identical expressions) by LoopIntegrationSpec; this test pins
        # the operator-level composition and the reserved-name filtering.
        #
        # Each pass of the while loop mimics one engine iteration: the
        # LoopStart region is re-executed (a fresh operator whose open() seeds
        # the loop variables, the back-edge state overriding them, and the
        # upstream table re-read), the produced state crosses the materialized
        # channel (a State to_tuple/from_tuple round-trip), the LoopEnd runs
        # the user update and evaluates the condition, and on continuation
        # only the user loop variables cross the back-edge.
        rows = [Tuple({"v": 10}), Tuple({"v": 20}), Tuple({"v": 30})]

        back_edge = None
        iterations = 0
        emitted = []
        while True:
            iterations += 1
            assert iterations <= 100, "loop failed to terminate"

            start = _StubLoopStart(
                initialization="i = 0; total = 0", output_expr="table.iloc[i]"
            )
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

            end = _StubLoopEnd(
                update="total += int(table.iloc[i]['v']); i += 1; output = total",
                condition_expr="i < len(table)",
            )
            end.process_state(forwarded, port=0)
            if not end.condition():
                break

            # Only the user loop variables cross the back-edge.
            back_edge = State(end.state)

        assert iterations == 3
        assert len(emitted) == 3  # one loop-body row emitted per iteration
        assert end.state["i"] == 3
        assert end.state["total"] == 60  # 10 + 20 + 30 carried across iterations
        # `table` is runtime-reserved; it must never persist in the loop state
        # that crosses the back-edge. `output` is an ordinary user variable
        # (loop expressions are eval'd directly), so it persists like any other.
        assert "table" not in end.state
        assert end.state["output"] == 60


class TestReservedStateKeysConstant:
    """The reserved-name handling -- the strip in ``run_update`` and the
    collision raises in ``run_update`` / ``produce_state_on_finish`` -- keys
    off the single ``_TABLE_KEY`` / ``_RESERVED_STATE_KEYS`` constants; pin
    the exact (frozen) contents."""

    def test_reserved_state_keys_is_exactly_frozen_table(self):
        # Envelope names (loop_counter / LoopStartId) are deliberately NOT
        # reserved -- they ride the StateFrame envelope and never touch user
        # state. Neither is "output": loop expressions are eval'd directly,
        # so it is an ordinary user variable.
        assert _RESERVED_STATE_KEYS == frozenset({"table"})
        assert isinstance(_RESERVED_STATE_KEYS, frozenset)


class TestReservedNameCollision:
    """A user loop variable named `table` collides with the runtime's input
    table. Both operators raise on the collision rather than silently dropping
    the user's value."""

    def test_loop_start_raises_when_user_variable_named_table(self):
        # `initialization` defines a variable named `table`, which would be
        # overwritten by the input table in produce_state_on_finish.
        op = _StubLoopStart(initialization="table = 1")
        op.open()
        with pytest.raises(ValueError, match="'table' is reserved by the loop runtime"):
            op.produce_state_on_finish(port=0)

    def test_loop_end_raises_when_update_rebinds_table(self):
        # `update` rebinds `table`, which run_update would otherwise strip.
        op = _StubLoopEnd(update="table = 1")
        incoming = State({"i": 1, "table": _ipc_one_row()})
        with pytest.raises(ValueError, match="'table' is reserved by the loop runtime"):
            op.process_state(incoming, port=0)

    def test_loop_end_raises_when_update_deletes_table(self):
        # `update` deletes `table` outright; run_update's identity check must
        # still flag the reserved-name collision (namespace.get) rather than
        # escape as a bare KeyError on the missing key.
        op = _StubLoopEnd(update="del table")
        incoming = State({"i": 1, "table": _ipc_one_row()})
        with pytest.raises(ValueError, match="'table' is reserved by the loop runtime"):
            op.process_state(incoming, port=0)


class TestLoopExpressionScoping:
    """User loop expressions may use generator expressions, comprehensions, or
    lambdas that reference loop variables. The eval/exec namespace is passed as
    globals (not a locals-only mapping) so those nested scopes resolve the
    loop variables -- a genexp / lambda body looks free names up in globals, so
    a locals-only namespace raises ``NameError`` on a perfectly valid
    expression. Regression guard for that scoping bug."""

    def test_eval_output_resolves_genexp_over_loop_vars(self):
        # `output` is a generator expression whose BODY references the loop
        # variable `bump` (a free variable inside the genexp scope, unlike the
        # leftmost iterable which resolves in the enclosing scope); it must
        # resolve rather than NameError.
        op = _StubLoopStart(initialization="bump = 10")
        op.open()
        result = op.eval_output(
            "sum(x + bump for x in [1, 2, 3])", Table([Tuple({"v": 1})])
        )
        assert result == 36

    def test_run_update_resolves_genexp_over_loop_vars(self):
        # `update` assigns from a genexp whose body references the loop
        # variable `base`.
        op = _StubLoopEnd(update="total = sum(v + base for v in [1, 2, 3])")
        incoming = State({"base": 10, "table": _ipc_one_row()})
        op.process_state(incoming, port=0)
        assert op.state["total"] == 36

    def test_run_update_resolves_lambda_capturing_loop_vars(self):
        # A lambda in `update` closes over the loop variable `offset`.
        op = _StubLoopEnd(update="ranked = sorted([3, 1, 2], key=lambda e: e - offset)")
        incoming = State({"offset": 0, "table": _ipc_one_row()})
        op.process_state(incoming, port=0)
        assert op.state["ranked"] == [1, 2, 3]

    def test_eval_condition_resolves_genexp_over_loop_vars(self):
        # `condition` is a genexp (`all(...)`) whose body references the loop
        # variable `floor`.
        op = _StubLoopEnd(
            update="i += 1", condition_expr="all(x > floor for x in [1, 2, 3])"
        )
        incoming = State({"i": 0, "floor": 0, "table": _ipc_one_row()})
        op.process_state(incoming, port=0)
        assert op.condition() is True

    def test_run_initialization_resolves_genexp_over_init_vars(self):
        # A genexp in `initialization` whose body references a variable defined
        # earlier in the same init block must resolve.
        op = _StubLoopStart(
            initialization="floor = 0\nok = all(v > floor for v in [1, 2, 3])"
        )
        op.open()
        assert op.state["ok"] is True
        assert op.state["floor"] == 0

    def test_initialized_state_has_no_builtins_leak(self):
        # exec with a globals namespace injects ``__builtins__``; it must not
        # leak into the persisted loop state (it is not JSON-serializable and
        # would break State materialization on the back-edge).
        op = _StubLoopStart(initialization="i = 0")
        op.open()
        assert "__builtins__" not in op.state
        list(op.process_tuple(Tuple({"v": 1}), port=0))
        produced = op.produce_state_on_finish(port=0)
        produced.to_tuple(0)  # must not raise

    def test_updated_state_has_no_builtins_leak(self):
        op = _StubLoopEnd(update="i += 1")
        incoming = State({"i": 0, "table": _ipc_one_row()})
        op.process_state(incoming, port=0)
        assert "__builtins__" not in op.state
        op.state.to_tuple(0)  # must not raise


# The stub subclasses above skip the base64 + `decode_python_template` layer
# that the real generated operators go through. These templates mirror
# LoopStart/LoopEndOpDesc.generatePythonCode exactly (user expressions arrive
# as `self.decode_python_template('<base64>')`); if those Scala templates
# change, update these -- the *OpDescSpec `code should include(...)` assertions
# pin the generated shape on the Scala side.
_LOOP_START_TEMPLATE = """from pytexera import *
class ProcessLoopStartOperator(LoopStartOperator):
    @overrides
    def open(self):
        self.run_initialization(self.decode_python_template('__INIT__'))

    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        yield self.eval_output(self.decode_python_template('__OUTPUT__'), table)
"""

_LOOP_END_TEMPLATE = """from pytexera import *
class ProcessLoopEndOperator(LoopEndOperator):
    @overrides
    def process_state(self, state: State, port: int) -> Optional[State]:
        self.run_update(self.decode_python_template('__UPDATE__'), state)
        return None

    @overrides
    def condition(self) -> bool:
        return self.eval_condition(self.decode_python_template('__CONDITION__'))
"""


def _b64(expr: str) -> str:
    # Mirror the `pyb` builder: base64 of the UTF-8 expression, which the
    # generated code hands to self.decode_python_template(...).
    return base64.b64encode(expr.encode("utf-8")).decode("ascii")


class TestGeneratedCodeShape:
    """Exec the *actual* generated-code shape (base64 +
    ``decode_python_template`` + exec/eval), which the plain stubs skip. Uses a
    quote and a newline in the user expressions -- a raw paste into the
    generated source would break on those -- so drift between the codegen and
    the runtime is caught here, without the slow @IntegrationTest job."""

    def test_generated_loop_start_execs_tricky_expressions(self):
        # initialization is exec'd (statements, so a newline is fine); output
        # is eval'd (a single expression, so use a quote, not a newline).
        init_expr = "i = 0\nnote = 'it\\'s fine'"
        output_expr = 'table.assign(msg="quote \' here")'
        source = _LOOP_START_TEMPLATE.replace("__INIT__", _b64(init_expr)).replace(
            "__OUTPUT__", _b64(output_expr)
        )
        # __name__ so the exec'd class's methods get a real __module__ (the
        # @overrides decorator on the generated methods inspects it).
        namespace: dict = {"__name__": "generated_loop_operator"}
        exec(source, namespace)

        op = namespace["ProcessLoopStartOperator"]()
        op.open()
        assert op.state["i"] == 0
        # The apostrophe survived base64 -> decode_python_template -> exec.
        assert op.state["note"] == "it's fine"

        (out,) = list(op.process_table(Table([Tuple({"a": 1})]), 0))
        assert list(out["msg"]) == ["quote ' here"]

    def test_generated_loop_end_execs_tricky_expressions(self):
        update_expr = "i += 1"
        # A double-quoted string containing an apostrophe in the condition
        # expression: it must survive the base64 round-trip intact.
        condition_expr = 'note == "it\'s" and i < 3'
        source = _LOOP_END_TEMPLATE.replace("__UPDATE__", _b64(update_expr)).replace(
            "__CONDITION__", _b64(condition_expr)
        )
        namespace: dict = {"__name__": "generated_loop_operator"}
        exec(source, namespace)

        op = namespace["ProcessLoopEndOperator"]()
        incoming = State({"i": 1, "note": "it's", "table": _ipc_one_row()})
        assert op.process_state(incoming, port=0) is None
        assert op.state["i"] == 2  # update ran
        assert op.condition() is True  # quoted condition round-tripped
