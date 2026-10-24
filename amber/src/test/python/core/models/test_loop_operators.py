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
  - LoopStart's first-entry state merge into self.variables.
  - LoopEnd's process_table identity yield; condition is abstract.
  - The guarded eval/exec helpers (eval_output / run_update / eval_condition)
    keep the reserved `table` name out of the persistent loop state, so user
    code cannot silently clobber loop machinery. The table never rides the
    State content: the runtime reads it from the Loop Start's input-port
    materialization and injects it via attach_loop_table; a user loop variable
    named `table` is a raised collision, not a silent drop
    (TestReservedNameCollision).
  - A multi-iteration loop driven to completion through the operators and the
    State to_tuple/from_tuple round-trip (TestLoopRunsToCompletion).
  - The exact generated-code shape -- base64 + decode_python_template + exec,
    with quote/newline-bearing expressions (TestGeneratedCodeShape).
  - Loop expressions that use generator expressions / comprehensions / lambdas
    over loop variables resolve (eval/exec namespace passed as globals), and
    exec's injected `__builtins__` never leaks into the persisted state
    (TestLoopExpressionScoping).
  - LoopStart's Output is a cell: what it assigns becomes a control variable of
    the iteration, a trailing expression is the data, and it runs once, before
    the state is produced (TestLoopStartOutputCell).

loop_counter and the LoopStart jump metadata (LoopStartId) are owned by the
worker runtime, not these operators -- they ride the StateFrame envelope as
their own columns (the loop-back write address is setup config, not state) --
so their handling is covered in test_main_loop.py::TestMainLoop.
"""

import base64
from typing import Iterator, Optional

import pytest

from core.models import Schema, State, Table, TableLike, Tuple
from core.models.operator import (
    _RESERVED_STATE_KEYS,
    LoopEndOperator,
    LoopStartOperator,
)


# ---------------------------------------------------------------------------
# Stub subclasses that mirror the generated Python in
# LoopStart/LoopEnd OpDesc. Keeping them here (rather than reusing the
# real generator) lets the test pin behavior without spinning up a Scala
# runtime to produce code.
# ---------------------------------------------------------------------------


class _StubLoopStart(LoopStartOperator):
    """Mirrors `ProcessLoopStartOperator` from LoopStartOpDesc codegen.

    open() hands over the user's `initialization` and `output` cell; both run
    once the input table has arrived, before the state is produced.
    process_table yields the data the `output` cell produced (via eval_output).
    """

    def __init__(self, initialization="i = 0", output_expr="table.iloc[i]"):
        super().__init__()
        self._initialization = initialization
        self._output_expr = output_expr

    def open(self) -> None:
        self.run_initialization(self._initialization)
        self.run_output(self._output_expr)

    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        yield self.eval_output(table)


class _StubLoopEnd(LoopEndOperator):
    """Mirrors `ProcessLoopEndOperator` from LoopEndOpDesc codegen.

    Consume-only: the runtime owns loop_counter and the nested pass-through, so
    the operator only runs the matching-loop path. run_update / eval_condition
    run the user's `update` / `condition` in a guarded namespace (user vars +
    table) so `table` never persists in or gets clobbered out of self.variables.
    """

    def __init__(self, update="i += 1", condition_expr="i < 3"):
        # No self.variables seeding here: the real generated ProcessLoopEndOperator
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


def _one_row_table() -> Table:
    """One-row Table, standing in for the loop's input table that the runtime
    reads from the Loop Start's input-port materialization and injects via
    ``attach_loop_table``."""
    return Table([Tuple({"v": 1})])


class TestLoopStartProcessState:
    def test_first_time_state_is_merged_into_self_variables_and_none_is_returned(self):
        # First entry: state from upstream (no LoopStartId stamped). The
        # base class must merge it into self.variables and return None so
        # nothing flows downstream of LoopStart until the table is in.
        op = _StubLoopStart()
        op.open()
        op.variables["i"] = 0  # simulate the user's initialization

        result = op.process_state(State({"upstream_key": "v"}), port=0)

        assert result is None, "first-time state must not be forwarded"
        assert op.variables["upstream_key"] == "v", (
            "state was not merged into self.variables"
        )

    # NOTE: LoopStart re-entry (+1) is owned by the worker runtime now, not the
    # operator (which only does the first-entry merge above). It and the nested
    # pass-through are covered in test_main_loop.py::TestMainLoop.


# ---------------------------------------------------------------------------
# LoopStartOperator — produce_state_on_finish
# ---------------------------------------------------------------------------


class TestLoopStartProduceStateOnFinish:
    def test_produced_state_carries_only_user_variables_never_the_table(self):
        # The input table does NOT ride the state: the LoopEnd runtime reads
        # it from the Loop Start's input-port materialization at consume time
        # (loopStartPortUris) and injects it via attach_loop_table. The
        # produced state must therefore stay small, pure-JSON user variables.
        op = _StubLoopStart()
        op.open()
        list(op.process_tuple(Tuple({"v": 1}), port=0))
        list(op.process_tuple(Tuple({"v": 2}), port=0))

        produced = op.produce_state_on_finish(port=0)

        assert isinstance(produced, dict)
        assert "table" not in produced
        produced.to_tuple(0)  # pure-JSON user vars: must serialize cleanly

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
        # runtime-owned and rides on the StateFrame envelope. The table is
        # injected at the LoopEnd by the runtime, never embedded here.
        assert "loop_counter" not in produced
        assert "table" not in produced


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
        # AttributeError on self._loop_table / self.variables, or NameError when
        # the user's condition references undefined loop variables.
        op = _StubLoopEnd(condition_expr="i < len(table)")
        # _loop_table stays None until a matching state is consumed; that
        # None is what condition() short-circuits on.
        assert op._loop_table is None
        assert op.condition() is False
        # Attaching the table alone (what the runtime does right before the
        # consume) must NOT mark the loop as consumed: only a successful
        # run_update does.
        op.attach_loop_table(_one_row_table())
        assert op._loop_table is None
        assert op.condition() is False

    def test_run_update_fails_loud_when_no_table_attached(self):
        # The runtime must attach the loop's input table before the matching
        # consume; a missing attach is a runtime wiring bug and must not
        # surface as a confusing NameError from the user's expression.
        op = _StubLoopEnd(update="i += 1")
        with pytest.raises(RuntimeError, match="not attached"):
            op.process_state(State({"i": 0}), port=0)


# ---------------------------------------------------------------------------
# Generated-style LoopEnd — single-loop matching branch
# ---------------------------------------------------------------------------


class TestLoopEndMatchingBranch:
    def test_matching_branch_runs_update_and_condition_reads_result(self):
        # The matching-loop branch (loop_counter == 0) is where the user's
        # update expression runs. process_state must return None so no
        # state flows downstream; the actual loop-back is driven by
        # main_loop.complete() reading executor.variables.
        op = _StubLoopEnd(update="i += 1", condition_expr="i < 3")
        # Simulate the runtime's consume: it reads the loop's input table from
        # the Loop Start's input-port materialization and attaches it, then
        # hands over LoopStart's produced state (pure user variables --
        # loop_counter / LoopStartId are runtime-owned and ride the StateFrame
        # envelope, never the content).
        op.attach_loop_table(_one_row_table())
        incoming = State({"i": 1})

        result = op.process_state(incoming, port=0)

        assert result is None, "matching-loop branch must not emit state downstream"
        assert op.variables["i"] == 2, (
            "user's update did not run on the matching branch"
        )
        # Only user variables persist in self.variables; the decoded table is kept
        # off to the side (self._loop_table) for condition(), never in the state.
        assert "table" not in op.variables
        assert isinstance(op._loop_table, Table)
        # condition() evaluates the user expression against the stashed state.
        assert op.condition() is True  # i became 2, 2 < 3

        # Run another iteration to push i past the threshold.
        op.attach_loop_table(_one_row_table())
        op.process_state(State({"i": 2}), port=0)
        assert op.condition() is False  # i became 3, 3 < 3 is False

        # Each update TAKES its attach: a further update without a fresh
        # attach must fail loud rather than silently reuse the previous
        # table (the clear in run_update is what this pins).
        with pytest.raises(RuntimeError, match="not attached"):
            op.process_state(State({"i": 3}), port=0)


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
            # The runtime reads the loop's input table from the Loop Start's
            # input-port materialization (the same rows the LoopStart
            # buffered) and attaches it before the consume.
            end.attach_loop_table(Table(rows))
            end.process_state(forwarded, port=0)
            if not end.condition():
                break

            # Only the user loop variables cross the back-edge.
            back_edge = State(end.variables)

        assert iterations == 3
        assert len(emitted) == 3  # one loop-body row emitted per iteration
        assert end.variables["i"] == 3
        assert end.variables["total"] == 60  # 10 + 20 + 30 carried across iterations
        # `table` is runtime-reserved; it must never persist in the loop state
        # that crosses the back-edge. `output` is an ordinary user variable
        # (loop expressions are eval'd directly), so it persists like any other.
        assert "table" not in end.variables
        assert end.variables["output"] == 60


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
        assert _RESERVED_STATE_KEYS == frozenset({"table", "D"})
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
        op.attach_loop_table(_one_row_table())
        incoming = State({"i": 1})
        with pytest.raises(ValueError, match="'table' is reserved by the loop runtime"):
            op.process_state(incoming, port=0)

    def test_loop_end_raises_when_update_deletes_table(self):
        # `update` deletes `table` outright; run_update's identity check must
        # still flag the reserved-name collision (namespace.get) rather than
        # escape as a bare KeyError on the missing key.
        op = _StubLoopEnd(update="del table")
        op.attach_loop_table(_one_row_table())
        incoming = State({"i": 1})
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
        op = _StubLoopStart(
            initialization="bump = 10", output_expr="sum(x + bump for x in [1, 2, 3])"
        )
        op.open()
        result = op.eval_output(Table([Tuple({"v": 1})]))
        assert result == 36

    def test_run_update_resolves_genexp_over_loop_vars(self):
        # `update` assigns from a genexp whose body references the loop
        # variable `base`.
        op = _StubLoopEnd(update="total = sum(v + base for v in [1, 2, 3])")
        op.attach_loop_table(_one_row_table())
        op.process_state(State({"base": 10}), port=0)
        assert op.variables["total"] == 36

    def test_run_update_resolves_lambda_capturing_loop_vars(self):
        # A lambda in `update` closes over the loop variable `offset`.
        op = _StubLoopEnd(update="ranked = sorted([3, 1, 2], key=lambda e: e - offset)")
        op.attach_loop_table(_one_row_table())
        op.process_state(State({"offset": 0}), port=0)
        assert op.variables["ranked"] == [1, 2, 3]

    def test_eval_condition_resolves_genexp_over_loop_vars(self):
        # `condition` is a genexp (`all(...)`) whose body references the loop
        # variable `floor`.
        op = _StubLoopEnd(
            update="i += 1", condition_expr="all(x > floor for x in [1, 2, 3])"
        )
        op.attach_loop_table(_one_row_table())
        op.process_state(State({"i": 0, "floor": 0}), port=0)
        assert op.condition() is True

    def test_run_initialization_resolves_genexp_over_init_vars(self):
        # A genexp in `initialization` whose body references a variable defined
        # earlier in the same init block must resolve.
        # Output "None": the output cell runs when the state is produced, and
        # this init defines no `i` for the default "table.iloc[i]".
        op = _StubLoopStart(
            initialization="floor = 0\nok = all(v > floor for v in [1, 2, 3])",
            output_expr="None",
        )
        op.open()
        produced = op.produce_state_on_finish(port=0)
        assert produced["ok"] is True
        assert produced["floor"] == 0

    def test_initialized_state_has_no_builtins_leak(self):
        # exec with a globals namespace injects ``__builtins__``; it must not
        # leak into the persisted loop state (it is not JSON-serializable and
        # would break State materialization on the back-edge).
        op = _StubLoopStart(initialization="i = 0")
        op.open()
        assert "__builtins__" not in op.variables
        list(op.process_tuple(Tuple({"v": 1}), port=0))
        produced = op.produce_state_on_finish(port=0)
        produced.to_tuple(0)  # must not raise

    def test_updated_state_has_no_builtins_leak(self):
        op = _StubLoopEnd(update="i += 1")
        op.attach_loop_table(_one_row_table())
        op.process_state(State({"i": 0}), port=0)
        assert "__builtins__" not in op.variables
        op.variables.to_tuple(0)  # must not raise


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
        self.run_output(self.decode_python_template('__OUTPUT__'))

    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        yield self.eval_output(table)
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
        # Both are cells of statements, so both carry a newline and a quote;
        # output ends in an expression, which is the data.
        init_expr = "i = 0\nnote = 'it\\'s fine'"
        output_expr = 'msg = "quote \' here"\ntable.assign(msg=msg)'
        source = _LOOP_START_TEMPLATE.replace("__INIT__", _b64(init_expr)).replace(
            "__OUTPUT__", _b64(output_expr)
        )
        # __name__ so the exec'd class's methods get a real __module__ (the
        # @overrides decorator on the generated methods inspects it).
        namespace: dict = {"__name__": "generated_loop_operator"}
        exec(source, namespace)

        op = namespace["ProcessLoopStartOperator"]()
        op.open()
        # init() runs once the input table has arrived.
        (out,) = list(op.process_table(Table([Tuple({"a": 1})]), 0))
        assert list(out["msg"]) == ["quote ' here"]
        assert op.variables["i"] == 0
        # The apostrophe survived base64 -> decode_python_template -> exec.
        assert op.variables["note"] == "it's fine"
        # So did the newline in output: its assignment is a control variable.
        assert op.variables["msg"] == "quote ' here"

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
        op.attach_loop_table(_one_row_table())
        incoming = State({"i": 1, "note": "it's"})
        assert op.process_state(incoming, port=0) is None
        assert op.variables["i"] == 2  # update ran
        assert op.condition() is True  # quoted condition round-tripped


# ---------------------------------------------------------------------------
# LoopStartOperator -- init() sees the input data D; provideData() may be None
# ---------------------------------------------------------------------------


class TestLoopStartInitializationSeesInputData:
    """The files example (paper Figure 5): init `i = 0; file = D.path[0]`,
    provideData `None`. init() runs once the input table D has arrived, and a
    value that arrived on the back edge replaces the one init() set."""

    @staticmethod
    def _files_start() -> _StubLoopStart:
        op = _StubLoopStart(
            initialization="i = 0\nfile = D.path[0]", output_expr="None"
        )
        op.open()
        for path in ("a.txt", "b.txt"):
            list(op.process_tuple(Tuple({"path": path}), port=0))
        return op

    def test_initialization_reads_the_input_table_as_D(self):
        produced = self._files_start().produce_state_on_finish(port=0)
        assert produced["i"] == 0
        assert produced["file"] == "a.txt"
        assert "D" not in produced and "table" not in produced

    def test_the_cells_see_the_declared_columns_when_the_input_carried_no_rows(self):
        # A port that carried no rows has no tuples to read column names off,
        # so the cells get the columns it was declared to carry, as any
        # operator's process_table does: D.path is an empty column, not a
        # KeyError.
        op = _StubLoopStart(
            initialization="paths = list(D.path)", output_expr="n = len(D)"
        )
        op.input_schemas[0] = Schema(raw_schema={"path": "STRING"})
        op.open()

        produced = op.produce_state_on_finish(port=0)

        assert produced["paths"] == []
        assert produced["n"] == 0

    def test_a_back_edge_value_replaces_the_initial_one(self):
        op = _StubLoopStart(
            initialization="i = 0\nfile = D.path[0]", output_expr="None"
        )
        op.open()
        op.process_state(State({"i": 1, "file": "b.txt"}), port=0)
        list(op.process_tuple(Tuple({"path": "a.txt"}), port=0))
        produced = op.produce_state_on_finish(port=0)
        assert (produced["i"], produced["file"]) == (1, "b.txt")

    def test_provide_data_none_feeds_the_body_nothing(self):
        op = self._files_start()
        op.produce_state_on_finish(port=0)
        assert [out for out in op.on_finish(port=0) if out is not None] == []

    def test_output_sees_the_initialized_variables_whichever_callback_runs_first(self):
        op = _StubLoopStart(initialization="k = len(D)", output_expr="D.iloc[[k - 1]]")
        op.open()
        for v in (1, 2, 3):
            list(op.process_tuple(Tuple({"v": v}), port=0))
        emitted = [out for out in op.on_finish(port=0) if out is not None]
        assert len(emitted) == 1 and int(emitted[0]["v"].iloc[0]) == 3
        assert op.produce_state_on_finish(port=0)["k"] == 3

    def test_initialization_may_not_rebind_D(self):
        op = _StubLoopStart(initialization="D = 1", output_expr="None")
        op.open()
        list(op.process_tuple(Tuple({"v": 1}), port=0))
        with pytest.raises(ValueError, match="'D' is reserved by the loop runtime"):
            op.produce_state_on_finish(port=0)

    def test_loop_end_update_and_condition_see_D(self):
        end = _StubLoopEnd(
            update="i += 1\nfile = D.path.get(i)", condition_expr="i < len(D)"
        )
        end.attach_loop_table(
            Table([Tuple({"path": "a.txt"}), Tuple({"path": "b.txt"})])
        )
        end.process_state(State({"i": 0, "file": "a.txt"}), port=0)
        assert end.variables["file"] == "b.txt"
        assert end.condition() is True


# ---------------------------------------------------------------------------
# LoopStartOperator -- the Output cell
# ---------------------------------------------------------------------------


class TestLoopStartOutputCell:
    """Output is a Python cell. Every name it assigns becomes a control variable
    of the iteration, replacing one from Initialization or the back edge, and
    the value of a trailing bare expression is the data sent to the body. The
    runtime produces the state BEFORE on_finish drains the data (see
    DataProcessor.process_internal_marker), so the cell must already have run
    when the state is produced."""

    @staticmethod
    def _start(output: str, initialization: str = "i = 0") -> _StubLoopStart:
        op = _StubLoopStart(initialization=initialization, output_expr=output)
        op.open()
        for path in ("a.txt", "b.txt"):
            list(op.process_tuple(Tuple({"path": path}), port=0))
        return op

    @staticmethod
    def _data(op: _StubLoopStart) -> list:
        return [out for out in op.on_finish(port=0) if out is not None]

    def test_an_assignment_is_in_the_state_produced_before_on_finish(self):
        op = self._start("file = D.path[i]")
        assert op.produce_state_on_finish(port=0) == State({"i": 0, "file": "a.txt"})

    def test_an_assignment_replaces_the_initialization_value(self):
        op = self._start("file = D.path[i]", initialization="i = 0\nfile = 'init'")
        assert op.produce_state_on_finish(port=0)["file"] == "a.txt"

    def test_an_assignment_replaces_the_back_edge_value(self):
        op = _StubLoopStart(initialization="i = 0", output_expr="file = D.path[i]")
        op.open()
        op.process_state(State({"i": 1, "file": "stale"}), port=0)
        for path in ("a.txt", "b.txt"):
            list(op.process_tuple(Tuple({"path": path}), port=0))
        produced = op.produce_state_on_finish(port=0)
        # The cell read the back edge's i, not init's.
        assert (produced["i"], produced["file"]) == (1, "b.txt")

    def test_a_trailing_expression_is_the_data(self):
        op = self._start("file = D.path[i]\nD[D.path == file]")
        assert op.produce_state_on_finish(port=0)["file"] == "a.txt"
        (data,) = self._data(op)
        assert list(data["path"]) == ["a.txt"]

    @pytest.mark.parametrize("output", ["file = D.path[i]", "", "# nothing to send"])
    def test_without_a_trailing_expression_the_body_gets_no_data(self, output):
        op = self._start(output)
        op.produce_state_on_finish(port=0)
        assert self._data(op) == []

    def test_a_single_expression_is_the_data_and_sets_no_variable(self):
        op = self._start("D.iloc[[i]]")
        assert op.produce_state_on_finish(port=0) == State({"i": 0})
        (data,) = self._data(op)
        assert list(data["path"]) == ["a.txt"]

    @pytest.mark.parametrize(
        "output, name",
        [
            ("D = D.head(1)", "D"),
            ("table = 1\nD.iloc[[i]]", "table"),
            ("del D", "D"),
            # Rebound by the trailing expression itself.
            ("file = D.path[i]\n(D := 1)", "D"),
        ],
    )
    def test_rebinding_a_reserved_name_raises(self, output, name):
        op = self._start(output)
        with pytest.raises(
            ValueError, match=f"'{name}' is reserved by the loop runtime"
        ):
            op.produce_state_on_finish(port=0)

    def test_rebinding_D_raises_before_the_trailing_expression_reads_it(self):
        # Otherwise the user would see an AttributeError on the int, not why.
        op = self._start("D = 1\nD.iloc[[0]]")
        with pytest.raises(ValueError, match="'D' is reserved by the loop runtime"):
            op.produce_state_on_finish(port=0)

    @pytest.mark.parametrize("state_first", [True, False])
    def test_the_cell_runs_once_when_both_callbacks_run(self, state_first):
        op = self._start("n += 1\nD.iloc[[n - 1]]", initialization="i = 0\nn = 0")
        if state_first:
            produced = op.produce_state_on_finish(port=0)
            data = self._data(op)
        else:
            data = self._data(op)
            produced = op.produce_state_on_finish(port=0)
        assert produced["n"] == 1
        assert len(data) == 1 and list(data[0]["path"]) == ["a.txt"]
        # A repeated call hands back the same result without running it again.
        assert op.produce_state_on_finish(port=0)["n"] == 1
        assert op.eval_output(op._collected_table(0)) is data[0]

    def test_a_lambda_or_generator_expression_resolves_its_free_names(self):
        op = self._start(
            "ranked = sorted(D.path, key=lambda p: p != first)\n"
            "bumped = sum(x + i for x in [1, 2])",
            initialization="i = 1\nfirst = 'b.txt'",
        )
        produced = op.produce_state_on_finish(port=0)
        assert produced["ranked"] == ["b.txt", "a.txt"]
        assert produced["bumped"] == 5

    def test_only_control_variables_reach_the_state(self):
        op = self._start("file = D.path[i]\nD.iloc[[i]]")
        produced = op.produce_state_on_finish(port=0)
        assert set(produced) == {"i", "file"}
        State(produced).to_tuple()  # pure-JSON control variables

    def test_the_files_example_binds_one_file_per_iteration(self):
        # The paper's files example: Initialization "i = 0", Output
        # "file = D.line[i]", Update "i += 1", Condition "i < len(D)".
        rows = [Tuple({"line": name}) for name in ("a.txt", "b.txt", "c.txt")]
        back_edge, files = None, []
        while True:
            assert len(files) < 10, "loop failed to terminate"
            start = _StubLoopStart(
                initialization="i = 0", output_expr="file = D.line[i]"
            )
            start.open()
            if back_edge is not None:
                start.process_state(back_edge, port=0)
            for row in rows:
                list(start.process_tuple(row, port=0))
            produced = start.produce_state_on_finish(port=0)
            assert self._data(start) == []
            files.append(produced["file"])

            end = _StubLoopEnd(update="i += 1", condition_expr="i < len(D)")
            end.attach_loop_table(Table(rows))
            end.process_state(State.from_tuple(State(produced).to_tuple()), port=0)
            if not end.condition():
                break
            back_edge = State(end.variables)

        assert files == ["a.txt", "b.txt", "c.txt"]


class TestLoopStartOutputCellKeepsOldOutputs:
    """A single-expression Output behaves exactly as when it went through eval()."""

    @staticmethod
    def _start(output: str) -> _StubLoopStart:
        op = _StubLoopStart(initialization="i = 0", output_expr=output)
        op.open()
        for v in (1, 2):
            list(op.process_tuple(Tuple({"v": v}), port=0))
        return op

    @pytest.mark.parametrize("output", [" table.iloc[[i]]", "\tD.iloc[[i]]", "  None"])
    def test_leading_whitespace_is_ignored_as_eval_did(self, output):
        op = self._start(output)
        assert op.produce_state_on_finish(port=0) == State({"i": 0})
        emitted = [out for out in op.on_finish(port=0) if out is not None]
        assert len(emitted) == (0 if output.strip() == "None" else 1)

    def test_a_walrus_in_a_single_expression_sets_no_control_variable(self):
        op = self._start("(row := D.iloc[[i]])")
        assert op.produce_state_on_finish(port=0) == State({"i": 0})
        assert len([out for out in op.on_finish(port=0) if out is not None]) == 1


class TestLoopPrimitivesKeepOnlyValues:
    """Imports, functions and classes a cell defines are helpers, not control
    variables: they never reach the State, which could not serialize them."""

    def test_the_output_cell_keeps_helpers_out_of_the_state(self):
        op = _StubLoopStart(
            initialization="i = 0",
            output_expr=(
                "import os\n"
                "def stem(p):\n    return os.path.splitext(p)[0]\n"
                "class Box:\n    pass\n"
                "pick = lambda d: d.v[i]\n"
                "n: int = 1\n"
                "name = stem('a.txt')"
            ),
        )
        op.open()
        list(op.process_tuple(Tuple({"v": 5}), port=0))
        produced = op.produce_state_on_finish(port=0)
        assert produced == State({"i": 0, "n": 1, "name": "a"})
        # and the State round-trips, as the back edge needs
        assert State.from_tuple(produced.to_tuple()) == produced

    def test_initialization_keeps_helpers_out_of_the_state(self):
        op = _StubLoopStart(
            initialization="import math\ni = math.floor(0.5)", output_expr="None"
        )
        op.open()
        list(op.process_tuple(Tuple({"v": 1}), port=0))
        assert op.produce_state_on_finish(port=0) == State({"i": 0})

    def test_update_keeps_helpers_out_of_the_variables(self):
        end = _StubLoopEnd(
            update="import math\ni = math.floor(i + 1.5)", condition_expr="i < 3"
        )
        end.attach_loop_table(Table([Tuple({"v": 1})]))
        end.process_state(State({"i": 0}), port=0)
        assert end.variables == State({"i": 1})

    def test_a_numeric_value_read_from_D_is_a_control_variable(self):
        op = _StubLoopStart(
            initialization="i = 0", output_expr="k = D.k[i]\nok = D.k[i] > 0"
        )
        op.open()
        list(op.process_tuple(Tuple({"k": 3}), port=0))
        produced = op.produce_state_on_finish(port=0)
        restored = State.from_tuple(produced.to_tuple())
        assert (restored["k"], restored["ok"]) == (3, True)
