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

import base64

import pandas
import pytest

from core.models import (
    BatchOperator,
    Schema,
    SourceOperator,
    State,
    Table,
    Tuple,
    TupleOperatorV2,
)
from core.models.operator import Operator, TableOperator, referenced_loop_variables


class _ConcreteOperator(TupleOperatorV2):
    """Minimal concrete subclass; implements abstract process_tuple."""

    def process_tuple(self, tuple_, port):
        yield tuple_


class _ConcreteSource(SourceOperator):
    """Minimal concrete subclass; implements abstract produce."""

    def produce(self):
        yield None


class _ConcreteBatch(BatchOperator):
    BATCH_SIZE = 4

    def process_batch(self, batch, port):
        yield batch


class _ProducingSource(SourceOperator):
    """Source whose produce() actually emits raw (non-Tuple) records."""

    def produce(self):
        yield {"x": 1}
        yield {"x": 2}


class _TableProducingSource(SourceOperator):
    """Source whose produce() emits a whole Table in one go."""

    def produce(self):
        yield Table([Tuple({"x": 1}), Tuple({"x": 2})])


class _MixedProducingSource(SourceOperator):
    """Source whose produce() interleaves a None signal between two records."""

    def produce(self):
        yield {"x": 1}
        yield None
        yield {"x": 2}


class _NoneOutputBatch(BatchOperator):
    """Batch operator whose process_batch declines to emit anything.

    It records the Batches it is handed so a test asserting *absence* of output
    can also assert the batch actually ran.
    """

    BATCH_SIZE = 1

    def __init__(self):
        super().__init__()
        self.batches = []

    def process_batch(self, batch, port):
        self.batches.append(batch)
        yield None


class _TupleOutputBatch(BatchOperator):
    """Batch operator whose process_batch emits a non-DataFrame output."""

    BATCH_SIZE = 1

    def process_batch(self, batch, port):
        yield Tuple({"y": 42})


class _DataFrameOutputBatch(BatchOperator):
    """Batch operator whose process_batch emits a multi-row DataFrame."""

    BATCH_SIZE = 1

    def process_batch(self, batch, port):
        yield pandas.DataFrame([{"y": 1}, {"y": 2}])


class _MultiOutputBatch(BatchOperator):
    """Batch operator whose process_batch emits more than one output."""

    BATCH_SIZE = 1

    def process_batch(self, batch, port):
        yield Tuple({"y": 1})
        yield Tuple({"y": 2})


class _SpyBatch(BatchOperator):
    """Batch operator that records the rows and the port of every Batch handed
    to it, and echoes the row count back as its single output."""

    BATCH_SIZE = 2

    def __init__(self):
        super().__init__()
        self.seen = []
        self.ports = []

    def process_batch(self, batch, port):
        self.seen.append([list(row) for _, row in batch.iterrows()])
        self.ports.append(port)
        yield Tuple({"batched": len(self.seen[-1])})


def _take(iterator, limit):
    """The first `limit` items of `iterator`.

    Bounded on purpose: `BatchOperator.on_finish` loops `while` the port buffer
    is non-empty, so a mutant that stops `_process_batch` from draining that
    buffer turns it into an infinite generator. An unbounded `list()` would
    hang there instead of failing an assertion.
    """
    return [item for _, item in zip(range(limit), iterator)]


class _ConcreteTable(TableOperator):
    """Concrete subclass that records the table it received via process_table."""

    def __init__(self):
        super().__init__()
        self.received_tables = []

    def process_table(self, table, port):
        self.received_tables.append(table)
        yield None


class TestPythonTemplateDecoder:
    def test_stdlib_decoder_decodes_str_input(self):
        decoder = Operator.PythonTemplateDecoder.StdlibBase64Decoder()
        encoded = base64.b64encode(b"hello").decode("ascii")
        assert decoder.to_str(encoded) == "hello"

    def test_stdlib_decoder_accepts_bytes_input(self):
        decoder = Operator.PythonTemplateDecoder.StdlibBase64Decoder()
        encoded = base64.b64encode("中".encode("utf-8"))  # bytes
        assert decoder.to_str(encoded) == "中"

    def test_stdlib_decoder_rejects_non_utf8_bytes_strictly(self):
        # `errors='strict'` must raise; `0x80` is not a valid UTF-8 leading byte.
        decoder = Operator.PythonTemplateDecoder.StdlibBase64Decoder()
        bad = base64.b64encode(b"\x80\x81").decode("ascii")
        with pytest.raises(UnicodeDecodeError):
            decoder.to_str(bad)

    def test_default_decoder_when_none_supplied(self):
        wrapper = Operator.PythonTemplateDecoder()
        encoded = base64.b64encode(b"abc").decode("ascii")
        assert wrapper.decode(encoded) == "abc"

    def test_uses_injected_custom_decoder(self):
        class CountingDecoder:
            def __init__(self):
                self.calls = 0

            def to_str(self, data):
                self.calls += 1
                return f"decoded:{data}"

        injected = CountingDecoder()
        wrapper = Operator.PythonTemplateDecoder(decoder=injected)
        assert wrapper.decode("x") == "decoded:x"
        assert injected.calls == 1

    def test_lru_cache_reuses_results_for_repeated_inputs(self):
        # Pin: the cache short-circuits the underlying decoder so identical
        # inputs incur only one decode call. This is what makes the wrapper
        # cheap when the same template appears in many tuples.
        class CountingDecoder:
            def __init__(self):
                self.calls = 0

            def to_str(self, data):
                self.calls += 1
                return f"d{self.calls}:{data}"

        injected = CountingDecoder()
        wrapper = Operator.PythonTemplateDecoder(decoder=injected, cache_size=8)
        first = wrapper.decode("same")
        second = wrapper.decode("same")
        assert first == "d1:same"
        assert second == "d1:same"  # same cached result
        assert injected.calls == 1

    def test_lru_cache_evicts_when_size_exceeded(self):
        class CountingDecoder:
            def __init__(self):
                self.calls = 0

            def to_str(self, data):
                self.calls += 1
                return f"d{self.calls}:{data}"

        injected = CountingDecoder()
        wrapper = Operator.PythonTemplateDecoder(decoder=injected, cache_size=2)
        wrapper.decode("a")
        wrapper.decode("b")
        wrapper.decode("c")  # evicts "a"
        wrapper.decode("a")  # cache miss → re-decode
        assert injected.calls == 4


class TestIsSourceProperty:
    def test_default_is_false(self):
        op = _ConcreteOperator()
        assert op.is_source is False

    def test_setter_true_takes_effect(self):
        op = _ConcreteOperator()
        op.is_source = True
        assert op.is_source is True

    def test_setter_can_flip_back_to_false(self):
        op = _ConcreteOperator()
        op.is_source = True
        op.is_source = False
        assert op.is_source is False

    def test_source_operator_subclass_reports_is_source_true(self):
        src = _ConcreteSource()
        assert src.is_source is True


class TestOperatorDefaultMethods:
    def test_open_is_no_op(self):
        # No state to assert; verify it does not raise and returns None.
        assert _ConcreteOperator().open() is None

    def test_close_is_no_op(self):
        assert _ConcreteOperator().close() is None

    def test_process_state_returns_input_state_unchanged(self):
        # Default behavior is to forward the State to downstream operators.
        op = _ConcreteOperator()
        state = State()
        assert op.process_state(state, port=0) is state

    def test_produce_state_on_start_returns_none_by_default(self):
        assert _ConcreteOperator().produce_state_on_start(port=0) is None

    def test_produce_state_on_finish_returns_none_by_default(self):
        assert _ConcreteOperator().produce_state_on_finish(port=0) is None

    def test_default_on_finish_yields_exactly_one_none(self):
        # TupleOperatorV2.on_finish is the model-layer default hook. No in-repo
        # subclass inherits it: SourceOperator/BatchOperator/TableOperator all
        # override it, and pytexera's UDFOperatorV2 shadows it with a
        # byte-identical body of its own. So this pins the base-class contract
        # for any future direct subclass -- a generator that yields exactly one
        # None, not an empty generator and not a plain return.
        assert list(_ConcreteOperator().on_finish(port=0)) == [None]


class TestLoopVariables:
    # Inside a control block the loop variables reach an operator as state
    # messages; the runtime registers each one (register_state) right before
    # process_state, with the nesting depth its envelope carries: 0 from the
    # innermost loop around the operator, 1 from the loop around that. A state
    # no loop sent (an operator's produce_state_on_finish) travels at depth 0.

    def test_empty_until_the_runtime_registers_a_state(self):
        assert dict(_ConcreteOperator().loop_variables) == {}

    def test_holds_the_variables_a_registered_state_carries(self):
        op = _ConcreteOperator()
        op.register_state(State({"i": 2, "k": "a"}), 0)
        assert dict(op.loop_variables) == {"i": 2, "k": "a"}

    def test_a_state_that_names_no_loop_variable_leaves_them_intact(self):
        # The loop's state, then a body operator's own state from
        # produce_state_on_finish, both at depth 0: the second adds its names
        # and must not erase i (Xiaozhen's KeyError 'i' on #8642).
        op = _ConcreteOperator()
        op.register_state(State({"i": 2}), 0)
        op.register_state(State({"note": "from-body-op"}), 0)
        assert dict(op.loop_variables) == {"i": 2, "note": "from-body-op"}

    def test_two_different_values_from_the_same_depth_fail_when_read(self):
        # Xiaozhen's case on #8642: the loop's i = 0, then a body operator's
        # state carrying i = 99, both at depth 0. A JVM operator whose setting
        # refers to i fails with this message (StateReferenceBinding); a UDF
        # gets the same one when it reads i.
        op = _ConcreteOperator()
        op.register_state(State({"i": 0, "k": "a"}), 0)
        op.register_state(State({"i": 99}), 0)
        with pytest.raises(RuntimeError) as error:
            op.loop_variables["i"]
        assert str(error.value) == (
            "loop variable i got two different values in one iteration, 0 and 99: "
            "a loop's variables must not change inside its body"
        )
        for read in (
            lambda view: view.get("i"),
            lambda view: dict(view),
            lambda view: list(view.values()),
            lambda view: list(view.items()),
        ):
            with pytest.raises(RuntimeError, match="two different values"):
                read(op.loop_variables)
        # Only reading i fails: the other names read, and i is still listed.
        assert op.loop_variables["k"] == "a"
        assert "i" in op.loop_variables
        assert sorted(op.loop_variables) == ["i", "k"]
        assert len(op.loop_variables) == 2

    def test_equal_copies_from_the_same_depth_read_without_error(self):
        # Copies of one message, from several upstream workers or branches,
        # carry equal values: no conflict, as on the JVM (2 equals 2.0 there).
        op = _ConcreteOperator()
        op.register_state(State({"i": 2}), 0)
        op.register_state(State({"i": 2}), 0)
        op.register_state(State({"i": 2.0}), 0)
        assert op.loop_variables["i"] == 2
        assert dict(op.loop_variables) == {"i": 2}

    def test_an_inner_value_replaces_two_different_outer_ones(self):
        # The enclosing loop delivered two different values of i, then the
        # inner loop's own i arrives: the operator reads the inner one.
        op = _ConcreteOperator()
        op.register_state(State({"i": 1}), 1)
        op.register_state(State({"i": 2}), 1)
        op.register_state(State({"i": 5}), 0)
        assert op.loop_variables["i"] == 5

    def test_different_outer_values_after_an_inner_one_read_the_inner_one(self):
        op = _ConcreteOperator()
        op.register_state(State({"i": 5}), 0)
        op.register_state(State({"i": 1}), 1)
        op.register_state(State({"i": 2}), 1)
        assert op.loop_variables["i"] == 5

    def test_the_inner_value_wins_over_an_outer_one_that_arrives_later(self):
        # i = 1 from the outer loop, i = 5 from the inner one, then the outer
        # message again: the smallest depth wins, as a JVM operator binds it.
        op = _ConcreteOperator()
        op.register_state(State({"i": 1}), 1)
        assert op.loop_variables["i"] == 1, "the outer i until the inner arrives"
        op.register_state(State({"i": 5}), 0)
        op.register_state(State({"i": 1}), 1)
        assert op.loop_variables["i"] == 5

    def test_an_outer_variable_the_inner_loop_does_not_name_stays_visible(self):
        op = _ConcreteOperator()
        op.register_state(State({"i": 1, "n": 10}), 1)
        op.register_state(State({"i": 5}), 0)
        assert dict(op.loop_variables) == {"i": 5, "n": 10}

    def test_a_deeper_message_takes_over_from_an_outer_one_at_any_depth(self):
        op = _ConcreteOperator()
        op.register_state(State({"i": 1}), 2)
        op.register_state(State({"i": 2}), 1)
        op.register_state(State({"i": 3}), 2)
        assert op.loop_variables["i"] == 2

    def test_registering_leaves_the_state_message_unchanged(self):
        op = _ConcreteOperator()
        first, second = State({"i": 1}), State({"note": "n"})
        op.register_state(first, 0)
        op.register_state(second, 0)
        assert first == State({"i": 1})
        assert second == State({"note": "n"})

    def test_loop_variables_are_read_only(self):
        # A UDF changes a loop variable by sending a state message, which
        # downstream operators then receive too, not by writing this view.
        op = _ConcreteOperator()
        op.register_state(State({"i": 1}), 0)
        with pytest.raises(TypeError):
            op.loop_variables["i"] = 2
        assert op.loop_variables["i"] == 1

    def test_registers_on_the_instance_only(self):
        # The defaults live on the class, so a registration written to the
        # class (instead of the instance) would reach every operator.
        target, bystander = _ConcreteOperator(), _ConcreteOperator()
        target.register_state(State({"i": 1}), 0)
        assert dict(bystander.loop_variables) == {}
        assert dict(_ConcreteOperator().loop_variables) == {}

    def test_a_subclass_init_that_skips_super_still_reads_and_registers(self):
        class _NoSuperInit(TupleOperatorV2):
            def __init__(self):
                pass

            def process_tuple(self, tuple_, port):
                yield tuple_

        op = _NoSuperInit()
        assert dict(op.loop_variables) == {}
        op.register_state(State({"i": 1}), 0)
        assert dict(op.loop_variables) == {"i": 1}


class TestLazyTemplateDecoder:
    def test_first_call_creates_decoder_and_caches_on_instance(self):
        op = _ConcreteOperator()
        assert not hasattr(op, "_python_template_decoder")
        op._get_template_decoder()
        assert hasattr(op, "_python_template_decoder")

    def test_subsequent_calls_reuse_the_cached_decoder(self):
        op = _ConcreteOperator()
        first = op._get_template_decoder()
        second = op._get_template_decoder()
        assert first is second

    def test_decode_python_template_delegates_to_lazy_decoder(self):
        op = _ConcreteOperator()
        encoded = base64.b64encode(b"payload").decode("ascii")
        assert op.decode_python_template(encoded) == "payload"


# The shape the backend gives a Python-generated operator's code inside a
# control block: where a text property holds `$name`, the rendered
# `self.decode_python_template('<base64 of "$name">')` is replaced by
# `self.loop_variable_text('name')`. Mirrors the sklearn trainers'
# `<name> = <type> (<value>),` hyperparameter rendering, including the lambda
# the SVC trainer emits as its boolean converter.
_GENERATED_TRAINER_TEMPLATE = """from pytexera import *

class ProcessTableOperator(UDFTableOperator):

  @overrides
  def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
    yield dict(C = float (self.loop_variable_text('c')),kernel = str (self.loop_variable_text('kernel')),probability = (lambda value: value.lower() == "true") (self.loop_variable_text('p')),)
"""


class TestLoopVariableText:
    def test_returns_the_text_of_an_int_a_float_and_a_string(self):
        op = _ConcreteOperator()
        op.register_state(State({"n": 3, "rate": 0.5, "ticker": "AAPL"}))
        assert op.loop_variable_text("n") == "3"
        assert op.loop_variable_text("rate") == "0.5"
        assert op.loop_variable_text("ticker") == "AAPL"

    def test_spells_a_boolean_as_the_jvm_binds_one(self):
        # A JVM operator's text property takes a boolean as true / false
        # (StateReferenceBinding), and so does this, not Python's True / False.
        op = _ConcreteOperator()
        op.register_state(State({"on": True, "off": False}))
        assert op.loop_variable_text("on") == "true"
        assert op.loop_variable_text("off") == "false"

    def test_a_falsy_value_is_still_carried(self):
        # Zero, False and the empty string are values the state carried, not
        # absent ones: none of them may fall into the missing-name error.
        op = _ConcreteOperator()
        op.register_state(State({"n": 0, "flag": False, "note": ""}))
        assert op.loop_variable_text("n") == "0"
        assert op.loop_variable_text("flag") == "false"
        assert op.loop_variable_text("note") == ""

    @pytest.mark.parametrize("value", [None, [1, 2], {"p": 2}, b"raw"], ids=repr)
    def test_raises_for_a_value_that_is_not_a_scalar(self, value):
        # The JVM rejects a non-scalar for a text property too, rather than
        # pass on a spelling of its own, such as Python's repr.
        op = _ConcreteOperator()
        op.register_state(State({"m": value}))
        with pytest.raises(RuntimeError) as excinfo:
            op.loop_variable_text("m")
        assert str(excinfo.value) == (
            f"the operator refers to loop variable $m, but its value {value!r} "
            "is not a scalar"
        )

    def test_a_value_that_looks_like_a_reference_is_returned_verbatim(self):
        # The text is the value itself; it is never resolved again.
        op = _ConcreteOperator()
        op.register_state(State({"ticker": "$AAPL", "AAPL": "not me"}))
        assert op.loop_variable_text("ticker") == "$AAPL"

    def test_a_later_state_message_replaces_a_value_it_carries(self):
        # The text is read from the value loop_variables holds when the code
        # runs, never cached: here the inner loop's message replaces the outer
        # loop's value.
        op = _ConcreteOperator()
        op.register_state(State({"i": 1}), 1)
        assert op.loop_variable_text("i") == "1"
        op.register_state(State({"i": 2}), 0)
        assert op.loop_variable_text("i") == "2"

    def test_a_later_state_message_without_the_name_keeps_its_value(self):
        # A loop body operator's own state (here a UDF publishing centroids)
        # reaches the operator after the loop's: it must not hide `i`.
        op = _ConcreteOperator()
        op.register_state(State({"i": 1}))
        op.register_state(State({"centroids": 3}))
        assert op.loop_variable_text("i") == "1"
        assert op.loop_variable_text("centroids") == "3"

    def test_an_inner_loops_state_shadows_the_outer_one_and_keeps_the_rest(self):
        # A nested loop's body receives the outer loop's state first (depth 1),
        # then the inner Loop Start's (depth 0), as a JVM operator does.
        op = _ConcreteOperator()
        op.register_state(State({"j": 5, "i": "outer"}), 1)
        op.register_state(State({"i": 0}), 0)
        assert op.loop_variable_text("j") == "5"
        assert op.loop_variable_text("i") == "0"

    def test_the_inner_value_wins_over_an_outer_one_that_arrives_later(self):
        # A JVM operator binds the inner loop's i even when another copy of
        # the outer message arrives after it, so the generated code reads the
        # same i.
        op = _ConcreteOperator()
        op.register_state(State({"i": 1}), 1)
        op.register_state(State({"i": 5}), 0)
        op.register_state(State({"i": 1}), 1)
        assert op.loop_variable_text("i") == "5"

    def test_registering_reaches_the_one_operator_only(self):
        # The defaults live on the class, so a registration written there
        # would reach every operator.
        target, bystander = _ConcreteOperator(), _ConcreteOperator()
        target.register_state(State({"i": 1}))
        assert target.loop_variable_text("i") == "1"
        with pytest.raises(RuntimeError, match="read before the iteration's state"):
            bystander.loop_variable_text("i")

    def test_raises_when_read_before_the_iterations_state_arrived(self):
        op = _ConcreteOperator()
        with pytest.raises(RuntimeError) as excinfo:
            op.loop_variable_text("i")
        assert str(excinfo.value) == (
            "loop variable $i is read before the iteration's state arrived"
        )

    def test_raises_for_a_name_no_state_message_carried(self):
        op = _ConcreteOperator()
        op.register_state(State({"i": 1}))
        op.register_state(State({"k": 2}))
        with pytest.raises(RuntimeError) as excinfo:
            op.loop_variable_text("j")
        assert str(excinfo.value) == (
            "the operator refers to loop variable $j, but no state message carried it"
        )

    def test_an_empty_state_has_arrived_but_carries_no_name(self):
        # An empty state is a state that arrived: the missing-name error, not
        # the not-yet-arrived one.
        op = _ConcreteOperator()
        op.register_state(State())
        with pytest.raises(RuntimeError, match="no state message carried it"):
            op.loop_variable_text("i")

    def test_a_prefix_of_a_declared_name_is_not_matched(self):
        op = _ConcreteOperator()
        op.register_state(State({"items": 3}))
        with pytest.raises(RuntimeError, match=r"\$item, but no state message"):
            op.loop_variable_text("item")

    def test_an_extension_of_a_declared_name_is_not_matched(self):
        op = _ConcreteOperator()
        op.register_state(State({"item": 3}))
        with pytest.raises(RuntimeError, match=r"\$items, but no state message"):
            op.loop_variable_text("items")

    def test_names_are_case_sensitive(self):
        op = _ConcreteOperator()
        op.register_state(State({"i": 1}))
        with pytest.raises(RuntimeError, match=r"\$I, but no state message"):
            op.loop_variable_text("I")

    def test_generated_code_reads_the_iterations_value_at_run_time(self):
        # __name__ so the exec'd class's methods get a real __module__ (the
        # @overrides decorator on the generated methods inspects it).
        namespace: dict = {"__name__": "generated_trainer_operator"}
        exec(_GENERATED_TRAINER_TEMPLATE, namespace)
        op = namespace["ProcessTableOperator"]()

        # Before the iteration's state arrives, the generated call fails loud
        # instead of converting a stand-in value.
        with pytest.raises(RuntimeError, match="read before the iteration's state"):
            list(op.process_table(Table([Tuple({"a": 1})]), 0))

        op.register_state(State({"c": 2, "kernel": "rbf", "p": True}))
        # The worker binds them before the operator's first tuple.
        op.bind_loop_variables()
        (params,) = list(op.process_table(Table([Tuple({"a": 1})]), 0))
        assert params == {"C": 2.0, "kernel": "rbf", "probability": True}

        # A state message after that leaves the iteration's values as bound.
        op.register_state(State({"c": 0.25, "kernel": "linear", "p": False}))
        (params,) = list(op.process_table(Table([Tuple({"a": 1})]), 0))
        assert params == {"C": 2.0, "kernel": "rbf", "probability": True}

        # The next iteration runs on a fresh worker, which binds its own.
        op = namespace["ProcessTableOperator"]()
        op.register_state(State({"c": 0.25, "kernel": "linear", "p": False}))
        op.bind_loop_variables()
        (params,) = list(op.process_table(Table([Tuple({"a": 1})]), 0))
        assert params == {"C": 0.25, "kernel": "linear", "probability": False}


class TestBindLoopVariables:
    # The paper (sec. 5): an operator binds its properties immediately before
    # its first tuple, and they stay fixed for the iteration; UDF code still
    # reads the values that arrive later. A JVM operator does the same
    # (OperatorExecutor.bindStateReferences).

    def test_a_state_after_binding_changes_loop_variables_but_not_the_text(self):
        # The outer loop's i, then the operator's first tuple, then a deeper
        # i (a body UDF's produce_state_on_finish): a JVM operator keeps 1.
        op = _ConcreteOperator()
        op.register_state(State({"i": 1}), 1)
        op.bind_loop_variables()
        op.register_state(State({"i": 5}), 0)
        assert op.loop_variable_text("i") == "1"
        assert op.loop_variables == {"i": 5}

    def test_binding_again_keeps_the_first_values(self):
        # The worker binds before every tuple and on every finishing port;
        # only the first call takes the values.
        op = _ConcreteOperator()
        op.register_state(State({"i": 1}))
        op.bind_loop_variables()
        op.register_state(State({"i": 2}))
        op.bind_loop_variables()
        assert op.loop_variable_text("i") == "1"

    def test_binds_the_inner_value_when_the_outer_one_arrives_again_first(self):
        # Xiaozhen's order on #8671 (outer, inner, outer again), all before
        # the first tuple: the bound i is the inner one, as on the JVM.
        op = _ConcreteOperator()
        op.register_state(State({"i": 1}), 1)
        op.register_state(State({"i": 5}), 0)
        op.register_state(State({"i": 1}), 1)
        op.bind_loop_variables()
        assert op.loop_variable_text("i") == "5"

    def test_a_name_that_arrives_only_after_binding_is_missing(self):
        op = _ConcreteOperator()
        op.register_state(State({"i": 1}))
        op.bind_loop_variables()
        op.register_state(State({"j": 2}))
        assert op.loop_variables["j"] == 2
        with pytest.raises(RuntimeError) as excinfo:
            op.loop_variable_text("j")
        # Bound at the first tuple, or when an input ended before any tuple.
        assert str(excinfo.value) == (
            "the operator refers to loop variable $j, but no state message "
            "carried it before the operator's first tuple or the end of an input"
        )

    def test_binding_before_any_state_binds_no_values(self):
        # Bound with nothing received (no state came before the first tuple,
        # or a source), the name is missing, not yet to arrive, and a later
        # state cannot supply it.
        op = _ConcreteOperator()
        op.bind_loop_variables()
        with pytest.raises(RuntimeError, match="before the operator's first tuple"):
            op.loop_variable_text("i")
        op.register_state(State({"i": 1}))
        with pytest.raises(RuntimeError, match="before the operator's first tuple"):
            op.loop_variable_text("i")

    def test_binding_reaches_the_one_operator_only(self):
        # The defaults live on the class, so a binding written there would
        # reach every operator.
        target, bystander = _ConcreteOperator(), _ConcreteOperator()
        for op in (target, bystander):
            op.register_state(State({"i": 1}), 1)
        target.bind_loop_variables()
        for op in (target, bystander):
            op.register_state(State({"i": 2}), 0)
        assert target.loop_variable_text("i") == "1"
        assert bystander.loop_variable_text("i") == "2"


def _missing(name: str) -> str:
    """The error for a required loop variable that no state carried in time."""
    return (
        f"the operator refers to loop variable ${name}, but no state message "
        "carried it before the operator's first tuple or the end of an input"
    )


# The shape of a generated table operator's code whose text property refers to
# $t, read only when its input has rows, as Line Chart reads its axis title.
_GENERATED_CHART_TEMPLATE = """from pytexera import *

class ProcessTableOperator(UDFTableOperator):

    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        if table.empty:
            yield {"html-content": "input table is empty"}
            return
        yield {"html-content": self.loop_variable_text('t')}
"""


class TestRequiredLoopVariables:
    # The paper (sec. 5): an operator reports an error if a required value is
    # missing at its first tuple, on any input, whether or not its code reads
    # the value then. A JVM operator's binding fails there, naming each
    # property no state message could write (StateReferenceBinding.bind).

    def test_binding_fails_for_a_required_variable_no_state_carried(self):
        op = _ConcreteOperator()
        op.require_loop_variables(["t"])
        op.register_state(State({"i": 1}))
        with pytest.raises(RuntimeError) as excinfo:
            op.bind_loop_variables()
        assert str(excinfo.value) == _missing("t")

    def test_binding_fails_when_no_state_arrived_at_all(self):
        # A source, or a body operator whose first tuple came before any state.
        op = _ConcreteOperator()
        op.require_loop_variables(["t"])
        with pytest.raises(RuntimeError) as excinfo:
            op.bind_loop_variables()
        assert str(excinfo.value) == _missing("t")

    def test_binding_names_every_variable_it_cannot_take_in_one_error(self):
        # As the JVM's binding does, in order, joined by "; ": a missing name,
        # and values a text property refuses (a list, an integer past 64 bits).
        op = _ConcreteOperator()
        op.require_loop_variables(["m", "big", "t", "i"])
        op.register_state(State({"i": 1, "m": [1, 2], "big": 2**64}))
        with pytest.raises(RuntimeError) as excinfo:
            op.bind_loop_variables()
        assert str(excinfo.value) == "; ".join(
            [
                "the operator refers to loop variable $m, but its value [1, 2] "
                "is not a scalar",
                "the operator refers to loop variable $big, but its value "
                "18446744073709551616 does not fit in the 64-bit integer a JVM "
                "operator reads it as",
                _missing("t"),
            ]
        )

    def test_a_failed_binding_binds_nothing_and_the_next_one_can_succeed(self):
        # As on the JVM, the values stay unbound after a failed binding: the
        # text still reads every state, and the next binding, before the next
        # tuple, takes the values once every required one has arrived. Each
        # later state comes from a more deeply nested loop, so it replaces the
        # values.
        op = _ConcreteOperator()
        op.require_loop_variables(["t"])
        op.register_state(State({"i": 1, "t": [1]}), 2)
        with pytest.raises(RuntimeError, match="is not a scalar"):
            op.bind_loop_variables()
        op.register_state(State({"i": 2, "t": "x"}), 1)
        assert op.loop_variable_text("i") == "2"
        op.bind_loop_variables()
        op.register_state(State({"i": 3, "t": "y"}), 0)
        assert op.loop_variable_text("i") == "2"
        assert op.loop_variable_text("t") == "x"

    def test_only_the_required_variables_need_to_arrive(self):
        # A loop variable no property refers to is UDF code's to read, if it
        # comes at all: binding does not wait for it.
        op = _ConcreteOperator()
        op.require_loop_variables(["t"])
        op.register_state(State({"t": "x"}))
        op.bind_loop_variables()
        assert op.loop_variable_text("t") == "x"
        with pytest.raises(RuntimeError, match=r"\$i, but no state message"):
            op.loop_variable_text("i")

    def test_requiring_reaches_the_one_operator_only(self):
        # The default lives on the class, so a requirement written there would
        # reach every operator.
        target, bystander = _ConcreteOperator(), _ConcreteOperator()
        target.require_loop_variables(["t"])
        for op in (target, bystander):
            op.register_state(State({"i": 1}))
        with pytest.raises(RuntimeError, match=r"\$t, but no state message"):
            target.bind_loop_variables()
        bystander.bind_loop_variables()
        assert bystander.loop_variable_text("i") == "1"

    def test_generated_code_that_skips_its_read_still_fails_at_binding(self):
        # An empty input takes the chart's early return, which never reads $t;
        # the binding before it reports the missing value all the same.
        namespace: dict = {"__name__": "generated_chart_operator"}
        exec(_GENERATED_CHART_TEMPLATE, namespace)
        op = namespace["ProcessTableOperator"]()
        op.require_loop_variables(["t"])
        op.register_state(State({"i": 1}))
        with pytest.raises(RuntimeError) as excinfo:
            op.bind_loop_variables()
        assert str(excinfo.value) == _missing("t")


def _two_values(name: str, earlier, later) -> str:
    """The error for reading a loop variable that got two different values."""
    return (
        f"loop variable {name} got two different values in one iteration, "
        f"{earlier} and {later}: a loop's variables must not change inside its body"
    )


# The shape of a generated operator's code whose text properties refer to $k
# only.
_GENERATED_K_TEMPLATE = """from pytexera import *

class ProcessTupleOperator(UDFOperatorV2):

    @overrides
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield {"k": self.loop_variable_text('k')}
"""


class TestTwoValuesOfOneNameAtBinding:
    # Reading a name that got two different values from the depth it is held
    # at fails (TestLoopVariables), and a JVM operator fails only for a name
    # its setting refers to (StateReferenceBinding). So binding fails only for
    # a required name, and the bound values keep the two: the other names
    # read, and reading that name fails.

    def test_an_operator_whose_code_reads_only_k_binds_and_reads_k(self):
        namespace: dict = {"__name__": "generated_k_operator"}
        exec(_GENERATED_K_TEMPLATE, namespace)
        op = namespace["ProcessTupleOperator"]()
        # What the executor manager requires for this code.
        op.require_loop_variables(referenced_loop_variables(_GENERATED_K_TEMPLATE))
        op.register_state(State({"i": 0, "k": "a"}), 0)
        op.register_state(State({"i": 99}), 0)
        op.bind_loop_variables()
        assert list(op.process_tuple(Tuple({"x": 1}), 0)) == [{"k": "a"}]

    def test_binding_fails_for_a_required_variable_with_two_values(self):
        # In the one error, in order, beside a refused and a missing value.
        op = _ConcreteOperator()
        op.require_loop_variables(["i", "m", "t"])
        op.register_state(State({"i": 0, "m": [1, 2]}), 0)
        op.register_state(State({"i": 99}), 0)
        with pytest.raises(RuntimeError) as excinfo:
            op.bind_loop_variables()
        assert str(excinfo.value) == "; ".join(
            [
                _two_values("i", 0, 99),
                "the operator refers to loop variable $m, but its value [1, 2] "
                "is not a scalar",
                _missing("t"),
            ]
        )

    def test_the_text_of_a_variable_with_two_values_at_binding_fails(self):
        # Bound with i's two values (its properties refer to k only), the
        # operator fails where its code reads i, and still does after a deeper
        # i arrives, as the bound values stay as they were.
        op = _ConcreteOperator()
        op.require_loop_variables(["k"])
        op.register_state(State({"i": 1, "k": "a"}), 1)
        op.register_state(State({"i": 2}), 1)
        op.bind_loop_variables()
        with pytest.raises(RuntimeError) as excinfo:
            op.loop_variable_text("i")
        assert str(excinfo.value) == _two_values("i", 1, 2)
        assert op.loop_variable_text("k") == "a"
        op.register_state(State({"i": 5}), 0)
        assert op.loop_variables["i"] == 5
        with pytest.raises(RuntimeError) as excinfo:
            op.loop_variable_text("i")
        assert str(excinfo.value) == _two_values("i", 1, 2)

    def test_a_deeper_value_before_binding_replaces_the_two_values(self):
        op = _ConcreteOperator()
        op.require_loop_variables(["i"])
        op.register_state(State({"i": 1}), 1)
        op.register_state(State({"i": 2}), 1)
        op.register_state(State({"i": 5}), 0)
        op.bind_loop_variables()
        assert op.loop_variable_text("i") == "5"


# Generated on the JVM, JDK 17 as the engine's images run it: each value's JSON
# as pyamber's State.to_json writes it, read by the JVM's State.fromJson, then
# turned to text as a JVM operator's text property binds it (value.toString in
# StateReferenceBinding).
_JVM_TEXT_OF_JSON = [
    ("0", "0"),
    ("1", "1"),
    ("-1", "-1"),
    ("42", "42"),
    ("2147483648", "2147483648"),
    ("9007199254740993", "9007199254740993"),
    ("9223372036854775807", "9223372036854775807"),
    ("-9223372036854775808", "-9223372036854775808"),
    ("0.0", "0.0"),
    ("-0.0", "-0.0"),
    ("1.0", "1.0"),
    ("-2.5", "-2.5"),
    ("100.0", "100.0"),
    ("1200.0", "1200.0"),
    ("123.0", "123.0"),
    ("123.456", "123.456"),
    ("0.1", "0.1"),
    ("0.3333333333333333", "0.3333333333333333"),
    ("4.35", "4.35"),
    ("0.001", "0.001"),
    ("0.0009999999999999998", "9.999999999999998E-4"),
    ("0.00099", "9.9E-4"),
    ("0.0005", "5.0E-4"),
    ("-1.5e-05", "-1.5E-5"),
    ("1e-07", "1.0E-7"),
    ("1234567.0", "1234567.0"),
    ("9999999.999999998", "9999999.999999998"),
    ("10000000.0", "1.0E7"),
    ("10000000.5", "1.00000005E7"),
    ("12345678.9", "1.23456789E7"),
    ("1e+16", "1.0E16"),
    ("1e+21", "1.0E21"),
    ("9007199254740992.0", "9.007199254740992E15"),
    ("1.7976931348623157e+308", "1.7976931348623157E308"),
    ("2.2250738585072014e-308", "2.2250738585072014E-308"),
    ("1e-320", "1.0E-320"),
    ("1.5e+300", "1.5E300"),
    ("true", "true"),
    ("false", "false"),
    ('"AAPL"', "AAPL"),
    ('""', ""),
    ('" padded "', " padded "),
    ('"\\u4f60\\u597d \\ud83d\\udc4b"', "你好 👋"),
    ('"1e7"', "1e7"),
    ('"true"', "true"),
    ('"line\\nbreak"', "line\nbreak"),
]

# Doubles JDK 17 spells with other digits than the fewest that read back as the
# value (JDK-4511638, fixed in JDK 19): (JSON, JDK 17's text, pyamber's text).
_JDK_17_OTHER_DIGITS = [
    ("1e+23", "9.999999999999999E22", "1.0E23"),
    ("2e+23", "1.9999999999999998E23", "2.0E23"),
    ("2.82879384806159e+17", "2.82879384806159008E17", "2.82879384806159E17"),
    ("5.960464477539063e-08", "5.9604644775390625E-8", "5.960464477539063E-8"),
    ("5e-324", "4.9E-324", "5.0E-324"),
]

# Integers past 64 bits: (JSON, the wrapped long the JVM reads).
_JVM_WRAPPED_INTEGERS = [
    ("9223372036854775808", "-9223372036854775808"),
    ("-9223372036854775809", "9223372036854775807"),
    ("18446744073709551616", "0"),
    ("100000000000000000000", "7766279631452241920"),
]


def _received(json_value: str) -> _ConcreteOperator:
    """An operator that received a state carrying `v` as this JSON."""
    op = _ConcreteOperator()
    op.register_state(State.from_json('{"v":' + json_value + "}"))
    return op


class TestLoopVariableTextParity:
    def test_spells_each_value_as_a_jvm_operators_text_property_binds_it(self):
        texts = [
            (json, _received(json).loop_variable_text("v"))
            for json, _ in _JVM_TEXT_OF_JSON
        ]
        assert texts == _JVM_TEXT_OF_JSON

    @pytest.mark.parametrize("json_value, jdk_17, text", _JDK_17_OTHER_DIGITS)
    def test_a_double_jdk_17_spells_otherwise_keeps_the_fewest_digits(
        self, json_value, jdk_17, text
    ):
        # Java's notation over Python's shortest digits: the text JDK 19 and
        # later print, and the same double JDK 17's text reads back as.
        assert _received(json_value).loop_variable_text("v") == text
        assert float(text) == float(jdk_17) == float(json_value)

    @pytest.mark.parametrize("json_value, jvm_long", _JVM_WRAPPED_INTEGERS)
    def test_refuses_an_integer_a_jvm_operator_reads_as_another(
        self, json_value, jvm_long
    ):
        # The JVM's State.fromJson keeps an integer as a 64-bit long, so it
        # reads this one wrapped, as jvm_long; refusing it keeps the generated
        # code from reading another number than a JVM operator.
        with pytest.raises(RuntimeError) as excinfo:
            _received(json_value).loop_variable_text("v")
        assert str(excinfo.value) == (
            f"the operator refers to loop variable $v, but its value {json_value} "
            "does not fit in the 64-bit integer a JVM operator reads it as"
        )

    @pytest.mark.parametrize(
        "json_value, text",
        [("NaN", "NaN"), ("Infinity", "Infinity"), ("-Infinity", "-Infinity")],
    )
    def test_spells_nan_and_infinity_as_java_does(self, json_value, text):
        # The JVM's State.fromJson rejects such a message (Jackson's default),
        # so no JVM operator reads one; the text is Double.toString's.
        assert _received(json_value).loop_variable_text("v") == text


class TestBatchOperatorValidation:
    def test_validate_batch_size_rejects_none(self):
        with pytest.raises(ValueError, match="cannot be None"):
            BatchOperator._validate_batch_size(None)

    def test_validate_batch_size_rejects_non_int(self):
        with pytest.raises(ValueError):
            BatchOperator._validate_batch_size("10")

    def test_validate_batch_size_non_int_message_names_the_float_type(self):
        # The message must name the offending type, not a template literal.
        with pytest.raises(ValueError) as excinfo:
            BatchOperator._validate_batch_size(10.0)
        assert str(excinfo.value) == "BATCH_SIZE cannot be <class 'float'>."

    def test_validate_batch_size_non_int_message_names_the_str_type(self):
        with pytest.raises(ValueError) as excinfo:
            BatchOperator._validate_batch_size("10")
        assert str(excinfo.value) == "BATCH_SIZE cannot be <class 'str'>."

    def test_concrete_batch_operator_with_float_size_reports_type_in_message(self):
        class _FloatBatch(BatchOperator):
            BATCH_SIZE = 10.0

            def process_batch(self, batch, port):
                yield batch

        with pytest.raises(ValueError) as excinfo:
            _FloatBatch()
        assert str(excinfo.value) == "BATCH_SIZE cannot be <class 'float'>."

    def test_validate_batch_size_rejects_zero(self):
        with pytest.raises(ValueError, match="positive"):
            BatchOperator._validate_batch_size(0)

    def test_validate_batch_size_rejects_negative(self):
        with pytest.raises(ValueError, match="positive"):
            BatchOperator._validate_batch_size(-3)

    def test_validate_batch_size_accepts_positive_int(self):
        # No raise = pass; method returns None implicitly.
        assert BatchOperator._validate_batch_size(1) is None
        assert BatchOperator._validate_batch_size(1024) is None

    def test_concrete_batch_operator_initializes_with_valid_size(self):
        op = _ConcreteBatch()
        assert op.BATCH_SIZE == 4


class TestTableOperator:
    def test_process_tuple_buffers_input_and_yields_none(self):
        # process_tuple is @final on TableOperator: it must record the tuple
        # internally and yield exactly one None so the framework's iterator
        # protocol still sees a value, but no output is produced per-tuple.
        op = _ConcreteTable()
        out = list(op.process_tuple(Tuple({"x": 1}), port=0))
        assert out == [None]
        # Nothing was passed downstream to process_table yet.
        assert op.received_tables == []

    def test_on_finish_calls_process_table_with_buffered_tuples(self):
        op = _ConcreteTable()
        list(op.process_tuple(Tuple({"x": 1, "y": "a"}), port=0))
        list(op.process_tuple(Tuple({"x": 2, "y": "b"}), port=0))
        # Drain on_finish so the generator runs.
        list(op.on_finish(port=0))

        assert len(op.received_tables) == 1
        table = op.received_tables[0]
        assert isinstance(table, Table)
        rows = [t for t in table.as_tuples()]
        assert rows == [Tuple({"x": 1, "y": "a"}), Tuple({"x": 2, "y": "b"})]

    def test_on_finish_with_no_buffered_tuples_yields_empty_table(self):
        op = _ConcreteTable()
        list(op.on_finish(port=0))
        assert len(op.received_tables) == 1
        assert list(op.received_tables[0].as_tuples()) == []

    def test_on_finish_with_no_rows_keeps_the_declared_columns(self):
        # The column names are read off the tuples, so a port that carried none
        # left the operator a frame of no columns and every operator naming one
        # of its own raised KeyError. The port's schema is the only record left.
        op = _ConcreteTable()
        op.input_schemas[0] = Schema(raw_schema={"x": "INTEGER", "y": "STRING"})

        list(op.on_finish(port=0))

        table = op.received_tables[0]
        # Still a Table, so an operator reading it with as_tuples() keeps working.
        assert isinstance(table, Table)
        assert list(table.as_tuples()) == []
        assert list(table.columns) == ["x", "y"]
        assert table.empty
        # The dtype each column would have had with rows under it.
        assert table["x"].dtype == "int32"

    def test_buffers_are_keyed_by_port(self):
        # Each input port has its own tuple buffer; on_finish for one port
        # must not surface tuples written through a different port.
        op = _ConcreteTable()
        list(op.process_tuple(Tuple({"x": 1}), port=0))
        list(op.process_tuple(Tuple({"x": 99}), port=1))

        list(op.on_finish(port=0))
        rows = list(op.received_tables[0].as_tuples())
        assert rows == [Tuple({"x": 1})]


class TestSourceOperatorFinalMethods:
    """SourceOperator replaces both TupleOperatorV2 tuple hooks: on_finish is a
    source's only output path, and process_tuple is deliberately inert because
    a source has no input."""

    def test_on_finish_converts_each_produced_item_to_tuples(self):
        # produce() may emit raw records; on_finish must normalize every one of
        # them into a Tuple before it leaves the operator. Tuple.__eq__ starts
        # with `isinstance(other, Tuple)`, so this equality already pins the
        # type -- a raw dict fails it outright.
        out = list(_ProducingSource().on_finish(port=0))
        assert out == [Tuple({"x": 1}), Tuple({"x": 2})]

    def test_on_finish_flattens_a_produced_table_into_its_tuples(self):
        # A single produced Table must be exploded into its rows, not passed
        # downstream as one Table object.
        out = list(_TableProducingSource().on_finish(port=0))
        assert out == [Tuple({"x": 1}), Tuple({"x": 2})]

    def test_on_finish_preserves_produce_order_and_forwards_a_none_signal(self):
        # produce() yielding None is the documented "no data this round" signal.
        # Placing it *between* two records pins three things at once: the None
        # survives conversion, the records around it are still normalized, and
        # produce()'s emission order is preserved.
        out = list(_MixedProducingSource().on_finish(port=0))
        assert out == [Tuple({"x": 1}), None, Tuple({"x": 2})]

    def test_process_tuple_yields_exactly_one_none(self):
        # A source ignores any tuple handed to it, but still has to behave as a
        # generator producing one None.
        src = _ConcreteSource()
        assert list(src.process_tuple(Tuple({"x": 1}), port=0)) == [None]


class TestBatchOperatorOutputConversion:
    """_process_batch owns the batch->output conversion: drop Nones, explode
    DataFrames row-wise, pass anything else through untouched."""

    def test_none_output_batch_is_dropped(self):
        # BATCH_SIZE=1 makes process_tuple flush immediately.
        op = _NoneOutputBatch()
        assert list(op.process_tuple(Tuple({"x": 1}), port=0)) == []
        # Positive control for the absence above: the empty output must mean
        # "the None was dropped", not "no batch ever ran". A mutant that
        # suppresses the flush, or never calls process_batch, leaves this empty.
        assert len(op.batches) == 1
        assert list(op.batches[0].columns) == ["x"]

    def test_non_dataframe_output_batch_is_yielded_as_is(self):
        op = _TupleOutputBatch()
        out = list(op.process_tuple(Tuple({"x": 1}), port=0))
        assert out == [Tuple({"y": 42})]

    def test_dataframe_output_batch_is_exploded_into_rows(self):
        # The counterweight to the test above: a DataFrame output must come out
        # as one object per row, not as the frame itself.
        op = _DataFrameOutputBatch()
        out = list(op.process_tuple(Tuple({"x": 1}), port=0))
        assert len(out) == 2
        # This also pins provenance: the rows come from the *output* frame
        # (column "y"), not the input batch (column "x"). A mutant that
        # iterates the input batch fails here with a KeyError, or on the row
        # count above. An extra `isinstance(row, DataFrame)` check would be
        # dead weight -- DataFrame.iterrows() yields Series by library
        # contract, and the frame-yielding mutant fails the count first.
        assert [row["y"] for row in out] == [1, 2]

    def test_every_output_of_process_batch_is_forwarded_in_order(self):
        # One Batch may produce several outputs; all of them must come out, in
        # the order process_batch emitted them.
        op = _MultiOutputBatch()
        out = list(op.process_tuple(Tuple({"x": 1}), port=0))
        assert out == [Tuple({"y": 1}), Tuple({"y": 2})]


class TestBatchOperatorBatchAssembly:
    """The other half of _process_batch: how the Batch handed to process_batch
    is assembled out of the per-port tuple buffer."""

    def test_flush_hands_process_batch_the_buffered_rows_in_order(self):
        op = _SpyBatch()  # BATCH_SIZE = 2
        # First tuple is buffered only -- the batch is not full yet.
        assert list(op.process_tuple(Tuple({"x": 1}), port=1)) == []
        assert op.seen == []
        # The second tuple fills the batch and triggers the flush.
        out = list(op.process_tuple(Tuple({"x": 2}), port=1))
        assert op.seen == [[[1], [2]]]  # FIFO: 1 before 2
        assert op.ports == [1]  # the port it was buffered on, not port 0
        assert out == [Tuple({"batched": 2})]

    def test_on_finish_drains_the_buffer_in_batch_size_chunks(self):
        op = _SpyBatch()
        op.BATCH_SIZE = 5  # buffer more than one batch's worth without flushing
        for value in (1, 2, 3):
            assert list(op.process_tuple(Tuple({"x": value}), port=1)) == []
        assert op.seen == []
        op.BATCH_SIZE = 2  # now the buffer holds more than a single batch
        out = _take(op.on_finish(port=1), 8)
        # Two chunks: BATCH_SIZE rows, then the remainder. Not one big batch
        # (the min() cap), not a single chunk (the while loop), and not LIFO.
        assert op.seen == [[[1], [2]], [[3]]]
        assert op.ports == [1, 1]
        assert out == [Tuple({"batched": 2}), Tuple({"batched": 1})]

    def test_batch_buffers_are_keyed_by_port(self):
        # Mirrors TestTableOperator::test_buffers_are_keyed_by_port for the
        # batch path: a tuple arriving on one port must not fill another
        # port's batch.
        op = _SpyBatch()  # BATCH_SIZE = 2
        assert list(op.process_tuple(Tuple({"x": 1}), port=0)) == []
        assert list(op.process_tuple(Tuple({"x": 99}), port=1)) == []
        assert op.seen == []  # neither port reached 2 tuples
        out = list(op.process_tuple(Tuple({"x": 2}), port=0))
        assert op.seen == [[[1], [2]]]  # the port-1 tuple is not in here
        assert op.ports == [0]
        assert out == [Tuple({"batched": 2})]


class _TableWithoutSuperInit(TableOperator):
    def __init__(self):
        self.threshold = 1  # forgets super().__init__()

    def process_table(self, table, port):
        yield table


class _BatchWithoutSuperInit(BatchOperator):
    BATCH_SIZE = 2

    def __init__(self):
        self.seen = 0  # forgets super().__init__()

    def process_batch(self, batch, port):
        yield batch


class _TableReturningDataFrame(TableOperator):
    def process_table(self, table, port):
        return table


class _TableReturningNone(TableOperator):
    def process_table(self, table, port):
        return None


class _TableReturningList(TableOperator):
    def process_table(self, table, port):
        return [{"x": 1}]


class _BatchReturningBatch(BatchOperator):
    BATCH_SIZE = 1

    def process_batch(self, batch, port):
        return batch


class _SourceReturningDict(SourceOperator):
    def produce(self):
        return {"x": 1}


class TestCommonUDFMistakes:
    """Mistakes UDF authors make should either work or say how to fix them."""

    def test_table_operator_works_without_super_init(self):
        op = _TableWithoutSuperInit()
        list(op.process_tuple(Tuple({"x": 1}), 0))
        (table,) = list(op.on_finish(0))
        assert list(table["x"]) == [1]

    def test_batch_operator_works_without_super_init(self):
        op = _BatchWithoutSuperInit()
        list(op.process_tuple(Tuple({"x": 1}), 0))
        outputs = list(op.process_tuple(Tuple({"x": 2}), 0))
        assert [t["x"] for t in outputs] == [1, 2]

    @pytest.mark.parametrize(
        "operator", [_TableReturningDataFrame, _TableReturningNone]
    )
    def test_process_table_return_says_to_yield(self, operator):
        op = operator()
        list(op.process_tuple(Tuple({"x": 1}), 0))
        with pytest.raises(TypeError) as exc_info:
            list(op.on_finish(0))
        assert "process_table must `yield` results, not `return` them" in str(
            exc_info.value
        )

    def test_process_table_returning_a_list_still_works(self):
        op = _TableReturningList()
        assert list(op.on_finish(0)) == [{"x": 1}]

    def test_process_batch_return_says_to_yield(self):
        op = _BatchReturningBatch()
        with pytest.raises(TypeError) as exc_info:
            list(op.process_tuple(Tuple({"x": 1}), 0))
        assert "process_batch must `yield` results" in str(exc_info.value)

    def test_produce_return_says_to_yield(self):
        op = _SourceReturningDict()
        with pytest.raises(TypeError) as exc_info:
            list(op.on_finish(0))
        assert "produce must `yield` results" in str(exc_info.value)


class TestBufferCreatedOnlyByGetter:
    def test_table_operator_has_no_buffer_until_first_tuple(self):
        op = _TableWithoutSuperInit()
        assert "_TableOperator__table_data" not in op.__dict__

        class _WithSuper(TableOperator):
            def process_table(self, table, port):
                yield table

        assert "_TableOperator__table_data" not in _WithSuper().__dict__

    def test_batch_operator_still_rejects_a_bad_batch_size_at_construction(self):
        class _BadSize(BatchOperator):
            BATCH_SIZE = 0

            def process_batch(self, batch, port):
                yield batch

        with pytest.raises(ValueError):
            _BadSize()
