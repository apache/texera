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
import math

import pandas
import pytest

from core.models import (
    BatchOperator,
    SourceOperator,
    State,
    Table,
    Tuple,
    TupleOperatorV2,
)
from core.models.operator import Operator, TableOperator


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

    def test_loop_state_is_none_until_the_runtime_registers_a_state(self):
        # Inside a control block the iteration's loop variables reach the
        # operator as a state message; the runtime registers it on
        # `loop_state` before `process_state` runs. Nothing has arrived yet.
        assert _ConcreteOperator().loop_state is None

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
        op.register_loop_state(State({"n": 3, "rate": 0.5, "ticker": "AAPL"}))
        assert op.loop_variable_text("n") == "3"
        assert op.loop_variable_text("rate") == "0.5"
        assert op.loop_variable_text("ticker") == "AAPL"

    def test_spells_a_boolean_as_the_jvm_binds_one(self):
        # A JVM operator's text property takes a boolean as true / false
        # (LateBoundExecutor), and so does this, not Python's True / False.
        op = _ConcreteOperator()
        op.register_loop_state(State({"on": True, "off": False}))
        assert op.loop_variable_text("on") == "true"
        assert op.loop_variable_text("off") == "false"

    def test_a_falsy_value_is_still_carried(self):
        # Zero, False and the empty string are values the state carried, not
        # absent ones: none of them may fall into the missing-name error.
        op = _ConcreteOperator()
        op.register_loop_state(State({"n": 0, "flag": False, "note": ""}))
        assert op.loop_variable_text("n") == "0"
        assert op.loop_variable_text("flag") == "false"
        assert op.loop_variable_text("note") == ""

    @pytest.mark.parametrize("value", [None, [1, 2], {"p": 2}, b"raw"], ids=repr)
    def test_raises_for_a_value_that_is_not_a_scalar(self, value):
        # The JVM rejects a non-scalar for a text property too, rather than
        # pass on a spelling of its own, such as Python's repr.
        op = _ConcreteOperator()
        op.register_loop_state(State({"m": value}))
        with pytest.raises(RuntimeError) as excinfo:
            op.loop_variable_text("m")
        assert str(excinfo.value) == (
            f"the operator refers to loop variable $m, but its value {value!r} "
            "is not a scalar"
        )

    def test_a_value_that_looks_like_a_reference_is_returned_verbatim(self):
        # The text is the value itself; it is never resolved again.
        op = _ConcreteOperator()
        op.register_loop_state(State({"ticker": "$AAPL", "AAPL": "not me"}))
        assert op.loop_variable_text("ticker") == "$AAPL"

    def test_a_later_state_message_replaces_a_value_it_carries(self):
        # Every iteration delivers a fresh state message; the text is read
        # from the latest one that carried the name, never cached.
        op = _ConcreteOperator()
        op.register_loop_state(State({"i": 1}))
        assert op.loop_variable_text("i") == "1"
        op.register_loop_state(State({"i": 2}))
        assert op.loop_variable_text("i") == "2"

    def test_a_later_state_message_without_the_name_keeps_its_value(self):
        # A loop body operator's own state (here a UDF publishing centroids)
        # reaches the operator after the loop's: it must not hide `i`.
        op = _ConcreteOperator()
        op.register_loop_state(State({"i": 1}))
        op.register_loop_state(State({"centroids": 3}))
        assert op.loop_variable_text("i") == "1"
        assert op.loop_variable_text("centroids") == "3"

    def test_an_inner_loops_state_shadows_the_outer_one_and_keeps_the_rest(self):
        # A nested loop's body receives the outer loop's state first, then the
        # inner Loop Start's, as a JVM operator does.
        op = _ConcreteOperator()
        op.register_loop_state(State({"j": 5, "i": "outer"}))
        op.register_loop_state(State({"i": 0}))
        assert op.loop_variable_text("j") == "5"
        assert op.loop_variable_text("i") == "0"

    def test_registering_makes_the_message_loop_state_and_changes_no_message(self):
        op = _ConcreteOperator()
        first, second = State({"i": 1}), State({"j": 2})
        op.register_loop_state(first)
        op.register_loop_state(second)
        assert op.loop_state is second
        # The merge is the operator's own: neither message gains a key.
        assert first == State({"i": 1})
        assert second == State({"j": 2})

    def test_registering_reaches_the_one_operator_only(self):
        # The defaults live on the class, so a registration written there
        # would reach every operator.
        target, bystander = _ConcreteOperator(), _ConcreteOperator()
        target.register_loop_state(State({"i": 1}))
        assert target.loop_variable_text("i") == "1"
        with pytest.raises(RuntimeError, match="read before the iteration's state"):
            bystander.loop_variable_text("i")
        assert bystander.loop_state is None
        assert _ConcreteOperator.loop_state is None

    def test_raises_when_read_before_the_iterations_state_arrived(self):
        op = _ConcreteOperator()
        with pytest.raises(RuntimeError) as excinfo:
            op.loop_variable_text("i")
        assert str(excinfo.value) == (
            "loop variable $i is read before the iteration's state arrived"
        )

    def test_raises_for_a_name_no_state_message_carried(self):
        op = _ConcreteOperator()
        op.register_loop_state(State({"i": 1}))
        op.register_loop_state(State({"k": 2}))
        with pytest.raises(RuntimeError) as excinfo:
            op.loop_variable_text("j")
        assert str(excinfo.value) == (
            "the operator refers to loop variable $j, but no state message carried it"
        )

    def test_an_empty_state_has_arrived_but_carries_no_name(self):
        # An empty state is a state that arrived: the missing-name error, not
        # the not-yet-arrived one.
        op = _ConcreteOperator()
        op.register_loop_state(State())
        with pytest.raises(RuntimeError, match="no state message carried it"):
            op.loop_variable_text("i")

    def test_a_prefix_of_a_declared_name_is_not_matched(self):
        op = _ConcreteOperator()
        op.register_loop_state(State({"items": 3}))
        with pytest.raises(RuntimeError, match=r"\$item, but no state message"):
            op.loop_variable_text("item")

    def test_an_extension_of_a_declared_name_is_not_matched(self):
        op = _ConcreteOperator()
        op.register_loop_state(State({"item": 3}))
        with pytest.raises(RuntimeError, match=r"\$items, but no state message"):
            op.loop_variable_text("items")

    def test_names_are_case_sensitive(self):
        op = _ConcreteOperator()
        op.register_loop_state(State({"i": 1}))
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

        op.register_loop_state(State({"c": 2, "kernel": "rbf", "p": True}))
        (params,) = list(op.process_table(Table([Tuple({"a": 1})]), 0))
        assert params == {"C": 2.0, "kernel": "rbf", "probability": True}

        # The next iteration's state reaches the same operator instance.
        op.register_loop_state(State({"c": 0.25, "kernel": "linear", "p": False}))
        (params,) = list(op.process_table(Table([Tuple({"a": 1})]), 0))
        assert params == {"C": 0.25, "kernel": "linear", "probability": False}


# The shape the backend gives a Python-generated operator's code inside a
# control block where a numeric or boolean property holds `$name`: the value
# it wrote into the code is replaced by
# `self.loop_variable_value('<name>', '<kind>')`. Mirrors the 2D histogram's
# `nbinsx=$xBins,` rendering, with a number and a boolean keyword beside it.
_GENERATED_HISTOGRAM_TEMPLATE = """from pytexera import *

class ProcessTableOperator(UDFTableOperator):

    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        yield dict(
            nbinsx=self.loop_variable_value('n', 'integer'),
            nbinsy=self.loop_variable_value('n', 'integer'),
            opacity=self.loop_variable_value('alpha', 'number'),
            text_auto=self.loop_variable_value('labels', 'boolean')
        )
"""


class TestLoopVariableValue:
    @staticmethod
    def _read(value, kind):
        op = _ConcreteOperator()
        op.register_loop_state(State({"v": value}))
        return op.loop_variable_value("v", kind)

    @staticmethod
    def _rejection(value, kind):
        op = _ConcreteOperator()
        op.register_loop_state(State({"v": value}))
        with pytest.raises(RuntimeError) as excinfo:
            op.loop_variable_value("v", kind)
        return str(excinfo.value)

    @pytest.mark.parametrize(
        "value, expected",
        [
            (3, 3),
            (-7, -7),
            (0, 0),
            # An integral float, as BigDecimal("3.0").longValueExact() takes it,
            # including one Python spells with an exponent (1e+16).
            (3.0, 3),
            (1e16, 10**16),
            ("42", 42),
            (" 42 ", 42),
            ("-5", -5),
            ("+5", 5),
            ("4.2e1", 42),
            (2**63 - 1, 2**63 - 1),
            (-(2**63), -(2**63)),
        ],
        ids=repr,
    )
    def test_an_integer_is_read_from_every_scalar_that_holds_one(self, value, expected):
        result = self._read(value, "integer")
        assert result == expected
        assert type(result) is int

    @pytest.mark.parametrize(
        "value",
        [
            # Not integral.
            3.5,
            "2.5",
            # Outside a signed 64-bit long, which longValueExact rejects.
            2**63,
            -(2**63) - 1,
            1e20,
            # Not a number at all, or not a finite one.
            "abc",
            "",
            "inf",
            "nan",
            "snan",
            "Infinity",
            float("inf"),
            # A boolean is spelled true / false first, as the JVM spells it,
            # so it is no integer even though Python's bool is an int.
            True,
            False,
        ],
        ids=repr,
    )
    def test_an_integer_rejects_what_the_jvm_rejects(self, value):
        assert self._rejection(value, "integer") == (
            f"the operator refers to loop variable $v, but its value {value!r} "
            "is not an integer"
        )

    @pytest.mark.parametrize(
        "value, expected",
        [
            (0.25, 0.25),
            (0.0, 0.0),
            (3, 3.0),
            ("2.5", 2.5),
            (" 7.5 ", 7.5),
            ("1e-3", 0.001),
            ("-.5", -0.5),
            ("1.", 1.0),
            ("Infinity", math.inf),
            ("-Infinity", -math.inf),
            (float("inf"), math.inf),
            (2**63, float(2**63)),
        ],
        ids=repr,
    )
    def test_a_number_is_read_from_every_scalar_that_holds_one(self, value, expected):
        result = self._read(value, "number")
        assert result == expected
        assert type(result) is float

    @pytest.mark.parametrize("value", ["NaN", "+NaN", float("nan")], ids=repr)
    def test_a_number_is_nan_where_the_jvm_reads_nan(self, value):
        assert math.isnan(self._read(value, "number"))

    @pytest.mark.parametrize(
        "value",
        [
            "abc",
            "",
            "1.2.3",
            True,
            False,
        ],
        ids=repr,
    )
    def test_a_number_rejects_what_the_jvm_rejects(self, value):
        assert self._rejection(value, "number") == (
            f"the operator refers to loop variable $v, but its value {value!r} "
            "is not a number"
        )

    @pytest.mark.parametrize(
        "value, expected",
        [
            (True, True),
            (False, False),
            ("true", True),
            ("false", False),
            # Any case, around any whitespace, as Scala's toBooleanOption.
            ("TRUE", True),
            ("False", False),
            ("tRuE", True),
            (" true ", True),
        ],
        ids=repr,
    )
    def test_a_boolean_is_read_from_every_scalar_that_holds_one(self, value, expected):
        assert self._read(value, "boolean") is expected

    @pytest.mark.parametrize("value", [1, 0, 1.0, "1", "0", "yes", "t", ""], ids=repr)
    def test_a_boolean_rejects_what_the_jvm_rejects(self, value):
        # 1 and 0 are no booleans to the JVM's toBooleanOption, so neither
        # are they here.
        assert self._rejection(value, "boolean") == (
            f"the operator refers to loop variable $v, but its value {value!r} "
            "is not a boolean"
        )

    @pytest.mark.parametrize("kind", ["integer", "number", "boolean"])
    @pytest.mark.parametrize("value", [None, [1, 2], {"p": 2}, b"raw"], ids=repr)
    def test_raises_for_a_value_that_is_not_a_scalar(self, value, kind):
        assert self._rejection(value, kind) == (
            f"the operator refers to loop variable $v, but its value {value!r} "
            "is not a scalar"
        )

    @pytest.mark.parametrize("kind", ["string", "Integer", "int", "double", ""])
    def test_raises_for_a_kind_the_backend_never_writes(self, kind):
        op = _ConcreteOperator()
        op.register_loop_state(State({"v": 3}))
        with pytest.raises(ValueError, match="cannot be read as"):
            op.loop_variable_value("v", kind)

    def test_an_unknown_kind_is_reported_before_the_state_is_read(self):
        # The kind is the generated code's own mistake, so it is reported
        # whether or not a state has arrived.
        with pytest.raises(ValueError, match="cannot be read as 'int'"):
            _ConcreteOperator().loop_variable_value("v", "int")

    @pytest.mark.parametrize("kind", ["integer", "number", "boolean"])
    def test_raises_when_read_before_the_iterations_state_arrived(self, kind):
        with pytest.raises(RuntimeError) as excinfo:
            _ConcreteOperator().loop_variable_value("n", kind)
        assert str(excinfo.value) == (
            "loop variable $n is read before the iteration's state arrived"
        )

    @pytest.mark.parametrize("kind", ["integer", "number", "boolean"])
    def test_raises_for_a_name_no_state_message_carried(self, kind):
        op = _ConcreteOperator()
        op.register_loop_state(State({"i": 1}))
        with pytest.raises(RuntimeError) as excinfo:
            op.loop_variable_value("n", kind)
        assert str(excinfo.value) == (
            "the operator refers to loop variable $n, but no state message carried it"
        )

    def test_a_later_state_message_wins_and_an_earlier_name_is_kept(self):
        op = _ConcreteOperator()
        op.register_loop_state(State({"n": 3, "on": True, "rate": 0.5}))
        op.register_loop_state(State({"n": 5, "centroids": "2"}))
        assert op.loop_variable_value("n", "integer") == 5
        assert op.loop_variable_value("on", "boolean") is True
        assert op.loop_variable_value("rate", "number") == 0.5
        assert op.loop_variable_value("centroids", "integer") == 2

    def test_generated_code_reads_the_iterations_value_at_run_time(self):
        # __name__ so the exec'd class's methods get a real __module__ (the
        # @overrides decorator on the generated methods inspects it).
        namespace: dict = {"__name__": "generated_histogram_operator"}
        exec(_GENERATED_HISTOGRAM_TEMPLATE, namespace)
        op = namespace["ProcessTableOperator"]()

        # Before the iteration's state arrives, the generated call fails loud
        # instead of running on the value the backend generated the code with.
        with pytest.raises(RuntimeError, match="read before the iteration's state"):
            list(op.process_table(Table([Tuple({"a": 1})]), 0))

        op.register_loop_state(State({"n": 3, "alpha": 0.25, "labels": True}))
        (params,) = list(op.process_table(Table([Tuple({"a": 1})]), 0))
        assert params == {"nbinsx": 3, "nbinsy": 3, "opacity": 0.25, "text_auto": True}

        # The next iteration's state reaches the same operator instance, and
        # its values are converted to the kind the code asks for.
        op.register_loop_state(State({"n": "90", "alpha": 2, "labels": "false"}))
        (params,) = list(op.process_table(Table([Tuple({"a": 1})]), 0))
        assert params == {
            "nbinsx": 90,
            "nbinsy": 90,
            "opacity": 2.0,
            "text_auto": False,
        }
        assert [type(v) for v in params.values()] == [int, int, float, bool]


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
