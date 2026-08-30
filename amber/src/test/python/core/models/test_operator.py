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
    SourceOperator,
    State,
    Table,
    Tuple,
    TupleOperatorV2,
)
from core.models.operator import Operator, TableOperator, LoopStartOperator


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

class _ConcreteLoopStart(LoopStartOperator):
    def process_table(self, table, port):
        yield table


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

class TestLoopStartOperator:
    def test_process_state_rejects_overwriting_loop_variable(self):
        op = _ConcreteLoopStart()
        op.state = State({"i": 0})

        with pytest.raises(
            ValueError,
            match=r"Loop state variable\(s\) cannot be overwritten: i",
        ):
            op.process_state(State({"i": 999}), port=0)

        assert op.state["i"] == 0


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
