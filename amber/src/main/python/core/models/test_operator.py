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

    def test_source_operator_class_attr_storage_diverges_from_property_read(
        self,
    ):
        # Documents the underlying defect without claiming a contract: the class
        # attribute is stored under one mangled name, the property reads from
        # another, so they cannot agree. Asserting the two attributes directly
        # decouples this from the eventual is_source-on-SourceOperator fix.
        src = _ConcreteSource()
        # The mangled attribute SourceOperator set — present but unused:
        assert getattr(src, "_SourceOperator__internal_is_source") is True
        # The attribute Operator.is_source actually reads — still the default:
        assert getattr(src, "_Operator__internal_is_source") is False

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Known bug: SourceOperator's __internal_is_source class attribute "
            "is name-mangled to _SourceOperator__internal_is_source, while "
            "Operator.is_source reads _Operator__internal_is_source. The "
            "intended contract is is_source=True on SourceOperator instances; "
            "this xfail flips to XPASS when the bug is fixed."
        ),
    )
    def test_source_operator_subclass_should_report_is_source_true(self):
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
