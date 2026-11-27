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

import datetime
from typing import Iterator, Optional

import pandas
import pytest
import pytexera.udf.udf_operator as udf_operator

from pytexera import AttributeType, Resource, Tuple, TupleLike, UDFOperatorV2
from pytexera import UDFBatchOperator, UDFSourceOperator, UDFTableOperator
from pytexera.udf.udf_operator import _UiParameterSupport


class _PassthroughUdfOperator(UDFOperatorV2):
    injected_params: dict = {}

    def _texera_injected_ui_parameters(self):
        return dict(self.injected_params)

    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield tuple_


def _make_param_op(injected: dict, *specs, name: str = "ParamOperator"):
    attrs = {"injected_params": injected}
    if specs:

        def open(self):
            for attr_name, param_args, param_kwargs in specs:
                param = self.UiParameter(*param_args, **param_kwargs)
                if attr_name:
                    setattr(self, attr_name, param)

        attrs["open"] = open
    return type(name, (_PassthroughUdfOperator,), attrs)


def _capture_warnings(monkeypatch) -> list[str]:
    warning_calls = []
    monkeypatch.setattr(
        udf_operator.logger,
        "warning",
        lambda msg, *args, **kwargs: warning_calls.append(msg.format(*args)),
    )
    return warning_calls


class SuperOpenParameterOperator(_PassthroughUdfOperator):
    def __init__(self):
        self.hook_call_count = 0

    def _texera_injected_ui_parameters(self):
        self.hook_call_count += 1
        return {"count": "3"}

    def open(self):
        super().open()
        self.count_parameter = self.UiParameter("count", AttributeType.INT)


class SuperOpenConflictingParameterOperator(_PassthroughUdfOperator):
    injected_params = {"duplicate": "1"}

    def open(self):
        self.UiParameter("duplicate", AttributeType.INT)
        super().open()
        self.UiParameter("duplicate", AttributeType.STRING)


InvalidValueParameterOperator = _make_param_op(
    {"count": "not-a-number"},
    ("count_parameter", ("count", AttributeType.INT), {}),
    name="InvalidValueParameterOperator",
)


class NoOpenParameterSupport(_UiParameterSupport):
    """A _UiParameterSupport subclass that defines no open() hook.

    __init_subclass__ has nothing to wrap here and must bail out instead
    of installing a wrapper around a non-existent attribute.
    """


class BaseHookTupleOperator(UDFOperatorV2):
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield from super().process_tuple(tuple_, port)


class BaseHookSourceOperator(UDFSourceOperator):
    def produce(self):
        yield from super().produce()


class BaseHookTableOperator(UDFTableOperator):
    def process_table(self, table, port: int):
        yield from super().process_table(table, port)


class BaseHookBatchOperator(UDFBatchOperator):
    def process_batch(self, batch, port: int):
        yield from super().process_batch(batch, port)


class TestUiParameterSupport:
    def test_injected_values_are_applied_before_open(self):
        operator = _make_param_op(
            {
                "count": "7",
                "enabled": "1",
                "created_at": "2024-01-01T00:00:00",
            },
            ("count_parameter", ("count", AttributeType.INT), {}),
            ("enabled_parameter", (), {"name": "enabled", "type": AttributeType.BOOL}),
            (
                "created_at_parameter",
                ("created_at",),
                {"type": AttributeType.TIMESTAMP},
            ),
        )()

        operator.open()

        assert operator.count_parameter.value == 7
        assert operator.enabled_parameter.value is True
        assert operator.created_at_parameter.value == datetime.datetime(
            2024, 1, 1, 0, 0
        )

    def test_duplicate_parameter_names_with_conflicting_types_raise(self):
        operator = _make_param_op(
            {"duplicate": "1"},
            (None, ("duplicate", AttributeType.INT), {}),
            (None, ("duplicate", AttributeType.STRING), {}),
        )()

        with pytest.raises(ValueError) as exc_info:
            operator.open()

        assert "Duplicate UiParameter name 'duplicate'" in str(exc_info.value)

    def test_duplicate_parameter_names_with_same_type_succeed(self):
        operator = _make_param_op(
            {"duplicate": "1"},
            ("first_parameter", ("duplicate", AttributeType.INT), {}),
            ("second_parameter", ("duplicate", AttributeType.INT), {}),
        )()

        operator.open()

        assert operator.first_parameter.value == 1
        assert operator.second_parameter.value == 1
        assert operator.first_parameter.type is AttributeType.INT
        assert operator.second_parameter.type is AttributeType.INT

    @pytest.mark.parametrize(
        ("raw_value", "attr_type", "expected"),
        [
            ("hello", AttributeType.STRING, "hello"),
            ("7", AttributeType.INT, 7),
            ("99", AttributeType.LONG, 99),
            ("3.14", AttributeType.DOUBLE, 3.14),
            ("1", AttributeType.BOOL, True),
            (
                "2024-01-01T00:00:00",
                AttributeType.TIMESTAMP,
                datetime.datetime(2024, 1, 1, 0, 0),
            ),
        ],
    )
    def test_parse_supported_types(self, raw_value, attr_type, expected):
        assert _UiParameterSupport._parse(raw_value, attr_type) == expected

    @pytest.mark.parametrize(
        ("raw_value", "attr_type"),
        [
            ("", AttributeType.INT),
            ("   ", AttributeType.LONG),
            ("", AttributeType.DOUBLE),
            ("\t", AttributeType.BOOL),
            ("", AttributeType.TIMESTAMP),
        ],
    )
    def test_parse_empty_non_string_values_raises_value_error(
        self, raw_value, attr_type
    ):
        with pytest.raises(ValueError, match="UiParameter value cannot be empty"):
            _UiParameterSupport._parse(raw_value, attr_type)

    def test_parse_empty_string_value(self):
        assert _UiParameterSupport._parse("", AttributeType.STRING) == ""

    def test_java_attribute_type_aliases_parse_like_python_names(self):
        assert AttributeType.INTEGER is AttributeType.INT
        assert AttributeType.BOOLEAN is AttributeType.BOOL
        assert _UiParameterSupport._parse("7", AttributeType.INTEGER) == 7
        assert _UiParameterSupport._parse("false", AttributeType.BOOLEAN) is False

    @pytest.mark.parametrize(
        ("raw_value", "expected"),
        [
            ("True", True),
            ("true", True),
            ("1", True),
            ("1.0", True),
            ("2", True),
            ("-1", True),
            ("False", False),
            ("false", False),
            ("0", False),
            ("0.0", False),
        ],
    )
    def test_parse_bool_string_values(self, raw_value, expected):
        assert _UiParameterSupport._parse(raw_value, AttributeType.BOOL) is expected

    @pytest.mark.parametrize(
        ("raw_value", "attr_type", "expected_message"),
        [
            (
                "payload",
                AttributeType.BINARY,
                "UiParameter does not support BINARY values",
            ),
            (
                "s3://bucket/path/to/object",
                AttributeType.LARGE_BINARY,
                "UiParameter does not support LARGE_BINARY values",
            ),
            (
                None,
                AttributeType.BINARY,
                "UiParameter does not support BINARY values",
            ),
        ],
    )
    def test_parse_binary_types_raise_helpful_error(
        self, raw_value, attr_type, expected_message
    ):
        with pytest.raises(ValueError, match=expected_message):
            _UiParameterSupport._parse(raw_value, attr_type)

    def test_parse_unsupported_type_raises_helpful_error(self):
        with pytest.raises(TypeError, match="UiParameter.type .* is not supported"):
            _UiParameterSupport._parse("value", object())

    def test_missing_injected_name_returns_none_and_warns(self, monkeypatch):
        operator = _make_param_op(
            {"sent": "1"},
            ("missing_parameter", ("missing", AttributeType.INT), {}),
        )()
        warning_calls = _capture_warnings(monkeypatch)

        operator.open()

        assert operator.missing_parameter.value is None
        assert any(
            "No injected UI parameter value found for name 'missing'" in call
            for call in warning_calls
        )

    def test_unused_injected_name_warns(self, monkeypatch):
        operator = _make_param_op(
            {"used": "1", "unused": "2"},
            ("used_parameter", ("used", AttributeType.INT), {}),
        )()
        warning_calls = _capture_warnings(monkeypatch)

        operator.open()

        assert operator.used_parameter.value == 1
        assert warning_calls == [
            "Injected UI parameter value(s) were not used: unused."
        ]

    def test_ui_parameter_argument_errors(self):
        operator = _PassthroughUdfOperator()

        with pytest.raises(TypeError, match="provided multiple times"):
            operator.UiParameter("count", AttributeType.INT, type=AttributeType.INT)
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            operator.UiParameter("count", AttributeType.INT, default="1")
        with pytest.raises(TypeError, match="UiParameter.type is required"):
            operator.UiParameter("count")
        with pytest.raises(TypeError, match="must be an AttributeType"):
            operator.UiParameter("count", object())

    def test_resource_parameter_receives_its_mount_directory(self):
        operator = _make_param_op(
            {"MODEL": "/mnt/texera-mounts/model-1/abc123"},
            (
                "model_parameter",
                ("MODEL", AttributeType.STRING),
                {"value": Resource.MODEL},
            ),
        )()

        operator.open()

        assert operator.model_parameter.value == "/mnt/texera-mounts/model-1/abc123"
        assert operator.model_parameter.resource is Resource.MODEL

    def test_resource_parameter_rejects_anything_but_a_resource(self):
        operator = _PassthroughUdfOperator()

        with pytest.raises(TypeError, match="must be a Resource"):
            operator.UiParameter("MODEL", AttributeType.STRING, value="model")

    def test_resource_parameter_must_be_a_string(self):
        operator = _PassthroughUdfOperator()

        # What the UDF receives is a directory path.
        with pytest.raises(TypeError, match="must be AttributeType.STRING"):
            operator.UiParameter("MODEL", AttributeType.INT, value=Resource.DATASET)

    def test_resource_kinds_match_the_frontend_and_backend(self):
        assert {resource.value for resource in Resource} == {"model", "dataset"}

    def test_super_open_applies_injected_values_once(self):
        operator = SuperOpenParameterOperator()

        operator.open()

        assert operator.hook_call_count == 1
        assert operator.count_parameter.value == 3

    def test_super_open_does_not_reset_duplicate_tracking(self):
        operator = SuperOpenConflictingParameterOperator()

        with pytest.raises(ValueError, match="Duplicate UiParameter name"):
            operator.open()

    def test_wrapped_open_uses_instance_local_state(self):
        count_spec = ("count_parameter", ("count", AttributeType.INT), {})
        first_cls = _make_param_op({"count": "1"}, count_spec, name="FirstOp")
        second_cls = _make_param_op({"count": "2"}, count_spec, name="SecondOp")
        assert getattr(first_cls.open, "__texera_ui_params_wrapped__", False) is True

        first_operator = first_cls()
        second_operator = second_cls()

        first_operator.open()
        second_operator.open()

        assert first_operator.count_parameter.value == 1
        assert second_operator.count_parameter.value == 2
        assert first_operator._ui_parameter_injected_values == {"count": "1"}
        assert second_operator._ui_parameter_injected_values == {"count": "2"}
        assert (
            first_operator._ui_parameter_injected_values
            is not second_operator._ui_parameter_injected_values
        )

    def test_subclass_without_open_is_left_unwrapped(self):
        # Not every _UiParameterSupport subclass declares open(); the
        # wrapper installation must be skipped rather than fabricating an
        # open() attribute on such a class.
        assert getattr(NoOpenParameterSupport, "open", None) is None

        support = NoOpenParameterSupport()
        parameter = support.UiParameter("absent", AttributeType.STRING)

        # UiParameter still works: the state is lazily initialised, and an
        # un-injected name resolves to None.
        assert parameter.value is None
        assert support._ui_parameter_injected_values == {}
        assert support._ui_parameter_name_types == {"absent": AttributeType.STRING}

    @pytest.mark.parametrize(
        ("raw_value", "attr_type"),
        [
            ("not-a-number", AttributeType.INT),
            ("not-a-number", AttributeType.LONG),
            ("12.5.6", AttributeType.DOUBLE),
            ("not-a-timestamp", AttributeType.TIMESTAMP),
        ],
    )
    def test_parse_unparseable_value_raises_value_error(self, raw_value, attr_type):
        with pytest.raises(ValueError) as exc_info:
            _UiParameterSupport._parse(raw_value, attr_type)

        message = str(exc_info.value)
        assert f"Failed to parse UiParameter value {raw_value!r}" in message
        assert f"as {attr_type.name}" in message
        assert f"valid {attr_type.name.lower()} value" in message
        # The underlying parser error is chained, not swallowed.
        assert exc_info.value.__cause__ is not None

    def test_unparseable_injected_value_fails_at_open(self):
        operator = InvalidValueParameterOperator()

        with pytest.raises(ValueError, match="Failed to parse UiParameter value"):
            operator.open()

    def test_wrapped_open_resets_the_in_progress_flag_after_a_failure(self):
        # The flag is cleared in a finally block, so a second open() after
        # a failing one must re-apply the injected values (rather than take
        # the "already in progress" shortcut and skip injection).
        operator = InvalidValueParameterOperator()
        with pytest.raises(ValueError):
            operator.open()

        assert operator._ui_parameter_open_in_progress is False

        with pytest.raises(ValueError):
            operator.open()


class TestBaseOperatorHooks:
    """Every UDF base class ships a default generator body so a user
    operator can call super() (or leave an optional hook alone) and simply
    produce nothing. Each default must yield exactly one None, which the
    runtime filters out -- returning a non-generator would break the
    caller's `for ... in` loop instead.
    """

    def test_tuple_operator_defaults_yield_nothing_useful(self):
        operator = BaseHookTupleOperator()

        assert operator.open() is None
        assert list(operator.process_tuple(Tuple({"x": 1}), 0)) == [None]
        assert list(operator.on_finish(0)) == [None]
        assert operator.close() is None

    def test_source_operator_defaults_yield_nothing_useful(self):
        operator = BaseHookSourceOperator()

        assert operator.open() is None
        assert list(operator.produce()) == [None]
        assert operator.close() is None

    def test_table_operator_defaults_yield_nothing_useful(self):
        operator = BaseHookTableOperator()
        table = pandas.DataFrame({"x": [1, 2]})

        assert operator.open() is None
        assert list(operator.process_table(table, 0)) == [None]
        assert operator.close() is None

    def test_batch_operator_defaults_yield_nothing_useful(self):
        operator = BaseHookBatchOperator()
        batch = pandas.DataFrame({"x": [1, 2]})

        assert operator.open() is None
        assert list(operator.process_batch(batch, 0)) == [None]
        assert operator.close() is None

    @pytest.mark.parametrize(
        ("operator_class", "hook_name"),
        [
            (UDFOperatorV2, "process_tuple"),
            (UDFSourceOperator, "produce"),
            (UDFTableOperator, "process_table"),
            (UDFBatchOperator, "process_batch"),
        ],
    )
    def test_processing_hook_is_abstract(self, operator_class, hook_name):
        # The default body exists for super() delegation only -- a user
        # operator must still implement the hook.
        assert hook_name in operator_class.__abstractmethods__
