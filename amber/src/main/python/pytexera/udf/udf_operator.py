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

from abc import abstractmethod
from dataclasses import dataclass
import functools
from typing import Any, Dict, Iterator, Optional, Set, Union

from pyamber import *
from core.models.schema.attribute_type import AttributeType, FROM_STRING_PARSER_MAPPING
from loguru import logger


@dataclass(frozen=True)
class _UiParameterValue:
    name: str
    type: AttributeType
    value: Any


class _UiParameterSupport:
    _ui_parameter_injected_values: Dict[str, Any]
    _ui_parameter_name_types: Dict[str, AttributeType]
    _ui_parameter_used_names: Set[str]
    _unsupported_ui_parameter_types = {
        AttributeType.BINARY,
        AttributeType.LARGE_BINARY,
    }

    # Reserved hook name. Backend injector will generate this in the user's class.
    def _texera_injected_ui_parameters(self) -> Dict[str, Any]:
        return {}

    def _ensure_ui_parameter_state(self) -> None:
        if "_ui_parameter_injected_values" not in self.__dict__:
            self._ui_parameter_injected_values = {}
        if "_ui_parameter_name_types" not in self.__dict__:
            self._ui_parameter_name_types = {}
        if "_ui_parameter_used_names" not in self.__dict__:
            self._ui_parameter_used_names = set()

    def _texera_apply_injected_ui_parameters(self) -> None:
        self._ensure_ui_parameter_state()
        values = self._texera_injected_ui_parameters()
        self._ui_parameter_injected_values = dict(values or {})
        self._ui_parameter_name_types = {}
        self._ui_parameter_used_names = set()

    def _warn_unused_injected_ui_parameters(self) -> None:
        unused_names = sorted(
            set(self._ui_parameter_injected_values) - self._ui_parameter_used_names
        )
        if unused_names:
            logger.warning(
                "Injected UI parameter value(s) were not used: {}.",
                ", ".join(unused_names),
            )

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        original_open = getattr(cls, "open", None)
        if original_open is None:
            return

        if getattr(original_open, "__texera_ui_params_wrapped__", False):
            return

        @functools.wraps(original_open)
        def wrapped_open(self, *args, **kwargs):
            """Apply injected UI parameters once before the outermost open()."""
            if getattr(self, "_ui_parameter_open_in_progress", False):
                return original_open(self, *args, **kwargs)

            self._ui_parameter_open_in_progress = True
            try:
                self._texera_apply_injected_ui_parameters()
                result = original_open(self, *args, **kwargs)
                self._warn_unused_injected_ui_parameters()
                return result
            finally:
                self._ui_parameter_open_in_progress = False

        setattr(wrapped_open, "__texera_ui_params_wrapped__", True)
        cls.open = wrapped_open

    def UiParameter(
        self, name: str, attr_type: Optional[AttributeType] = None, **kwargs: Any
    ) -> _UiParameterValue:
        """
        Return the current UI parameter value parsed as attr_type.

        Re-reading the same name with the same type is idempotent. Reusing a
        name with a different type is rejected because the parsed value would be
        ambiguous.
        """
        if "type" in kwargs:
            if attr_type is not None:
                raise TypeError("UiParameter.type was provided multiple times.")
            attr_type = kwargs.pop("type")

        if kwargs:
            unexpected_arguments = ", ".join(sorted(kwargs))
            raise TypeError(
                f"UiParameter got unexpected keyword argument(s): "
                f"{unexpected_arguments}."
            )

        if attr_type is None:
            raise TypeError("UiParameter.type is required.")

        if not isinstance(attr_type, AttributeType):
            raise TypeError(
                f"UiParameter.type must be an AttributeType, got {attr_type!r}."
            )

        self._ensure_ui_parameter_state()
        existing_type = self._ui_parameter_name_types.get(name)
        if existing_type is not None and existing_type != attr_type:
            raise ValueError(
                f"Duplicate UiParameter name '{name}' with conflicting types: "
                f"{existing_type.name} vs {attr_type.name}."
            )

        self._ui_parameter_name_types[name] = attr_type
        if name in self._ui_parameter_injected_values:
            self._ui_parameter_used_names.add(name)
            raw_value = self._ui_parameter_injected_values[name]
        else:
            logger.warning(
                "No injected UI parameter value found for name '{}'.",
                name,
            )
            raw_value = None

        return _UiParameterValue(
            name=name,
            type=attr_type,
            value=self._parse(raw_value, attr_type),
        )

    @staticmethod
    def _parse(value: Any, attr_type: AttributeType) -> Any:
        if attr_type in _UiParameterSupport._unsupported_ui_parameter_types:
            raise ValueError(
                f"UiParameter does not support {attr_type.name} values. "
                "Use a supported type instead."
            )

        parser = FROM_STRING_PARSER_MAPPING.get(attr_type)
        if parser is None:
            raise TypeError(
                f"UiParameter.type {attr_type!r} is not supported for parsing."
            )

        if value is None:
            return None

        if (
            attr_type is not AttributeType.STRING
            and isinstance(value, str)
            and not value.strip()
        ):
            raise ValueError(
                f"UiParameter value cannot be empty for type {attr_type.name}."
            )

        try:
            return parser(value)
        except Exception as e:
            raise ValueError(
                f"Failed to parse UiParameter value {value!r} as {attr_type.name}. "
                f"Please provide a valid {attr_type.name.lower()} value."
            ) from e


class UDFOperatorV2(_UiParameterSupport, TupleOperatorV2):
    """
    Base class for tuple-oriented user-defined operators. A concrete implementation must
    be provided upon using.
    """

    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    @abstractmethod
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        """
        Process an input Tuple from the given link.

        :param tuple_: Tuple, a Tuple from an input port to be processed.
        :param port: int, input port index of the current Tuple.
        :return: Iterator[Optional[TupleLike]], producing one TupleLike object at a
            time, or None.

        See .examples/ for example operators.
        """
        yield

    def on_finish(self, port: int) -> Iterator[Optional[TupleLike]]:
        """
        Callback when one input port is exhausted.

        :param port: int, input port index of the current exhausted port.
        :return: Iterator[Optional[TupleLike]], producing one TupleLike object at a
            time, or None.
        """
        yield

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass


class UDFSourceOperator(_UiParameterSupport, SourceOperator):
    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    @abstractmethod
    def produce(self) -> Iterator[Optional[Union[TupleLike, TableLike]]]:
        """
        Produce Tuples or Tables. Used by the source operator only.

        :return: Iterator[Union[TupleLike, TableLike, None]], producing
            one TupleLike object, one TableLike object, or None, at a time.
        """
        yield

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass


class UDFTableOperator(_UiParameterSupport, TableOperator):
    """
    Base class for table-oriented user-defined operators. A concrete implementation must
    be provided upon using.
    """

    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    @abstractmethod
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        """
        Process an input Table from the given link. The Table is represented as
        pandas.DataFrame.

        :param table: Table, a table to be processed.
        :param port: int, input index of the current Table.
        :return: Iterator[Optional[TableLike]], producing one TableLike object at a
            time, or None.
        """
        yield

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass


class UDFBatchOperator(_UiParameterSupport, BatchOperator):
    """
    Base class for batch-oriented user-defined operators. A concrete implementation must
    be provided upon using.
    """

    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    @abstractmethod
    def process_batch(self, batch: Batch, port: int) -> Iterator[Optional[BatchLike]]:
        """
        Process an input Batch from the given link. The Batch is represented as
        pandas.DataFrame.

        :param batch: Batch, a batch to be processed.
        :param port: int, input index of the current Batch.
        :return: Iterator[Optional[BatchLike]], producing one BatchLike object at a
            time, or None.
        """
        yield

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass
