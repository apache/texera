import ctypes
import struct
import typing
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, List, Iterator, Dict, Callable

from typing_extensions import Protocol, runtime_checkable
import pandas
import pickle
import pyarrow
from loguru import logger
from pandas._libs.missing import checknull

from .schema.attribute_type import TO_PYOBJECT_MAPPING, AttributeType
from .schema.field import Field
from .schema.schema import Schema


@runtime_checkable
class TupleLike(Protocol):
    def __getitem__(self, item: typing.Union[str, int]) -> Field:
        ...

    def __setitem__(self, key: typing.Union[str, int], value: Field) -> None:
        ...


@dataclass
class InputExhausted:
    pass


class ArrowTableTupleProvider:
    """
    This class provides "view"s for tuple from a pyarrow.Table.
    """

    def __init__(self, table: pyarrow.Table):
        """
        Construct a provider from a pyarrow.Table.
        Keep the current chunk and tuple idx as its state.
        """
        self._table = table
        self._current_idx = 0
        self._current_chunk = 0

    def __iter__(self) -> Iterator[Callable]:
        """
        Return itself as it is iterable.
        """
        return self

    def __next__(self) -> Callable:
        """
        Provide the field accessor of the next tuple.
        If current chunk is exhausted, move to the first tuple of the next chunk.
        """
        if self._current_idx >= len(self._table.column(0).chunks[self._current_chunk]):
            self._current_idx = 0
            self._current_chunk += 1
            if self._current_chunk >= self._table.column(0).num_chunks:
                raise StopIteration

        chunk_idx = self._current_chunk
        tuple_idx = self._current_idx

        def field_accessor(field_name: str) -> Field:
            """
            Retrieve the field value by a given field name.
            This abstracts and hides the underlying implementation of the tuple data
            storage from the user.
            """
            value = self._table.column(field_name).chunks[chunk_idx][tuple_idx].as_py()
            field_type = self._table.schema.field(field_name).type

            # for binary types, convert pickled objects back.
            if (
                    field_type == pyarrow.binary()
                    and value is not None
                    and value[:6] == b"pickle"
            ):
                value = pickle.loads(value[10:])
            return value

        self._current_idx += 1
        return field_accessor


class Tuple:
    """
    Lazy-Tuple implementation.
    """

    def __init__(
            self,
            tuple_like: typing.Optional["TupleLike"] = None,
            schema: typing.Optional[Schema] = None,
    ):
        """
        Construct a lazy-tuple with given TupleLike object. If the field value is a
        accessor callable, the actual value is fetched upon first reference.

        :param tuple_like: in which the field value could be the actual value in
            memory, or a callable accessor.
        """
        assert len(tuple_like) != 0
        if isinstance(tuple_like, Tuple):
            self._field_data = tuple_like._field_data
        elif isinstance(tuple_like, pandas.Series):
            self._field_data = OrderedDict(tuple_like.to_dict())
        else:
            self._field_data = OrderedDict(tuple_like) if tuple_like else OrderedDict()
        self._schema: typing.Optional[Schema] = schema
        if self._schema:
            self.finalize(schema)

    def __getitem__(self, item: typing.Union[int, str]) -> Field:
        """
        Get a field value with given item. If the value is an accessor, fetch it from
        the accessor.

        :param item: field name or field index
        :return: field value
        """
        assert isinstance(
            item, (int, str)
        ), "field can only be retrieved by index or name"

        if isinstance(item, int):
            item: str = self.get_field_names()[item]

        if (
                callable(self._field_data[item])
                and getattr(self._field_data[item], "__name__", "Unknown")
                == "field_accessor"
        ):
            # evaluate the field now
            field_accessor = self._field_data[item]
            self._field_data[item] = field_accessor(field_name=item)
        return self._field_data[item]

    def __setitem__(self, field_name: str, field_value: Field) -> None:
        """
        Set a field with the given value.
        :param field_name
        :param field_value
        """
        assert isinstance(field_name, str), "field can only be set by name"
        assert not callable(field_value), "field cannot be of type callable"
        self._field_data[field_name] = field_value

    def as_series(self) -> pandas.Series:
        """Convert the tuple to Pandas series format"""
        return pandas.Series(self.as_dict())

    def as_dict(self) -> OrderedDict[str, Field]:
        """
        Return a dictionary copy of this tuple.
        Fields will be fetched from accessor if absent.
        :return: dict with all the fields
        """
        # evaluate all the fields now
        for i in self.get_field_names():
            self.__getitem__(i)
        return deepcopy(self._field_data)

    def as_key_value_pairs(self) -> List[typing.Tuple[str, Field]]:
        return [(k, v) for k, v in self.as_dict().items()]

    def get_field_names(self) -> typing.Tuple[str]:
        return tuple(map(str, self._field_data.keys()))

    def get_fields(self, output_field_names=None) -> typing.Tuple[Field]:
        """
        Get values from tuple for selected fields.
        """
        if output_field_names is None:
            output_field_names = self.get_field_names()
        return tuple(self[i] for i in output_field_names)

    def finalize(self, schema: Schema) -> None:
        """
        Finalizes a Tuple by adding a schema to it. This convert all Fields into the
        AttributeType defined in the Schema and make the Tuple immutable.

        A Tuple can have no Schema initially. The types of Fields are not restricted.
        This is to provide the maximum flexibility for users to construct Tuples as
        they wish. When a Schema is added, the Tuple is finalized to match the Schema.

        :param schema: target Schema to finalize the Tuple.
        :return:
        """
        self.cast_to_schema(schema)
        self.validate_schema(schema)
        self._schema = schema

    def cast_to_schema(self, schema: Schema) -> None:
        """
        Safely cast each field value to match the target schema.
        If failed, the value will stay not changed.

        This current conducts two kinds of casts:
            1. cast NaN to None;
            2. cast any object to bytes (using pickle).
        :param schema: The target Schema that describes the target AttributeType to
            cast.
        :return:
        """
        for field_name in self.get_field_names():
            try:
                field_value: Field = self[field_name]

                # convert NaN to None to support null value conversion
                if checknull(field_value):
                    self[field_name] = None

                if field_value is not None:
                    field_type = schema.get_attr_type(field_name)
                    if field_type == AttributeType.BINARY:
                        self[field_name] = b"pickle    " + pickle.dumps(field_value)
            except Exception as err:
                # Surpass exceptions during cast.
                # Keep the value as it is if the cast fails, and continue to attempt
                # on the next one.
                logger.warning(err)
                continue

    def validate_schema(self, schema: Schema) -> None:
        """
        Checks if the field values in the Tuple matches the expected Schema.
        :param schema: Schema
        :return:
        """

        schema_fields = schema.get_attr_names()
        tuple_fields = self.get_field_names()
        expected_but_missing = set(schema_fields) - set(tuple_fields)
        unexpected = set(tuple_fields) - set(schema_fields)
        if expected_but_missing:
            raise KeyError(
                f"field{'' if len(expected_but_missing) == 1 else 's'} "
                f"{', '.join(map(repr, expected_but_missing))} "
                f"{'is' if len(expected_but_missing) == 1 else 'are'} "
                f"expected but missing in the {self}."
            )

        if unexpected:
            raise KeyError(
                f"{self} contains {'an' if len(unexpected) == 1 else ''} unexpected "
                f"field{'' if len(unexpected) == 1 else 's'}: "
                f"{', '.join(map(repr, unexpected))}."
            )

        for field_name, field_value in self.as_key_value_pairs():
            expected = schema.get_attr_type(field_name)
            if not isinstance(
                    field_value, (TO_PYOBJECT_MAPPING.get(expected), type(None))
            ):
                raise TypeError(
                    f"Unmatched type for field '{field_name}', expected {expected}, "
                    f"got {field_value} ({type(field_value)}) instead."
                )

    def __iter__(self) -> Iterator[Field]:
        return iter(self.get_fields())

    def __str__(self) -> str:
        return f"Tuple[{str(self.as_dict()).strip('{').strip('}')}]"

    __repr__ = __str__

    def __eq__(self, other: Any) -> bool:
        return (
                isinstance(other, Tuple)
                and self.get_field_names() == other.get_field_names()
                and all(self[i] == other[i] for i in self.get_field_names())
        )

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def __len__(self) -> int:
        return len(self._field_data)

    def __contains__(self, __x: object) -> bool:
        return __x in self._field_data

    def get_partial_tuple(self, indices) -> "Tuple":
        assert self._schema is not None
        schema = self._schema.get_partial_schema(indices)
        new_raw_tuple = OrderedDict()
        for index, (key, value) in enumerate(self.as_key_value_pairs(), 0):
            if index in indices:
                new_raw_tuple[key] = value
        return Tuple(new_raw_tuple, schema=schema)

    def __hash__(self):
        result = 17
        salt = 31
        for name, field in self.as_key_value_pairs():
            attr_type = self._schema.get_attr_type(name)
            for c in name:
                result = result * salt + ord(c)

            if attr_type == AttributeType.BOOL:
                result = result * salt + int(field)
            elif attr_type == AttributeType.INT:
                result = result * salt + field
            elif attr_type == AttributeType.LONG:
                result = result * salt + field ^ (field >> 32)
            elif attr_type == AttributeType.DOUBLE:

                def double_to_long_bits(double_value):
                    # Pack the double value into a binary string of 8 bytes
                    packed_value = struct.pack("d", double_value)

                    # Unpack the binary string to a 64-bit integer (long in Python 2, int in Python 3)
                    long_bits = struct.unpack("Q", packed_value)[0]

                    # If you need a signed long (for Python 2 or Python 3)
                    # signed_long_bits = ctypes.c_longlong(long_bits).value

                    return long_bits

                long_value = double_to_long_bits(field)
                result = result * salt + long_value ^ (long_value >> 32)
            elif attr_type == AttributeType.STRING:
                for c in field:
                    result = result * salt + ord(c)
            elif attr_type == AttributeType.TIMESTAMP:
                long_value = field.time()
                result = result * salt + long_value ^ (long_value >> 32)
            elif attr_type == AttributeType.BINARY:
                for b in field:
                    result = result * salt + ord(b)

        def java_int(value):
            return ctypes.c_int32(value).value

        return java_int(result)
