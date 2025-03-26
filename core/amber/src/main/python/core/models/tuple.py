import ctypes
import struct
import typing
from collections import OrderedDict
from copy import deepcopy
from typing import Any, List, Iterator, Callable

from typing_extensions import Protocol, runtime_checkable
import pandas
import pickle
import pyarrow
from loguru import logger
from pandas._libs.missing import checknull
from pympler import asizeof

from .schema.attribute_type import TO_PYOBJECT_MAPPING, AttributeType
from .schema.field import Field
from .schema.schema import Schema


@runtime_checkable
class TupleLike(Protocol):
    def __getitem__(self, item: typing.Union[str, int]) -> Field: ...

    def __setitem__(self, key: typing.Union[str, int], value: Field) -> None: ...


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
        if self._table.num_columns == 0:
            # empty table
            raise StopIteration
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

            # Handle ListType<Binary> as BINARY type (List[ByteBuffer] from Scala)
            elif (
                isinstance(field_type, pyarrow.LargeListType)
            ) and field_type.value_type == pyarrow.binary():
                # value is already a list of bytes objects, which is correct
                # Handle special case for pickled list items
                if value and isinstance(value[0], bytes) and value[0][:6] == b"pickle":
                    value = [pickle.loads(item[10:]) for item in value]

            return value

        self._current_idx += 1
        return field_accessor


def double_to_long(value: float) -> int:
    """
    Convert a double value into a long value.
    :param value: A double (Python float) value.
    :return: The converted long (Python int) value.
    """
    # Pack the double value into a binary string of 8 bytes
    packed_value = struct.pack("d", value)
    # Unpack the binary string to a 64-bit integer (int in Python 3)
    long_value = struct.unpack("Q", packed_value)[0]
    return long_value


def int_32(value: int) -> int:
    """
    Convert a Python int (unbounded) to a 32-bit int with overflow.
    :param value: A Python int value.
    :return: The converted 32-bit integer, with overflow.
    """
    return ctypes.c_int32(value).value


def java_hash_bool(value: bool) -> int:
    """
    Java's hash function for a boolean value.
    :param value: A boolean value.
    :return: Java's hash value in a 32-bit integer.
    """
    return 1231 if value else 1237


def java_hash_long(value: int) -> int:
    """
    Java's hash function for a long value.
    :param value: A long (Python int) value.
    :return: Java's hash value in a 32-bit integer.
    """
    return int_32(value ^ (value >> 32))


def java_hash_bytes(
    byte_data: typing.Union[List[typing.Union[bytes, bytearray]], Iterator[int]],
    init: int,
    salt: int,
):
    """
    Java's hash function for an array of bytes.
    :param byte_data: Either a list of bytes/bytearray objects
                      or iterator of int (byte) values.
    :param init: An init hash value.
    :param salt: A hash salt value.
    :return: Java's hash value in a 32-bit integer.
    """
    hash_value = init

    # Case 1: Input is a non-empty list of bytes/bytearray objects
    if (
        isinstance(byte_data, list)
        and byte_data
        and isinstance(byte_data[0], (bytes, bytearray))
    ):
        for byte_obj in byte_data:
            for byte in byte_obj:
                hash_value = int_32(salt * hash_value + byte)
    # Case 2: Input is an iterator of integers or other iterable
    else:
        for byte in byte_data:
            hash_value = int_32(salt * hash_value + byte)

    return hash_value


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
        self._field_data: "OrderedDict[str, Field]"
        if isinstance(tuple_like, Tuple):
            self._field_data = tuple_like._field_data
        elif isinstance(tuple_like, pandas.Series):
            self._field_data = OrderedDict(tuple_like.to_dict())
        else:
            self._field_data = OrderedDict(tuple_like) if tuple_like else OrderedDict()
        self._schema: typing.Optional[Schema] = schema

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

    def as_dict(self) -> "OrderedDict[str, Field]":
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

    def get_fields(self, output_field_names=None) -> typing.Tuple[Field, ...]:
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
        assert self._schema is None
        self.cast_to_schema(schema)
        self.validate_schema(schema)
        self._schema = schema

    def cast_to_schema(self, schema: Schema) -> None:
        """
        Safely cast each field value to match the target schema.

        This currently conducts two kinds of casts:
            1. cast NaN to None
            2. cast any object to bytes (using pickle)

        Args:
            schema: The target Schema that describes the AttributeType to cast to
        """
        MAX_CHUNK_SIZE = 1 * 1024 * 1024 * 1024  # 1GB in bytes
        PICKLE_PREFIX = b"pickle    "

        for field_name in self.get_field_names():
            try:
                field_value: Field = self[field_name]

                # Convert NaN to None to support null value conversion
                if checknull(field_value):
                    self[field_name] = None
                    continue

                if field_value is None:
                    continue

                field_type = schema.get_attr_type(field_name)

                # Handle BINARY type - expected to be a list of byte arrays
                if field_type == AttributeType.BINARY:
                    self._process_binary_field(
                        field_name, field_value, MAX_CHUNK_SIZE, PICKLE_PREFIX
                    )

            except Exception as err:
                # Suppress exceptions during cast.
                # Keep the value as it is if the cast fails,
                # and continue to the next one.
                logger.warning(f"Failed to cast field '{field_name}': {err}")

    def _process_binary_field(
        self,
        field_name: str,
        field_value: Any,
        max_chunk_size: int,
        pickle_prefix: bytes,
    ) -> None:
        """
        Process a field with BINARY type, converting it to the expected format.

        Args:
            field_name: Name of the field being processed
            field_value: Value to convert to binary format
            max_chunk_size: Maximum size of each binary chunk
            pickle_prefix: Prefix to add to pickled data
        """
        # If it's already a list, make sure all items are bytes
        if isinstance(field_value, list):
            self[field_name] = [
                item if isinstance(item, bytes) else bytes(item) for item in field_value
            ]
        # If it's a single bytes object, convert to list with one item
        elif isinstance(field_value, bytes):
            if len(field_value) > max_chunk_size:
                self[field_name] = [
                    field_value[i : i + max_chunk_size]
                    for i in range(0, len(field_value), max_chunk_size)
                ]
            else:
                self[field_name] = [field_value]
        # For other types, pickle them
        else:
            pickled_data = pickle.dumps(field_value)

            if len(pickled_data) > max_chunk_size:
                self[field_name] = [
                    pickle_prefix + pickled_data[i : i + max_chunk_size]
                    for i in range(0, len(pickled_data), max_chunk_size)
                ]
            else:
                self[field_name] = [pickle_prefix + pickled_data]

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
            expected_type = TO_PYOBJECT_MAPPING.get(expected)

            if field_value is None:
                continue

            # Special handling for BINARY type
            if expected == AttributeType.BINARY:
                if not isinstance(field_value, list):
                    raise TypeError(
                        f"Type mismatch for field '{field_name}': "
                        f"expected {expected} (list), "
                        f"got {type(field_value).__name__} instead."
                    )

                # Verify all items are bytes objects
                non_bytes_items = [
                    (i, type(item).__name__)
                    for i, item in enumerate(field_value)
                    if not isinstance(item, bytes)
                ]

                if non_bytes_items:
                    index, type_name = non_bytes_items[0]  # Report first error
                    raise TypeError(
                        f"Type mismatch in BINARY list field '{field_name}' "
                        f"at index {index}: "
                        f"expected 'bytes', got '{type_name}' instead."
                    )
            # For other types, use the standard type check
            elif not isinstance(field_value, expected_type):
                raise TypeError(
                    f"Type mismatch for field '{field_name}': "
                    f"expected {expected} ({expected_type.__name__}), "
                    f"got {type(field_value).__name__} instead."
                )

    def get_partial_tuple(self, attribute_names: List[str]) -> "Tuple":
        """
        Creates a partial Tuple with fields specified by the attribute names.

        :param attribute_names: A list of attribute names for which to create the
                                partial tuple.
        :return: A new Tuple instance containing only the specified fields,
                preserving the order specified by the attribute names.
        """
        assert self._schema is not None
        schema = self._schema.get_partial_schema(attribute_names)
        new_raw_tuple = OrderedDict()
        for name in attribute_names:
            new_raw_tuple[name] = self[name]
        return Tuple(new_raw_tuple, schema=schema)

    def __iter__(self) -> Iterator[Field]:
        return iter(self.get_fields())

    def __str__(self) -> str:
        content = ", ".join(
            [repr(key) + ": " + repr(value) for key, value in self.as_key_value_pairs()]
        )
        return f"Tuple[{content}]"

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

    def __hash__(self) -> int:
        """
        Aligned with Java's built-in hash algorithm implementation described in
        _Josh Bloch's Effective Java_.
        This algorithm is taken by
         - Built-in Java (java.util.Objects.hash)
         - Guava (com.google.common.base.Objects.hashCode)
        :return: A 32-bit integer value.
        """
        result = 1
        salt = 31  # for ease of optimization

        mapping = {
            AttributeType.BOOL: lambda f: java_hash_bool(f),
            AttributeType.INT: lambda f: int_32(f),
            AttributeType.LONG: lambda f: java_hash_long(f),
            AttributeType.DOUBLE: lambda f: java_hash_long(double_to_long(f)),
            AttributeType.STRING: lambda f: java_hash_bytes(map(ord, f), 0, salt),
            AttributeType.TIMESTAMP: lambda f: java_hash_long(int(f.timestamp())),
            AttributeType.BINARY: lambda f: java_hash_bytes(f, 1, salt),
        }

        for name, field in self.as_key_value_pairs():
            attr_type = self._schema.get_attr_type(name)
            if field is None:
                hash_value = 0
            else:
                hash_value = mapping[attr_type](field)
            result = result * salt + hash_value

        return int_32(result)

    def in_mem_size(self) -> int:
        """
        Calculate the in-memory size of the Tuple instance.
        :return: The size in bytes.
        """
        return asizeof.asizeof(self)
