import datetime
import typing
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, List, Mapping, TypeVar

import pandas

AttributeType = TypeVar('AttributeType', int, float, str, datetime.datetime)

TupleLike = TypeVar('TupleLike', pandas.Series, List[typing.Tuple[str, AttributeType]], Mapping[str, AttributeType])


@dataclass
class InputExhausted:
    pass


class ArrowTableTupleProvider:
    """
    This class provides "view"s for tuple from an arrow table
    """

    def __init__(self, table):
        """
        Construct a provider from an arrow table.
        Keep the current chunk and tuple idx as its state.
        """
        self._table = table
        self._current_idx = 0
        self._current_chunk = 0

    def __iter__(self):
        """
        return itself as it is iterable.
        """
        return self

    def __next__(self):
        """
        provide the field accessor of the next tuple.
        If current chunk is exhausted, move to the first
        tuple of the next chunk.
        """
        if self._current_idx >= len(self._table.column(0).chunks[self._current_chunk]):
            self._current_idx = 0
            self._current_chunk += 1
            if self._current_chunk >= self._table.column(0).num_chunks:
                raise StopIteration

        chunk_idx = self._current_chunk
        tuple_idx = self._current_idx

        def field_accessor(field_name):
            """
            retrieve the field value by a given field name.
            This abstracts and hides the underlying implementation
            of the tuple data storage from the user.
            """
            return self._table.column(field_name).chunks[chunk_idx][tuple_idx]

        self._current_idx += 1
        return field_accessor


class Tuple:
    """
    Lazy-Tuple implementation.
    """

    def __init__(self, field_data: TupleLike = None, field_names=None, field_accessor=None):
        """
        Construct a lazy-tuple with given data
        :param field_data: tuple-like data that is already in memory
        :param field_names: all field names that can be fetched
        :param field_accessor: a lambda function that fetches a field with given field name
        """
        self.__field_accessor = field_accessor
        self.__field_names = field_names
        if field_data is None:
            self.__field_data = {}
        else:
            self.__field_data = dict(field_data)

    def get_field_names(self):
        if self.__field_names is None:
            return tuple(self.__field_data.keys())
        else:
            return self.__field_names

    def __getitem__(self, item) -> Any:
        """
        Get a field with given name. If it's not present, fetch it from accessor.
        :param item: field name
        :return: field value
        """
        assert isinstance(item, (str, int)), "field can only be retrieved by index or name"

        if isinstance(item, int):
            item = self.get_field_names()[item]

        if item not in self.__field_data:
            # evaluate the field now
            self.__field_data[item] = self.__field_accessor(item).as_py()
        return self.__field_data[item]

    def __setitem__(self, key, value: AttributeType):
        """
        Set a field with given value.
        :param key: field name
        :param value: field value
        """
        assert isinstance(key, str), "field can only be set by name"
        self.__field_data[key] = value

    def as_series(self) -> pandas.Series:
        """convert the tuple to Pandas series format"""
        return pandas.Series(self.as_dict())

    def as_dict(self) -> Mapping[str, Any]:
        """
        Return a dictionary copy of this tuple.
        Fields will be fetched from accessor if absent.
        :return: dict with all the fields
        """
        # evaluate all the fields now
        for i in self.get_field_names():
            self.__getitem__(i)
        return deepcopy(self.__field_data)

    def as_key_value_pairs(self) -> List[typing.Tuple[str, Any]]:
        return [(k, v) for k, v in self.as_dict().items()]

    def to_values(self, output_field_names=None) -> typing.Tuple[Any, ...]:
        """
        Get values from tuple for selected fields.
        """
        if output_field_names is None:
            if self.__field_names is None:
                return tuple(self.__field_data.values())
            else:
                return tuple(self[i] for i in self.__field_names)
        return tuple(self[i] for i in output_field_names)

    def __str__(self) -> str:
        return f"Tuple[{str(self.as_dict()).strip('{').strip('}')}]"

    __repr__ = __str__

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Tuple):
            return False
        else:
            return self.__field_names == other.__field_names and all(self[i] == other[i] for i in self.__field_names)

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def __iter__(self):
        return (self[i] for i in self.__field_names)

    def reset(self, field_accessor):
        """
        Reset the tuple with given field accessor
        to avoid unnecessary tuple object creation.
        Field values will be cleared but field names(schema)
        will not be reset in this operation.
        :param field_accessor: new accessor
        """
        self.__field_data.clear()
        self.__field_accessor = field_accessor


class OutputTuple:
    """
    Container of pure data values. Only used after user returns
    a modified tuple or tuple-like.
    """

    def __init__(self, tuple_like: typing.Union[Tuple, TupleLike], output_field_names):
        """
        Create an OutputTuple from tuple-like objects or tuple.
        :param tuple_like: data object
        :param output_field_names: output schema
        """
        if isinstance(tuple_like, Tuple):
            self.data = tuple_like.to_values(output_field_names)
        else:
            if isinstance(tuple_like, List):
                field_dict = dict(tuple_like)
            else:
                field_dict = tuple_like
            self.data = tuple(field_dict[i] if i in field_dict else None for i in output_field_names)

    def get_fields(self, indices) -> typing.Tuple[Any, ...]:
        """
        Get values from output tuple for selected indices.
        """
        return tuple(self.data[i] for i in indices)

    def __iter__(self):
        return iter(self.data)

