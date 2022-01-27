import datetime
import typing
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, List, Mapping, Iterator, TypeVar

import pandas

AttributeType = TypeVar(
    'AttributeType',
    int,
    float,
    str,
    datetime.datetime
)

TupleLike = TypeVar(
    'TupleLike',
    pandas.Series,
    Iterator[typing.Tuple[str, AttributeType]],
    Mapping[str, typing.Callable],
    Mapping[str, AttributeType]
)


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

    def __init__(self, field_data: typing.Optional[TupleLike] = None):
        """
        Construct a lazy-tuple with given data
        :param field_data: tuple-like data that is already in memory
        """
        if field_data is not None:
            self._field_data = dict(field_data)
        else:
            self._field_data = dict()

    def get_field_names(self) -> typing.Tuple[str]:
        return tuple(map(str, self._field_data.keys()))

    def __getitem__(self, item: typing.Union[int, str]) -> Any:
        """
        Get a field with given name. If it's not present, fetch it from accessor.
        :param item: field name or field index
        :return: field value
        """
        assert isinstance(item, (int, str)), "field can only be retrieved by index or name"

        if isinstance(item, int):
            item: str = self.get_field_names()[item]

        if item not in self._field_data or callable(self._field_data[item]):
            # evaluate the field now
            self._field_data[item] = self._field_data[item](item).as_py()
        return self._field_data[item]

    def __setitem__(self, field_name: str, field_value: AttributeType):
        """
        Set a field with given value.
        :param field_name
        :param field_value
        """
        assert isinstance(field_name, str), "field can only be set by name"
        assert not callable(field_value), "field cannot be of type callable"
        self._field_data[field_name] = field_value

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
        return deepcopy(self._field_data)

    def as_key_value_pairs(self) -> List[typing.Tuple[str, Any]]:
        return [(k, v) for k, v in self.as_dict().items()]

    def values(self, output_field_names=None) -> typing.Tuple[Any, ...]:
        """
        Get values from tuple for selected fields.
        """
        if output_field_names is None:
            output_field_names = self.get_field_names()
        return tuple(self[i] for i in output_field_names)

    def __str__(self) -> str:
        return f"Tuple[{str(self.as_dict()).strip('{').strip('}')}]"

    __repr__ = __str__

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Tuple):
            return False
        else:
            return all(self[i] == other[i] for i in self.get_field_names())

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def __iter__(self):
        return (self[i] for i in self.get_field_names())


def to_tuple(tuple_like: typing.Union[Tuple, TupleLike]) -> Tuple:
    if isinstance(tuple_like, Tuple):
        return tuple_like
    return Tuple(tuple_like)
