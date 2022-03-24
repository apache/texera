import datetime
import pyarrow
import typing
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, List, Mapping, Iterator, TypeVar, Dict, Callable

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

        def field_accessor(field_name: str) -> AttributeType:
            """
            Retrieve the field value by a given field name.
            This abstracts and hides the underlying implementation
            of the tuple data storage from the user.
            """
            value = self._table.column(field_name).chunks[chunk_idx][tuple_idx].as_py()
            field_type = self._table.schema.field_by_name(field_name).type

            # for binary types, convert pickled objects back.
            if field_type == pyarrow.binary() and value[:6] == b'pickle':
                import pickle
                value = pickle.loads(value[10:])
            return value

        self._current_idx += 1
        return field_accessor


class Tuple:
    """
    Lazy-Tuple implementation.
    """

    def __init__(self, tuple_like: typing.Optional[TupleLike] = None):
        """
        Construct a lazy-tuple with given TupleLike object. If the field value is a callable
        accessor callable, the actual value is fetched upon first reference.

        :param tuple_like: in which the field value could be the actual value in memory, or a
            callable accessor.
        """
        assert len(tuple_like) != 0
        if isinstance(tuple_like, Tuple):
            self._field_data = tuple_like._field_data
        elif isinstance(tuple_like, pandas.Series):
            self._field_data = tuple_like.to_dict()
        else:
            self._field_data = dict(tuple_like) if tuple_like else dict()

    def __getitem__(self, item: typing.Union[int, str]) -> AttributeType:
        """
        Get a field value with given item. If the value is an accessor, fetch it from the accessor.
        :param item: field name or field index
        :return: field value
        """
        assert isinstance(item, (int, str)), "field can only be retrieved by index or name"

        if isinstance(item, int):
            item: str = self.get_field_names()[item]

        if callable(self._field_data[item]) \
                and getattr(self._field_data[item], '__name__', 'Unknown') == "field_accessor":
            # evaluate the field now
            field_accessor = self._field_data[item]
            self._field_data[item] = field_accessor(field_name=item)
        return self._field_data[item]

    def __setitem__(self, field_name: str, field_value: AttributeType) -> None:
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

    def as_dict(self) -> Dict[str, AttributeType]:
        """
        Return a dictionary copy of this tuple.
        Fields will be fetched from accessor if absent.
        :return: dict with all the fields
        """
        # evaluate all the fields now
        for i in self.get_field_names():
            self.__getitem__(i)
        return deepcopy(self._field_data)

    def as_key_value_pairs(self) -> List[typing.Tuple[str, AttributeType]]:
        return [(k, v) for k, v in self.as_dict().items()]

    def get_field_names(self) -> typing.Tuple[str]:
        return tuple(map(str, self._field_data.keys()))

    def get_fields(self, output_field_names=None) -> typing.Tuple[AttributeType]:
        """
        Get values from tuple for selected fields.
        """
        if output_field_names is None:
            output_field_names = self.get_field_names()
        return tuple(self[i] for i in output_field_names)

    def __iter__(self) -> Iterator[AttributeType]:
        return iter(self.get_fields())

    def __str__(self) -> str:
        return f"Tuple[{str(self.as_dict()).strip('{').strip('}')}]"

    __repr__ = __str__

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Tuple) and \
               self.get_field_names() == other.get_field_names() and \
               all(self[i] == other[i] for i in self.get_field_names())

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def __len__(self) -> int:
        return len(self._field_data)
