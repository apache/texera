import datetime
import typing
from dataclasses import dataclass
from typing import Any, List, Mapping, TypeVar

import pandas

AttributeType = TypeVar('AttributeType', int, float, str, datetime.datetime)

TupleLike = TypeVar('TupleLike', pandas.Series, List[typing.Tuple[str, AttributeType]], Mapping[str, AttributeType])


@dataclass
class InputExhausted:
    pass


class Tuple(pandas.Series):
    """
    Tuple implementation with pandas.Series.
    """

    def __init__(self, tuple_like: TupleLike):
        assert len(tuple_like) != 0

        # convert List[Tuple[str, Any]] into a dict-like object
        if isinstance(tuple_like, list):
            tuple_like = dict(tuple_like)

        super().__init__(tuple_like)

    def as_series(self) -> pandas.Series:
        return pandas.Series.copy(self, deep=True)

    def as_dict(self) -> Mapping[str, Any]:
        return self.to_dict()

    def as_key_value_pairs(self) -> List[typing.Tuple[str, Any]]:
        return list(self.to_dict().items())

    def __str__(self) -> str:
        return f"Tuple[{str(self.as_dict()).strip('{').strip('}')}]"

    __repr__ = __str__

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Tuple):
            return False
        else:
            return pandas.Series.__eq__(self.as_series(), other.as_series()).all()

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)
