import typing
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, TypeVar

import pandas

TupleLike = TypeVar('TupleLike', pandas.Series, List[typing.Tuple[str, Any]], Dict[str, Any])


@dataclass
class InputExhausted:
    pass


class Tuple(pandas.Series):
    """
    Tuple implementation with pandas.Series.
    """

    def __init__(self, tuple_like: TupleLike):
        super().__init__(tuple_like)

    def as_series(self) -> pandas.Series:
        return self

    def as_dict(self) -> Mapping[str, Any]:
        return self.to_dict()

    def as_key_value_pairs(self) -> List[typing.Tuple[str, Any]]:
        return list(self.to_dict().items())

    def __str__(self):
        return f"Tuple[{str(self.as_dict()).strip('{').strip('}')}]"
