from abc import ABC
from dataclasses import dataclass

import pandas


@dataclass
class ITuple(ABC):
    """
    Python representation of the Texera.Tuple
    """


# Use pandas.Series as a Tuple, so that isinstance(pandas.Series(), Tuple) can be true.
# A pandas.Series instance can be viewed as a Tuple to be processed.
ITuple.register(pandas.Series)


@dataclass
class InputExhausted:
    pass


class Tuple:
    _series_representation: pandas.Series

    def __init__(self, tuple_like: ITuple):
        if isinstance(tuple_like, pandas.Series):
            self._series_representation = tuple_like
        elif isinstance(tuple_like, Tuple):
            self._series_representation = tuple_like._series_representation
        elif isinstance(tuple_like, dict):
            self._series_representation = pandas.Series(tuple_like)
        elif isinstance(tuple_like, list):
            self._series_representation = pandas.Series({key: value for key, value in tuple_like})

    def __getitem__(self, item):
        return self._series_representation.__getitem__(item)

    def __setitem__(self, key, value):
        self._series_representation.__setitem__(key, value)

    def as_series(self):
        return self._series_representation


# Use pandas.Series as a Tuple, so that isinstance(pandas.Series(), Tuple) can be true.
# A pandas.Series instance can be viewed as a Tuple to be processed.
ITuple.register(Tuple)
