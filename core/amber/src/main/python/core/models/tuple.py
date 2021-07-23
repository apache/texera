from abc import ABC
from dataclasses import dataclass

import pandas


@dataclass
class Tuple(ABC):
    """
    Python representation of the Texera.Tuple
    """


# Use pandas.Series as a Tuple.
Tuple.register(pandas.Series)


@dataclass
class InputExhausted:
    pass


if __name__ == '__main__':
    assert issubclass(pandas.Series, Tuple)
