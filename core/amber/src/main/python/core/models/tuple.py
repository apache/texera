from abc import ABC
from dataclasses import dataclass

import pandas


@dataclass
class Tuple(ABC):
    """
    Python representation of the Texera.Tuple
    """


# Use pandas.Series as a Tuple, so that isinstance(pandas.Series(), Tuple) can be true.
# A pandas.Series instance can be viewed as a Tuple to be processed.
Tuple.register(pandas.Series)


@dataclass
class InputExhausted:
    pass
