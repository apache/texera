from abc import ABC
from dataclasses import dataclass


class ITuple(ABC):
    pass


@dataclass
class Tuple(ITuple):
    """
    Python representation of the Texera.Tuple, as a pandas.Series.
    """


@dataclass
class InputExhausted:
    pass
