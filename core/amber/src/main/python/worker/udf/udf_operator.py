from abc import ABC
from typing import Union, Iterable

from worker.models.identity import LinkIdentity
from worker.models.tuple import Tuple, InputExhausted, ITuple


class UDFOperator(ABC):
    """
    Base class for row-oriented one-table input, one-table output user-defined operators. This must be implemented
    before using.
    """

    def __init__(self):
        pass

    def open(self, *args, **kwargs) -> None:
        pass

    def process_texera_tuple(self, tuple: Union[Tuple, InputExhausted],
                             input: LinkIdentity) -> Iterable[ITuple]:
        pass

    def close(self) -> None:
        pass
