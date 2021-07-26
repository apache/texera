from abc import ABC, abstractmethod
from typing import Iterator, Union

from core.models.tuple import InputExhausted, Tuple
from proto.edu.uci.ics.amber.engine.common import LinkIdentity


class UDFOperator(ABC):
    """
    Base class for row-oriented one-table input, one-table output user-defined operators. This must be implemented
    before using.
    """

    def open(self) -> None:
        pass

    @abstractmethod
    def process_texera_tuple(self, tuple_: Union[Tuple, InputExhausted], input_: LinkIdentity) -> Iterator[Tuple]:
        pass

    def close(self) -> None:
        pass
