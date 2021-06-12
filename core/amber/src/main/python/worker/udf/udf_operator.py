from abc import ABC
from typing import Union, Iterable

from worker.models.generated.virtualidentity_pb2 import LinkIdentity
from worker.models.tuple import Tuple, InputExhausted


class UDFOperator(ABC):
    """
    Base class for row-oriented one-table input, one-table output user-defined operators. This must be implemented
    before using.
    """

    def open(self, *args, **kwargs) -> None:
        pass

    def process_texera_tuple(self, tuple_: Union[Tuple, InputExhausted], input_: LinkIdentity) -> Iterable[Tuple]:
        pass

    def close(self) -> None:
        pass
