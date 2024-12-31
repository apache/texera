import typing
from abc import ABC
from typing import Iterator

from betterproto import Message

from core.models import Tuple
from core.models.marker import Marker
from core.util import get_one_of
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import Partitioning
from proto.edu.uci.ics.amber.core import ActorVirtualIdentity


class Partitioner(ABC):
    def __init__(self, partitioning: Message):
        self.partitioning: Partitioning = get_one_of(partitioning)

    def add_tuple_to_batch(
        self, tuple_: Tuple
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, typing.List[Tuple]]]:
        pass

    def flush(
        self, marker: Marker
    ) -> Iterator[
        typing.Tuple[ActorVirtualIdentity, typing.Union[Marker, typing.List[Tuple]]]
    ]:
        pass

    def reset(self) -> None:
        pass

    def __repr__(self):
        return f"Partitioner[partitioning={self.partitioning}]"
