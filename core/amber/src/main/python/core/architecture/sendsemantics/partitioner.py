import typing
from abc import ABC
from typing import Iterator

from betterproto import Message
from pyarrow import Table

from core.models import Tuple, Schema
from core.models.payload import DataPayload, DataFrame
from core.util import get_one_of
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import Partitioning
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity


class Partitioner(ABC):
    def __init__(self, partitioning: Message, schema: Schema):
        self.partitioning: Partitioning = get_one_of(partitioning)
        self.schema = schema

    def tuple_to_frame(self, tuples: typing.List[Tuple]) -> DataFrame:
        return DataFrame(
            frame=Table.from_pydict(
                {
                    name: [t[name] for t in tuples]
                    for name in self.schema.get_attr_names()
                },
                schema=self.schema.as_arrow_schema(),
            )
        )

    def add_tuple_to_batch(
        self, tuple_: Tuple
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, DataFrame]]:
        pass

    def no_more(self) -> Iterator[typing.Tuple[ActorVirtualIdentity, DataPayload]]:
        pass

    def reset(self) -> None:
        pass

    def __repr__(self):
        return f"Partitioner[partitioning={self.partitioning}]"
