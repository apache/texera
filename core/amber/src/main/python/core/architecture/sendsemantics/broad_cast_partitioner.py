import typing
from typing import Iterator

from overrides import overrides

from core.architecture.sendsemantics.partitioner import Partitioner
from core.models import Tuple
from core.models.marker import Marker
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import (
    Partitioning,
    BroadcastPartitioning,
)
from proto.edu.uci.ics.amber.core import ActorVirtualIdentity


class BroadcastPartitioner(Partitioner):
    def __init__(self, partitioning: BroadcastPartitioning):
        super().__init__(set_one_of(Partitioning, partitioning))
        self.batch_size = partitioning.batch_size
        self.batch: list[Tuple] = list()
        self.receivers = list(
            {channel.to_worker_id for channel in partitioning.channels}
        )

    @overrides
    def add_tuple_to_batch(
        self, tuple_: Tuple
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, typing.List[Tuple]]]:
        self.batch.append(tuple_)
        if len(self.batch) == self.batch_size:
            for receiver in self.receivers:
                yield receiver, self.batch
            self.reset()

    @overrides
    def flush(
        self, marker: Marker
    ) -> Iterator[
        typing.Tuple[ActorVirtualIdentity, typing.Union[Marker, typing.List[Tuple]]]
    ]:
        if len(self.batch) > 0:
            for receiver in self.receivers:
                yield receiver, self.batch

        self.reset()
        for receiver in self.receivers:
            yield receiver, marker

    @overrides
    def reset(self) -> None:
        self.batch = list()
