import typing
from typing import Iterator

from overrides import overrides

from core.architecture.sendsemantics.partitioner import Partitioner
from core.models import Tuple
from core.models.marker import Marker
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import (
    Partitioning,
    RoundRobinPartitioning,
)
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity


class RoundRobinPartitioner(Partitioner):
    def __init__(self, partitioning: RoundRobinPartitioning):
        super().__init__(set_one_of(Partitioning, partitioning))
        self.batch_size = partitioning.batch_size
        self.receivers = [
            (receiver, [])
            for receiver in {channel.to_worker_id for channel in partitioning.channels}
        ]
        self.round_robin_index = 0

    @overrides
    def add_tuple_to_batch(
        self, tuple_: Tuple
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, typing.List[Tuple]]]:
        receiver, batch = self.receivers[self.round_robin_index]
        batch.append(tuple_)
        if len(batch) == self.batch_size:
            yield receiver, batch
            self.receivers[self.round_robin_index] = (receiver, list())
        self.round_robin_index = (self.round_robin_index + 1) % len(self.receivers)

    @overrides
    def flush(
        self, marker: Marker
    ) -> Iterator[
        typing.Tuple[ActorVirtualIdentity, typing.Union[Marker, typing.List[Tuple]]]
    ]:
        for receiver, batch in self.receivers:
            if len(batch) > 0:
                yield receiver, batch
                batch.clear()
            yield receiver, marker
