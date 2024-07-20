import typing
from typing import Iterator

from overrides import overrides

from core.architecture.sendsemantics.partitioner import Partitioner
from core.models import Tuple
from core.models.payload import OutputDataFrame, DataPayload, EndOfUpstream
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import (
    Partitioning,
    BroadcastPartitioning,
)
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity


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
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, OutputDataFrame]]:
        self.batch.append(tuple_)
        if len(self.batch) == self.batch_size:
            for receiver in self.receivers:
                yield receiver, OutputDataFrame(frame=self.batch)
            self.reset()

    @overrides
    def no_more(self) -> Iterator[typing.Tuple[ActorVirtualIdentity, DataPayload]]:
        if len(self.batch) > 0:
            for receiver in self.receivers:
                yield receiver, OutputDataFrame(frame=self.batch)

        self.reset()
        for receiver in self.receivers:
            yield receiver, EndOfUpstream()

    @overrides
    def reset(self) -> None:
        self.batch = list()
