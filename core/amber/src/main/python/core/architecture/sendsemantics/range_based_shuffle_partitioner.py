import typing
from typing import Iterator, List

from loguru import logger
from overrides import overrides

from core.architecture.sendsemantics.partitioner import Partitioner
from core.models import Tuple
from core.models.payload import OutputDataFrame, DataPayload, EndOfUpstream
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import (
    RangeBasedShufflePartitioning,
    Partitioning,
)
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity


class RangeBasedShufflePartitioner(Partitioner):
    def __init__(self, partitioning: RangeBasedShufflePartitioning):
        super().__init__(set_one_of(Partitioning, partitioning))
        logger.info(f"got {partitioning}")
        self.batch_size = partitioning.batch_size
        self.receivers: List[typing.Tuple[ActorVirtualIdentity, List[Tuple]]] = [
            (receiver, list()) for receiver in partitioning.receivers
        ]
        self.range_column_indices = partitioning.range_column_indices
        self.range_min = partitioning.range_min
        self.range_max = partitioning.range_max
        self.keys_per_receiver = int(
            (
                (partitioning.range_max - partitioning.range_min)
                // len(partitioning.receivers)
            )
            + 1
        )

    def get_receiver_index(self, column_val) -> int:
        if column_val < self.range_min:
            return 0
        elif column_val > self.range_max:
            return len(self.receivers) - 1
        else:
            return int((column_val - self.range_min) // self.keys_per_receiver)

    @overrides
    def add_tuple_to_batch(
        self, tuple_: Tuple
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, OutputDataFrame]]:
        column_val = tuple_[self.range_column_indices[0]]
        receiver_index = self.get_receiver_index(column_val)
        receiver, batch = self.receivers[receiver_index]
        batch.append(tuple_)
        if len(batch) == self.batch_size:
            yield receiver, OutputDataFrame(frame=batch)
            self.receivers[receiver_index] = (receiver, list())

    @overrides
    def no_more(self) -> Iterator[typing.Tuple[ActorVirtualIdentity, DataPayload]]:
        for receiver, batch in self.receivers:
            if len(batch) > 0:
                yield receiver, OutputDataFrame(frame=batch)
            yield receiver, EndOfUpstream()
