import typing
from collections import OrderedDict
from itertools import chain
from loguru import logger
from typing import Iterable, Iterator

from core.architecture.sendsemantics.hash_based_shuffle_partitioner import (
    HashBasedShufflePartitioner,
)
from core.architecture.sendsemantics.range_based_shuffle_partitioner import (
    RangeBasedShufflePartitioner,
)
from core.architecture.sendsemantics.one_to_one_partitioner import OneToOnePartitioner
from core.architecture.sendsemantics.partitioner import Partitioner
from core.architecture.sendsemantics.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from core.architecture.sendsemantics.broad_cast_partitioner import (
    BroadcastPartitioner,
)
from core.models import Tuple
from core.models.payload import OutputDataFrame, DataPayload
from core.util import get_one_of
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import (
    HashBasedShufflePartitioning,
    OneToOnePartitioning,
    Partitioning,
    RoundRobinPartitioning,
    RangeBasedShufflePartitioning,
    BroadcastPartitioning,
)
from proto.edu.uci.ics.amber.engine.common import (
    ActorVirtualIdentity,
    PhysicalLinkIdentity,
)


class TupleToBatchConverter:
    def __init__(
        self,
    ):
        self._partitioners: OrderedDict[
            PhysicalLinkIdentity, Partitioning
        ] = OrderedDict()
        self._partitioning_to_partitioner: dict[
            type(Partitioning), type(Partitioner)
        ] = {
            OneToOnePartitioning: OneToOnePartitioner,
            RoundRobinPartitioning: RoundRobinPartitioner,
            HashBasedShufflePartitioning: HashBasedShufflePartitioner,
            RangeBasedShufflePartitioning: RangeBasedShufflePartitioner,
            BroadcastPartitioning: BroadcastPartitioner,
        }

    def add_partitioning(
        self, tag: PhysicalLinkIdentity, partitioning: Partitioning
    ) -> None:
        """
        Add down stream operator and its transfer policy
        :param tag:
        :param partitioning:
        :return:
        """
        the_partitioning = get_one_of(partitioning)
        logger.debug(f"adding {the_partitioning}")
        partitioner: type = self._partitioning_to_partitioner[type(the_partitioning)]
        self._partitioners.update({tag: partitioner(the_partitioning)})

    def tuple_to_batch(
        self, tuple_: Tuple
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, OutputDataFrame]]:
        return chain(
            *(
                partitioner.add_tuple_to_batch(tuple_)
                for partitioner in self._partitioners.values()
            )
        )

    def emit_end_of_upstream(
        self,
    ) -> Iterable[typing.Tuple[ActorVirtualIdentity, DataPayload]]:
        return chain(
            *(partitioner.no_more() for partitioner in self._partitioners.values())
        )
