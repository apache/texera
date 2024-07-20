import typing
from collections import OrderedDict
from itertools import chain
from loguru import logger
from typing import Iterable, Iterator

from core.architecture.packaging.input_manager import WorkerPort, Channel
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
from core.models import Tuple, Schema
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
    PhysicalLink,
    PortIdentity,
    ChannelIdentity,
)


class OutputManager:
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self._partitioners: OrderedDict[PhysicalLink, Partitioning] = OrderedDict()
        self._partitioning_to_partitioner: dict[
            type(Partitioning), type(Partitioner)
        ] = {
            OneToOnePartitioning: OneToOnePartitioner,
            RoundRobinPartitioning: RoundRobinPartitioner,
            HashBasedShufflePartitioning: HashBasedShufflePartitioner,
            RangeBasedShufflePartitioning: RangeBasedShufflePartitioner,
            BroadcastPartitioning: BroadcastPartitioner,
        }
        self._ports: typing.Dict[PortIdentity, WorkerPort] = dict()
        self._channels: typing.Dict[ChannelIdentity, Channel] = dict()

    def add_output_port(self, port_id: PortIdentity, schema: Schema) -> None:
        if port_id.id is None:
            port_id.id = 0
        if port_id.internal is None:
            port_id.internal = False

        # each port can only be added and initialized once.
        if port_id not in self._ports:
            self._ports[port_id] = WorkerPort(schema)

    def get_port(self, port_id=None) -> WorkerPort:
        return list(self._ports.values())[0]

    def add_partitioning(self, tag: PhysicalLink, partitioning: Partitioning) -> None:
        """
        Add down stream operator and its transfer policy
        :param tag:
        :param partitioning:
        :return:
        """
        the_partitioning = get_one_of(partitioning)
        logger.debug(f"adding {the_partitioning}")
        partitioner = self._partitioning_to_partitioner[type(the_partitioning)]
        self._partitioners[tag] = (
            partitioner(the_partitioning)
            if partitioner != OneToOnePartitioner
            else partitioner(the_partitioning, self.worker_id)
        )

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
