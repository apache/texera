import threading
import typing
from collections import OrderedDict
from itertools import chain
from typing import Iterable, Iterator

from loguru import logger
from pyarrow import Table

from core.architecture.packaging.input_manager import WorkerPort, Channel
from core.architecture.sendsemantics.broad_cast_partitioner import (
    BroadcastPartitioner,
)
from core.architecture.sendsemantics.hash_based_shuffle_partitioner import (
    HashBasedShufflePartitioner,
)
from core.architecture.sendsemantics.one_to_one_partitioner import OneToOnePartitioner
from core.architecture.sendsemantics.partitioner import Partitioner
from core.architecture.sendsemantics.range_based_shuffle_partitioner import (
    RangeBasedShufflePartitioner,
)
from core.architecture.sendsemantics.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from core.models import Tuple, Schema, MarkerFrame
from core.models.marker import Marker
from core.models.payload import DataPayload, DataFrame
from core.runnables.port_result_writer import PortResultWriter
from core.storage.document_factory import DocumentFactory
from core.storage.model.virtual_document import VirtualDocument
from core.util import get_one_of
from core.util.virtual_identity import get_worker_index
from proto.edu.uci.ics.amber.core import (
    ActorVirtualIdentity,
    PhysicalLink,
    PortIdentity,
    ChannelIdentity,
)
from proto.edu.uci.ics.amber.engine.architecture.rpc import ChannelMarkerPayload
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import (
    HashBasedShufflePartitioning,
    OneToOnePartitioning,
    Partitioning,
    RoundRobinPartitioning,
    RangeBasedShufflePartitioning,
    BroadcastPartitioning,
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
        self._port_result_writer: typing.Optional[PortResultWriter] = None

    def add_output_port(self, port_id: PortIdentity, schema: Schema, storage_uri: str) -> None:
        if port_id.id is None:
            port_id.id = 0
        if port_id.internal is None:
            port_id.internal = False

        if storage_uri != "":
            document: VirtualDocument[Tuple]
            document, _ = DocumentFactory.open_document(storage_uri)
            optional_writer = document.writer(str(get_worker_index(self.worker_id)))
            self.enable_port_storage_writer_if_not_exists()
            self._port_result_writer.add_writer(port_id, optional_writer)

        # each port can only be added and initialized once.
        if port_id not in self._ports:
            self._ports[port_id] = WorkerPort(schema)

    def enable_port_storage_writer_if_not_exists(self):
        if self._port_result_writer is None:
            self._port_result_writer = PortResultWriter()
            threading.Thread(
                target=self._port_result_writer.run, daemon=True, name="port_storage_writer_thread"
            ).start()

    def get_port(self, port_id=None) -> WorkerPort:
        return list(self._ports.values())[0]

    def get_output_channel_ids(self):
        return self._channels.keys()

    def save_tuple_to_storage_if_needed(self, amber_tuple: Tuple, port_id=None) -> None:
        if self._port_result_writer is not None:
            if port_id in self._ports:
                self._port_result_writer.put_tuple(port_id, amber_tuple)
            else:
                for available_port_id in self._ports:
                    self._port_result_writer.put_tuple(available_port_id, amber_tuple)

    def close_output_storage_writers(self) -> None:
        if self._port_result_writer is not None:
            self._port_result_writer.stop()

    def add_partitioning(self, tag: PhysicalLink, partitioning: Partitioning) -> None:
        """
        Add down stream operator and its transfer policy
        :param tag:
        :param partitioning:
        :return:
        """
        the_partitioning = get_one_of(partitioning)
        logger.debug(f"adding {the_partitioning}")
        for channel_id in the_partitioning.channels:
            self._channels[channel_id] = Channel()
        partitioner = self._partitioning_to_partitioner[type(the_partitioning)]
        self._partitioners[tag] = (
            partitioner(the_partitioning)
            if partitioner != OneToOnePartitioner
            else partitioner(the_partitioning, self.worker_id)
        )

    def tuple_to_batch(
        self, tuple_: Tuple
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, DataFrame]]:
        return chain(
            *(
                (
                    (receiver, self.tuple_to_frame(tuples))
                    for receiver, tuples in partitioner.add_tuple_to_batch(tuple_)
                )
                for partitioner in self._partitioners.values()
            )
        )

    def emit_marker_to_channel(
        self, channel_id: ChannelIdentity, marker: ChannelMarkerPayload
    ) -> Iterable[typing.Tuple[ActorVirtualIdentity, DataPayload]]:
        return chain(
            *(
                (
                    (
                        receiver,
                        (
                            payload
                            if isinstance(payload, ChannelMarkerPayload)
                            else self.tuple_to_frame(payload)
                        ),
                    )
                    for receiver, payload in partitioner.flush(marker)
                    if receiver == channel_id.to_worker_id
                )
                for partitioner in self._partitioners.values()
            )
        )

    def emit_marker(
        self, marker: Marker
    ) -> Iterable[typing.Tuple[ActorVirtualIdentity, DataPayload]]:
        return chain(
            *(
                (
                    (
                        receiver,
                        (
                            MarkerFrame(payload)
                            if isinstance(payload, Marker)
                            else self.tuple_to_frame(payload)
                        ),
                    )
                    for receiver, payload in partitioner.flush(marker)
                )
                for partitioner in self._partitioners.values()
            )
        )

    def tuple_to_frame(self, tuples: typing.List[Tuple]) -> DataFrame:
        return DataFrame(
            frame=Table.from_pydict(
                {
                    name: [t[name] for t in tuples]
                    for name in self.get_port().get_schema().get_attr_names()
                },
                schema=self.get_port().get_schema().as_arrow_schema(),
            )
        )
