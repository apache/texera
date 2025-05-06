# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import typing

from pyarrow.lib import Table

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
from core.models import Tuple, InternalQueue, DataFrame, MarkerFrame
from core.models.internal_queue import DataElement
from core.models.marker import StartOfInputChannel, EndOfInputChannel, Marker
from core.storage.document_factory import DocumentFactory
from core.util import Stoppable, get_one_of
from core.util.runnable.runnable import Runnable
from core.util.virtual_identity import get_from_actor_id_for_input_port_storage
from proto.edu.uci.ics.amber.core import ActorVirtualIdentity, ChannelIdentity
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import (
    HashBasedShufflePartitioning,
    OneToOnePartitioning,
    Partitioning,
    RoundRobinPartitioning,
    RangeBasedShufflePartitioning,
    BroadcastPartitioning,
)


class InputPortMaterializationReaderThread(Runnable, Stoppable):
    def __init__(
        self,
        uri: str,
        queue: InternalQueue,
        worker_actor_id: ActorVirtualIdentity,
        partitioning: Partitioning
    ):
        """
        Args:
            uri (str): The URI of the materialized document.
            queue: An instance of IQueue where messages are enqueued.
            worker_actor_id (ActorVirtualIdentity): The target worker actor's identity.
        """
        self.uri = uri
        self.queue = queue
        self.worker_actor_id = worker_actor_id
        self.sequence_number = 0  # Counter to mimic AtomicLong behavior.
        self.buffer = []  # Buffer for Tuple objects.
        from_actor_id = get_from_actor_id_for_input_port_storage(
            self.uri, self.worker_actor_id
        )
        self.channel_id = ChannelIdentity(
            from_actor_id, self.worker_actor_id, is_control=False
        )
        self._stopped = False
        self.materialization = None
        self.tuple_schema = None
        self._partitioning_to_partitioner: dict[
            type(Partitioning), type(Partitioner)
        ] = {
            OneToOnePartitioning: OneToOnePartitioner,
            RoundRobinPartitioning: RoundRobinPartitioner,
            HashBasedShufflePartitioning: HashBasedShufflePartitioner,
            RangeBasedShufflePartitioning: RangeBasedShufflePartitioner,
            BroadcastPartitioning: BroadcastPartitioner,
        }
        the_partitioning = get_one_of(partitioning)
        partitioner = self._partitioning_to_partitioner[type(the_partitioning)]
        print(f"Input port materialization thread: adding {the_partitioning}")
        self.partitioner = (
            partitioner(the_partitioning)
            if partitioner != OneToOnePartitioner
            else partitioner(the_partitioning, self.worker_actor_id)
        )

    def tuple_to_batch_with_filter(self, tuple_: Tuple) -> typing.Iterator[DataFrame]:
        for receiver, tuples in self.partitioner.add_tuple_to_batch(tuple_):
            print(f"Trying receiver={receiver!r}; ")
            if receiver == self.worker_actor_id:
                # Found the one we want → immediately return its DataFrame
                yield self.tuple_to_frame(tuples)
            else:
                # Log every non‐matching receiver
                print(
                    f"Skipping receiver={receiver!r}; "
                    f"looking for worker_actor_id={self.worker_actor_id!r}"
                )

    def run(self) -> None:
        """
        Main execution method that reads tuples from the materialized document and
        enqueues them in batches. It first emits a start marker and, when finished,
        emits an end marker.
        """
        # Setup a unique channel identity.

        # Emit start marker.
        self.emit_marker(StartOfInputChannel())

        try:
            # Open the document and obtain an iterator over the tuples.
            self.materialization, self.tuple_schema = DocumentFactory.open_document(
                self.uri
            )
            storage_iterator = (
                self.materialization.get()
            )  # Iterator over Tuple objects.

            # Iterate and process tuples.
            for tup in storage_iterator:
                if self._stopped:
                    break
                for data_frame in self.tuple_to_batch_with_filter(tup):
                    queue_element = DataElement(
                        tag=self.channel_id,
                        payload=data_frame,
                    )
                    self.queue.put(queue_element)
        finally:
            # Emit end marker.
            self.emit_marker(EndOfInputChannel())

    def stop(self):
        """Sets the stop flag so the run loop may terminate."""
        self._stopped = True

    def flush(self) -> None:
        """
        Flush the current batch of tuples in the buffer, wrapping them in a DataFrame
        and sending them as a WorkflowFIFOMessage.
        """
        if not self.buffer:
            return
        data_payload = self.tuple_to_frame(self.buffer)
        queue_element = DataElement(
            tag=self.channel_id,
            payload=data_payload,
        )
        self.queue.put(queue_element)
        self.buffer.clear()

    def emit_marker(self, marker) -> None:
        """
        Emit a marker (for example, StartOfInputChannel or EndOfInputChannel)
        by first flushing the current data and then sending the marker message.
        """
        for receiver, payload in self.partitioner.flush(marker):
            print(f"Trying receiver={receiver!r}; ")
            if receiver == self.worker_actor_id:
                # Found the one we want → immediately return its DataFrame
                final_payload = (
                    MarkerFrame(payload)
                    if isinstance(payload, Marker)
                    else self.tuple_to_frame(payload)
                )
                queue_element = DataElement(
                    tag=self.channel_id,
                    payload=final_payload,
                )
                self.queue.put(queue_element)
            else:
                # Log every non‐matching receiver
                print(
                    f"Skipping receiver={receiver!r}; "
                    f"looking for worker_actor_id={self.worker_actor_id!r}"
                )

    def get_sequence_number(self) -> int:
        """
        Returns a monotonically increasing sequence number.
        """
        curr = self.sequence_number
        self.sequence_number += 1
        return curr

    def tuple_to_frame(self, tuples: typing.List[Tuple]) -> DataFrame:
        return DataFrame(
            frame=Table.from_pydict(
                {
                    name: [t[name] for t in tuples]
                    for name in self.tuple_schema.get_attr_names()
                },
                schema=self.tuple_schema.as_arrow_schema(),
            )
        )
