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
from core.models import Tuple, InternalQueue, DataFrame, DataPayload
from core.models.internal_queue import DataElement, EmbeddedControlMessageElement
from core.storage.document_factory import DocumentFactory
from core.util import Stoppable, get_one_of
from core.util.runnable.runnable import Runnable
from core.util.virtual_identity import get_from_actor_id_for_input_port_storage
from proto.edu.uci.ics.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
    EmbeddedControlMessageIdentity,
)
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import (
    HashBasedShufflePartitioning,
    OneToOnePartitioning,
    Partitioning,
    RoundRobinPartitioning,
    RangeBasedShufflePartitioning,
    BroadcastPartitioning,
)
from loguru import logger
from typing import Union
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    ControlInvocation,
    EmptyRequest,
    EmbeddedControlMessageType,
    EmbeddedControlMessage,
    AsyncRpcContext,
    ControlRequest,
)


class InputPortMaterializationReaderRunnable(Runnable, Stoppable):
    def __init__(
        self,
        uri: str,
        queue: InternalQueue,
        worker_actor_id: ActorVirtualIdentity,
        partitioning: Partitioning,
    ):
        """
        Args:
            uri (str): The URI of the materialized document.
            queue: An instance of IQueue where messages are enqueued.
            worker_actor_id (ActorVirtualIdentity): The target worker actor's identity.
            partitioning: The partitioning information for this virtual reader worker
        """
        self.uri = uri
        self.queue = queue
        self.worker_actor_id = worker_actor_id
        from_actor_id = get_from_actor_id_for_input_port_storage(
            self.uri, self.worker_actor_id
        )
        self.channel_id = ChannelIdentity(
            from_actor_id, self.worker_actor_id, is_control=False
        )
        self._stopped = False
        self._finished = False
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
        the_partitioning: Partitioning = get_one_of(partitioning)
        partitioner = self._partitioning_to_partitioner[type(the_partitioning)]
        self.partitioner: Partitioner = (
            partitioner(the_partitioning)
            if partitioner != OneToOnePartitioner
            else partitioner(the_partitioning, self.worker_actor_id)
        )

    def finished(self) -> bool:
        """
        :return: Whether this reader thread has finished its logic.
        """
        return self._finished

    def tuple_to_batch_with_filter(self, tuple_: Tuple) -> typing.Iterator[DataFrame]:
        """
        Let the partitioner produce batches to each (hypothetical) downstream
        worker but only selects the worker that this thread is running on
        as the input. This mimics the iterator logic of that in output
        manager.
        """
        for receiver, tuples in self.partitioner.add_tuple_to_batch(tuple_):
            if receiver == self.worker_actor_id:
                yield self.tuples_to_data_frame(tuples)

    def run(self) -> None:
        """
        Main execution logic that reads tuples from the materialized storage and
        enqueues them in batches. It first emits a StartChannel ECM and, when finished,
        emits an EndChannel ECM. Use the same partitioner implementation as that in
        output manager, where a tuple is batched by the partitioner and only
        selected as the input of this worker according to the partitioner.
        """
        try:
            self.materialization, self.tuple_schema = DocumentFactory.open_document(
                self.uri
            )
            self.emit_ecm("StartChannel", EmbeddedControlMessageType.NO_ALIGNMENT)
            storage_iterator = self.materialization.get()

            # Iterate and process tuples.
            for tup in storage_iterator:
                if self._stopped:
                    break
                # Each tuple is sent to the partitioner and converted to
                # a batch-based iterator.
                for data_frame in self.tuple_to_batch_with_filter(tup):
                    self.emit_payload(data_frame)
            self.emit_ecm("EndChannel", EmbeddedControlMessageType.PORT_ALIGNMENT)
            self._finished = True
        except Exception as err:
            logger.exception(err)

    def stop(self):
        """Sets the stop flag so the run loop may terminate."""
        self._stopped = True

    def emit_ecm(self, method_name: str, alignment: EmbeddedControlMessageType) -> None:
        """
        Emit an ECM (StartChannel or EndChannel), and
        flush the remaining data batches if any. This mimics the
        iterator logic of that in output manager.
        """
        ecm = EmbeddedControlMessage(
            EmbeddedControlMessageIdentity(method_name),
            alignment,
            [],
            {
                self.worker_actor_id.name: ControlInvocation(
                    method_name,
                    ControlRequest(empty_request=EmptyRequest()),
                    AsyncRpcContext(ActorVirtualIdentity(), ActorVirtualIdentity()),
                    -1,
                )
            },
        )

        for payload in self.partitioner.flush(self.worker_actor_id, ecm):
            final_payload = (
                payload
                if isinstance(payload, EmbeddedControlMessage)
                else self.tuples_to_data_frame(payload)
            )
            self.emit_payload(final_payload)

    def emit_payload(self, payload: Union[DataPayload, EmbeddedControlMessage]) -> None:
        """
        Put the payload to the DP internal queue.
        """
        queue_element = (
            EmbeddedControlMessageElement(tag=self.channel_id, payload=payload)
            if isinstance(payload, EmbeddedControlMessage)
            else DataElement(tag=self.channel_id, payload=payload)
        )
        self.queue.put(queue_element)

    def tuples_to_data_frame(self, tuples: typing.List[Tuple]) -> DataFrame:
        """
        Converts a list of tuples to a DataFrame using pyarrow.Table.from_pydict
        :param tuples:
        :return:
        """
        return DataFrame(
            frame=Table.from_pydict(
                {
                    name: [t[name] for t in tuples]
                    for name in self.tuple_schema.get_attr_names()
                },
                schema=self.tuple_schema.as_arrow_schema(),
            )
        )
