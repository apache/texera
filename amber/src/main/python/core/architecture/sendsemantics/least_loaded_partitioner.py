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
from loguru import logger
from overrides import overrides
from typing import Iterator

from core.architecture.sendsemantics.partitioner import Partitioner
from core.models import Tuple
from core.models.state import State
from core.util import set_one_of
from proto.org.apache.texera.amber.core import ActorVirtualIdentity
from proto.org.apache.texera.amber.engine.architecture.rpc import EmbeddedControlMessage
from proto.org.apache.texera.amber.engine.architecture.sendsemantics import (
    LeastLoadedPartitioning,
    Partitioning,
)


class LeastLoadedPartitioner(Partitioner):
    """
    Fills one batch at a time and sends it to the receiver the coordinator
    currently ranks least backlogged.

    Round robin holds one batch per receiver and advances a tuple at a time, so
    each batch fills at 1/receivers the rate: a link accumulates
    senders x receivers x batch_size tuples before the first batch ships. This
    holds a single batch that fills at the full rate, so the first one ships
    after batch_size tuples no matter how wide the link is -- and chooses its
    destination at send time, when a measurement of who is behind exists.
    """

    def __init__(
        self, partitioning: LeastLoadedPartitioning, worker_id: str = None
    ) -> None:
        super().__init__(set_one_of(Partitioning, partitioning))
        self.batch_size = partitioning.batch_size
        # Only the receiver order is used; the per-receiver lists this builds go
        # unused, since this partitioner keeps a single batch of its own.
        self.receivers = [
            receiver for receiver, _ in self.build_receiver_batches(partitioning.channels)
        ]
        self._batch: typing.List[Tuple] = []

        # Which receiver the next full batch goes to. Seeded by this sender's
        # position among the link's senders rather than 0, so batches that fill
        # before the coordinator's first ranking arrives are spread instead of
        # landing on one receiver together. Taken from the partitioning's own
        # channel list, which every sender holds identically.
        self._preferred_index = 0
        if worker_id is not None and self.receivers:
            senders = list(
                dict.fromkeys(
                    channel.from_worker_id.name for channel in partitioning.channels
                )
            )
            if worker_id in senders:
                self._preferred_index = senders.index(worker_id) % len(self.receivers)
            else:
                logger.warning(
                    f"least-loaded routing: {worker_id} is not among this link's "
                    f"senders {senders}; starting at receiver 0, which can send "
                    f"the first batches of every sender to the same receiver"
                )

    def set_preferred_receiver_index(self, index: int) -> None:
        """
        Point the next batch at a different receiver.

        Ignores an out-of-range index rather than raising: the coordinator
        derives it from its own copy of the channel list, and a mismatch during
        reconfiguration should fall back to the last good target instead of
        killing the worker.
        """
        if 0 <= index < len(self.receivers):
            self._preferred_index = index
        else:
            logger.warning(
                f"least-loaded routing: index {index} out of range for "
                f"{len(self.receivers)} receivers; preference ignored"
            )

    def _ship_target(self) -> ActorVirtualIdentity:
        return self.receivers[self._preferred_index]

    @overrides
    def add_tuple_to_batch(
        self, tuple_: Tuple
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, typing.List[Tuple]]]:
        self._batch.append(tuple_)
        if len(self._batch) >= self.batch_size:
            yield self._ship_target(), self._batch
            self._batch = []

    @overrides
    def flush(
        self, to: ActorVirtualIdentity, ecm: EmbeddedControlMessage
    ) -> Iterator[typing.Union[EmbeddedControlMessage, typing.List[Tuple]]]:
        # The pending batch has exactly one destination, so it is emitted on that
        # receiver's call only -- yielding it on every receiver's call would
        # duplicate the data to all of them.
        if self._batch and self._ship_target() == to:
            yield self._batch
            self._batch = []
        yield ecm

    @overrides
    def flush_state(
        self, state: State
    ) -> Iterator[
        typing.Tuple[ActorVirtualIdentity, typing.Union[State, typing.List[Tuple]]]
    ]:
        if self._batch:
            yield self._ship_target(), self._batch
            self._batch = []
        # State is shared context rather than partitioned data: every receiver
        # gets it, exactly as the other partitioners do.
        for receiver in self.receivers:
            yield receiver, state
