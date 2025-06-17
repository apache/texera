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
from typing import Iterator

from loguru import logger
from overrides import overrides

from core.architecture.sendsemantics.partitioner import Partitioner
from core.models import Tuple
from core.models.state import State
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import (
    RangeBasedShufflePartitioning,
    Partitioning,
)
from proto.edu.uci.ics.amber.core import ActorVirtualIdentity
from proto.edu.uci.ics.amber.engine.architecture.rpc import EmbeddedControlMessage


class RangeBasedShufflePartitioner(Partitioner):
    def __init__(self, partitioning: RangeBasedShufflePartitioning):
        super().__init__(set_one_of(Partitioning, partitioning))
        logger.info(f"got {partitioning}")
        self.batch_size = partitioning.batch_size
        # Partitioning contains an ordered list of downstream worker ids.
        # Currently we are using the index of such an order to choose
        # a downstream worker to send tuples to.
        # Must use dict.fromkeys to ensure the order of receiver workers
        # from partitioning is preserved (using `{}` to create a set
        # does not preserve order and will not work correctly.)
        self.receivers = [
            (rid, [])
            for rid in dict.fromkeys(
                channel.to_worker_id for channel in partitioning.channels
            )
        ]
        self.range_attribute_names = partitioning.range_attribute_names
        self.range_min = partitioning.range_min
        self.range_max = partitioning.range_max
        self.keys_per_receiver = int(
            (
                (partitioning.range_max - partitioning.range_min)
                // len(partitioning.channels)
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
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, typing.List[Tuple]]]:
        column_val = tuple_[self.range_attribute_names[0]]
        receiver_index = self.get_receiver_index(column_val)
        receiver, batch = self.receivers[receiver_index]
        batch.append(tuple_)
        if len(batch) == self.batch_size:
            yield receiver, batch
            self.receivers[receiver_index] = (receiver, list())

    @overrides
    def flush(
        self, to: ActorVirtualIdentity, ecm: EmbeddedControlMessage
    ) -> Iterator[typing.Union[EmbeddedControlMessage, typing.List[Tuple]]]:
        for receiver, batch in self.receivers:
            if receiver == to:
                if len(batch) > 0:
                    yield batch
                yield ecm

    @overrides
    def flush_state(
        self, state: State
    ) -> Iterator[
        typing.Tuple[ActorVirtualIdentity, typing.Union[State, typing.List[Tuple]]]
    ]:
        for receiver, batch in self.receivers:
            if len(batch) > 0:
                yield receiver, batch
            yield receiver, state
