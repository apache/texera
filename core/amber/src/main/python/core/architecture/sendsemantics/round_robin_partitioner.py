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

from overrides import overrides

from core.architecture.sendsemantics.partitioner import Partitioner
from core.models import Tuple
from core.models.marker import Marker
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import (
    Partitioning,
    RoundRobinPartitioning,
)
from proto.edu.uci.ics.amber.core import ActorVirtualIdentity


class RoundRobinPartitioner(Partitioner):
    def __init__(self, partitioning: RoundRobinPartitioning):
        super().__init__(set_one_of(Partitioning, partitioning))
        self.batch_size = partitioning.batch_size
        # Partitioning contains an ordered list of downstream worker ids.
        # Currently we are using the index of such an order to choose
        # a downstream worker to send tuples to.
        # Must use dict.fromkeys to ensure the order of receiver workers
        # from partitioning is preserved (using `{}` to create a set
        # does not preserve order and will not work with input-port
        # materialization reader threads.)
        self.receivers = [
            (rid, [])
            for rid in dict.fromkeys(
                channel.to_worker_id for channel in partitioning.channels
            )
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
        self, to: ActorVirtualIdentity, marker: Marker
    ) -> Iterator[typing.Union[Marker, typing.List[Tuple]]]:
        for receiver, batch in self.receivers:
            if receiver == to:
                if len(batch) > 0:
                    yield batch
                    batch.clear()
                yield marker

    @overrides
    def flush_marker(
        self, marker: Marker
    ) -> Iterator[
        typing.Tuple[ActorVirtualIdentity, typing.Union[Marker, typing.List[Tuple]]]
    ]:
        for receiver, batch in self.receivers:
            if len(batch) > 0:
                yield receiver, batch
                batch.clear()
            yield receiver, marker
