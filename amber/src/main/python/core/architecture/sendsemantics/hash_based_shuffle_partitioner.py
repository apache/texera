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
    HashBasedShufflePartitioning,
    Partitioning,
)


class HashBasedShufflePartitioner(Partitioner):
    def __init__(self, partitioning: HashBasedShufflePartitioning):
        super().__init__(set_one_of(Partitioning, partitioning))
        logger.debug(f"got {partitioning}")
        self.batch_size = partitioning.batch_size
        # Indexed by hash_code to choose the downstream worker to send to.
        self.receivers = self.build_receiver_batches(partitioning.channels)
        self.hash_attribute_names = partitioning.hash_attribute_names

    @overrides
    def add_tuple_to_batch(
        self, tuple_: Tuple
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, typing.List[Tuple]]]:
        partial_tuple = (
            tuple_
            if not self.hash_attribute_names
            else tuple_.get_partial_tuple(self.hash_attribute_names)
        )
        hash_code = hash(partial_tuple) % len(self.receivers)
        receiver, batch = self.receivers[hash_code]
        batch.append(tuple_)
        if len(batch) == self.batch_size:
            yield receiver, batch
            self.receivers[hash_code] = (receiver, list())

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
