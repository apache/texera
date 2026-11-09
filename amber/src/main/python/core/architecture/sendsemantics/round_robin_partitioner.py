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

from overrides import overrides

from core.architecture.sendsemantics.indexed_shuffle_partitioner import (
    IndexedShufflePartitioner,
)
from core.models import Tuple
from proto.org.apache.texera.amber.engine.architecture.sendsemantics import (
    RoundRobinPartitioning,
)


class RoundRobinPartitioner(IndexedShufflePartitioner):
    # Unlike the hash/range shuffles, round-robin drains a slot's batch on flush.
    _clear_batch_on_flush = True

    def __init__(self, partitioning: RoundRobinPartitioning):
        super().__init__(partitioning)
        # Indexed by round_robin_index to choose the downstream worker to send to.
        self.round_robin_index = 0

    @overrides
    def _route(self, tuple_: Tuple) -> int:
        return self.round_robin_index

    @overrides
    def _advance(self) -> None:
        self.round_robin_index = (self.round_robin_index + 1) % len(self.receivers)
