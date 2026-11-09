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

from loguru import logger
from overrides import overrides

from core.architecture.sendsemantics.indexed_shuffle_partitioner import (
    IndexedShufflePartitioner,
)
from core.models import Tuple
from proto.org.apache.texera.amber.engine.architecture.sendsemantics import (
    RangeBasedShufflePartitioning,
)


class RangeBasedShufflePartitioner(IndexedShufflePartitioner):
    def __init__(self, partitioning: RangeBasedShufflePartitioning):
        super().__init__(partitioning)
        logger.info(f"got {partitioning}")
        # Indexed by get_receiver_index to choose the downstream worker to send to.
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
    def _route(self, tuple_: Tuple) -> int:
        column_val = tuple_[self.range_attribute_names[0]]
        return self.get_receiver_index(column_val)
