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
    HashBasedShufflePartitioning,
)


class HashBasedShufflePartitioner(IndexedShufflePartitioner):
    def __init__(self, partitioning: HashBasedShufflePartitioning):
        super().__init__(partitioning)
        logger.debug(f"got {partitioning}")
        # Indexed by hash_code to choose the downstream worker to send to.
        self.hash_attribute_names = partitioning.hash_attribute_names

    @overrides
    def _route(self, tuple_: Tuple) -> int:
        partial_tuple = (
            tuple_
            if not self.hash_attribute_names
            else tuple_.get_partial_tuple(self.hash_attribute_names)
        )
        return hash(partial_tuple) % len(self.receivers)
