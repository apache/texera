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

from .schema import Schema
from .state import State
from .tuple import Tuple


class StateStorage:
    """Two-column wire/storage format for a ``State`` and its ``loop_counter``.

    ``loop_counter`` is loop-control bookkeeping owned by the worker runtime,
    not part of the user ``State``. In memory it rides on the ``StateFrame``
    envelope; whenever state is serialized (network) or materialized (the state
    storage table) it is written as its own ``loop_counter`` column parallel to
    ``content`` so it never enters the user state JSON.

    This is the single source of truth for the two-column layout. The Scala
    ``StateStorage`` object must stay byte-for-byte in sync (same column names,
    order, and types), since the same state table is written/read by both.
    """

    CONTENT = "content"
    LOOP_COUNTER = "loop_counter"
    SCHEMA = Schema(raw_schema={CONTENT: "STRING", LOOP_COUNTER: "LONG"})

    @staticmethod
    def to_tuple(state: State, loop_counter: int) -> Tuple:
        return Tuple(
            {
                StateStorage.CONTENT: state.to_json(),
                StateStorage.LOOP_COUNTER: int(loop_counter),
            },
            schema=StateStorage.SCHEMA,
        )

    @staticmethod
    def from_tuple(row: Tuple) -> "tuple[State, int]":
        return (
            State.from_json(row[StateStorage.CONTENT]),
            int(row[StateStorage.LOOP_COUNTER]),
        )
