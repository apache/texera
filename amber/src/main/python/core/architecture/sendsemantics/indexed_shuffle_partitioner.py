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
from abc import abstractmethod
from overrides import overrides
from typing import Iterator

from core.architecture.sendsemantics.partitioner import Partitioner
from core.models import Tuple
from core.models.state import State
from core.util import set_one_of
from proto.org.apache.texera.amber.core import ActorVirtualIdentity
from proto.org.apache.texera.amber.engine.architecture.rpc import EmbeddedControlMessage
from proto.org.apache.texera.amber.engine.architecture.sendsemantics import Partitioning


class IndexedShufflePartitioner(Partitioner):
    """Base for partitioners that keep one (receiver, batch) slot per downstream
    worker and route each tuple to a slot by a computed index.

    Subclasses provide the routing via ``_route`` and, if needed, advance any
    routing state via ``_advance``. ``_clear_batch_on_flush`` controls whether a
    yielded batch is cleared in place during ``flush``/``flush_state``.
    """

    # Whether flush/flush_state clear a yielded batch in place after emitting it.
    _clear_batch_on_flush = False

    def __init__(self, partitioning):
        super().__init__(set_one_of(Partitioning, partitioning))
        self.batch_size = partitioning.batch_size
        self.receivers = self.build_receiver_batches(partitioning.channels)

    @abstractmethod
    def _route(self, tuple_: Tuple) -> int:
        """Return the index of the receiver slot this tuple is routed to."""

    def _advance(self) -> None:
        """Advance any routing state after a tuple is placed. No-op by default."""

    @overrides
    def add_tuple_to_batch(
        self, tuple_: Tuple
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, typing.List[Tuple]]]:
        index = self._route(tuple_)
        receiver, batch = self.receivers[index]
        batch.append(tuple_)
        if len(batch) == self.batch_size:
            yield receiver, batch
            self.receivers[index] = (receiver, list())
        self._advance()

    @overrides
    def flush(
        self, to: ActorVirtualIdentity, ecm: EmbeddedControlMessage
    ) -> Iterator[typing.Union[EmbeddedControlMessage, typing.List[Tuple]]]:
        for receiver, batch in self.receivers:
            if receiver == to:
                if len(batch) > 0:
                    yield batch
                    if self._clear_batch_on_flush:
                        batch.clear()
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
                if self._clear_batch_on_flush:
                    batch.clear()
            yield receiver, state
