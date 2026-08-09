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
from overrides import overrides
from typing import Iterator

from core.architecture.sendsemantics.partitioner import Partitioner
from core.models import Tuple
from core.models.state import State
from core.util import set_one_of
from proto.org.apache.texera.amber.core import ActorVirtualIdentity
from proto.org.apache.texera.amber.engine.architecture.rpc import EmbeddedControlMessage
from proto.org.apache.texera.amber.engine.architecture.sendsemantics import (
    Partitioning,
    SignalPartitioning,
)


class SignalPartitioner(Partitioner):
    """Partitioner for signal links (try/catch frame wiring).

    Data tuples are dropped at the sender -- never batched, serialized, or
    networked -- while States and ECMs still travel, because a frame's gate
    only needs the error signal and the end-of-stream marker. Mirror of the
    Scala `SignalPartitioner`.
    """

    def __init__(self, partitioning: SignalPartitioning):
        super().__init__(set_one_of(Partitioning, partitioning))
        self.receivers = list(
            {channel.to_worker_id for channel in partitioning.channels}
        )

    @overrides
    def add_tuple_to_batch(
        self, tuple_: Tuple
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, typing.List[Tuple]]]:
        # tuples never traverse a signal link
        return iter(())

    @overrides
    def flush(
        self, to: ActorVirtualIdentity, ecm: EmbeddedControlMessage
    ) -> Iterator[typing.Union[EmbeddedControlMessage, typing.List[Tuple]]]:
        for receiver in self.receivers:
            if receiver == to:
                yield ecm

    @overrides
    def flush_state(
        self, state: State
    ) -> Iterator[
        typing.Tuple[ActorVirtualIdentity, typing.Union[State, typing.List[Tuple]]]
    ]:
        for receiver in self.receivers:
            yield receiver, state

    @overrides
    def reset(self) -> None:
        pass
