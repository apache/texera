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
from abc import ABC
from typing import Iterator
from betterproto import Message
from core.models import Tuple
from core.models.state import State
from core.util import get_one_of
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import Partitioning
from proto.edu.uci.ics.amber.core import ActorVirtualIdentity
from proto.edu.uci.ics.amber.engine.architecture.rpc import EmbeddedControlMessage


class Partitioner(ABC):
    def __init__(self, partitioning: Message):
        self.partitioning: Partitioning = get_one_of(partitioning)

    def add_tuple_to_batch(
        self, tuple_: Tuple
    ) -> Iterator[typing.Tuple[ActorVirtualIdentity, typing.List[Tuple]]]:
        pass

    def flush(
        self, to: ActorVirtualIdentity, ecm: EmbeddedControlMessage
    ) -> Iterator[typing.Union[EmbeddedControlMessage, typing.List[Tuple]]]:
        pass

    def flush_state(
        self, state: State
    ) -> Iterator[
        typing.Tuple[ActorVirtualIdentity, typing.Union[State, typing.List[Tuple]]]
    ]:
        pass

    def reset(self) -> None:
        pass

    def __repr__(self):
        return f"Partitioner[partitioning={self.partitioning}]"
