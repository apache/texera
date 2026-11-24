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


import asyncio
from types import SimpleNamespace

import pytest

from core.architecture.handlers.control.add_partitioning_handler import (
    AddPartitioningHandler,
)
from core.architecture.packaging.output_manager import OutputManager
from core.architecture.sendsemantics.hash_based_shuffle_partitioner import (
    HashBasedShufflePartitioner,
)
from core.architecture.sendsemantics.one_to_one_partitioner import OneToOnePartitioner
from core.util import set_one_of
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
    PhysicalLink,
    PortIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    AddPartitioningRequest,
    EmptyReturn,
)
from proto.org.apache.texera.amber.engine.architecture.sendsemantics import (
    HashBasedShufflePartitioning,
    OneToOnePartitioning,
    Partitioning,
)

WORKER_ID = "Worker:WF0-test-main-0"


def _channel(src: str, dst: str) -> ChannelIdentity:
    return ChannelIdentity(
        from_worker_id=ActorVirtualIdentity(src),
        to_worker_id=ActorVirtualIdentity(dst),
        is_control=False,
    )


def _handler() -> AddPartitioningHandler:
    return AddPartitioningHandler(
        SimpleNamespace(output_manager=OutputManager(worker_id=WORKER_ID))
    )


def _add(handler, tag: PhysicalLink, partitioning) -> EmptyReturn:
    request = AddPartitioningRequest(
        tag=tag, partitioning=set_one_of(Partitioning, partitioning)
    )
    return asyncio.run(handler.add_partitioning(request))


class TestAddPartitioningHandler:
    def test_registers_a_partitioner_for_the_link(self):
        handler = _handler()
        link = PhysicalLink(to_port_id=PortIdentity(id=0))

        result = _add(
            handler,
            link,
            OneToOnePartitioning(batch_size=10, channels=[_channel(WORKER_ID, "A")]),
        )

        assert result == EmptyReturn()
        partitioner = handler.context.output_manager._partitioners[link]
        assert isinstance(partitioner, OneToOnePartitioner)
        assert partitioner.receiver == ActorVirtualIdentity("A")

    def test_registers_only_channels_this_worker_sends_on(self):
        handler = _handler()
        mine = _channel(WORKER_ID, "A")
        others = _channel("Worker:WF0-other-main-0", "B")

        _add(
            handler,
            PhysicalLink(),
            HashBasedShufflePartitioning(
                batch_size=10, channels=[mine, others], hash_attribute_names=["k"]
            ),
        )

        assert list(handler.context.output_manager.get_output_channel_ids()) == [mine]

    def test_keeps_one_partitioner_per_link(self):
        handler = _handler()
        first = PhysicalLink(to_port_id=PortIdentity(id=0))
        second = PhysicalLink(to_port_id=PortIdentity(id=1))

        _add(
            handler,
            first,
            OneToOnePartitioning(batch_size=10, channels=[_channel(WORKER_ID, "A")]),
        )
        _add(
            handler,
            second,
            HashBasedShufflePartitioning(
                batch_size=10,
                channels=[_channel(WORKER_ID, "B"), _channel(WORKER_ID, "C")],
                hash_attribute_names=["k"],
            ),
        )

        partitioners = handler.context.output_manager._partitioners
        assert list(partitioners) == [first, second]
        assert isinstance(partitioners[first], OneToOnePartitioner)
        assert isinstance(partitioners[second], HashBasedShufflePartitioner)
        assert sorted(
            c.to_worker_id.name
            for c in handler.context.output_manager.get_output_channel_ids()
        ) == ["A", "B", "C"]

    def test_rejects_a_request_without_a_partitioning(self):
        handler = _handler()
        request = AddPartitioningRequest(
            tag=PhysicalLink(), partitioning=Partitioning()
        )
        with pytest.raises(Exception):
            asyncio.run(handler.add_partitioning(request))
        assert not handler.context.output_manager._partitioners
