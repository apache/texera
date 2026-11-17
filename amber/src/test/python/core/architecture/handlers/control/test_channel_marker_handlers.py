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

from core.architecture.handlers.control.end_channel_handler import EndChannelHandler
from core.architecture.handlers.control.start_channel_handler import (
    StartChannelHandler,
)
from core.architecture.packaging.input_manager import InputManager
from core.models import Schema
from core.models.internal_marker import EndChannel, StartChannel
from core.models.internal_queue import InternalQueue
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
    PortIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import EmptyRequest

WORKER_ID = "worker-1"
UPSTREAM_ID = ActorVirtualIdentity("upstream-worker")


def _channel(index: int) -> ChannelIdentity:
    """Return a distinct data-channel identity."""
    return ChannelIdentity(
        ActorVirtualIdentity(f"{UPSTREAM_ID.name}-{index}"),
        ActorVirtualIdentity(WORKER_ID),
        False,
    )


def _build_context(*port_numbers: int):
    """Build real input state and the minimal marker-handler context."""
    manager = InputManager(WORKER_ID, InternalQueue())
    channels = []
    ports = []
    for index, port_number in enumerate(port_numbers):
        channel = _channel(index)
        port = PortIdentity(port_number, False)
        manager.add_input_port(port, Schema(), [], [])
        manager.register_input(channel, port)
        channels.append(channel)
        ports.append(port)
    context = SimpleNamespace(
        input_manager=manager,
        current_input_channel_id=channels[-1] if channels else _channel(99),
        tuple_processing_manager=SimpleNamespace(current_internal_marker=None),
    )
    return context, channels, ports


@pytest.mark.parametrize(
    ("handler_type", "method_name", "marker_type"),
    [
        (StartChannelHandler, "start_channel", StartChannel),
        (EndChannelHandler, "end_channel", EndChannel),
    ],
)
def test_channel_marker_snapshots_the_current_ports_identity(
    handler_type, method_name, marker_type
):
    """Each channel retains its own port."""
    context, channels, ports = _build_context(3, 7)
    handler = handler_type(context)

    context.current_input_channel_id = channels[1]
    asyncio.run(getattr(handler, method_name)(EmptyRequest()))

    assert context.tuple_processing_manager.current_internal_marker == marker_type(
        ports[1].id
    )
    assert context.input_manager.get_port(ports[0]).completed is False
    assert context.input_manager.get_port(ports[1]).completed is (
        handler_type is EndChannelHandler
    )


@pytest.mark.parametrize(
    ("handler_type", "method_name"),
    [
        (StartChannelHandler, "start_channel"),
        (EndChannelHandler, "end_channel"),
    ],
)
def test_unknown_channel_leaves_marker_and_known_ports_untouched(
    handler_type, method_name
):
    """A missing channel mapping must fail before producing any side effect."""
    context, _, ports = _build_context(3)
    context.current_input_channel_id = _channel(99)

    with pytest.raises(KeyError):
        asyncio.run(getattr(handler_type(context), method_name)(EmptyRequest()))

    assert context.tuple_processing_manager.current_internal_marker is None
    assert context.input_manager.get_port(ports[0]).completed is False


def test_invalid_end_channel_port_fails_before_completing_the_port():
    """An invalid marker must not complete its port."""
    context, _, ports = _build_context(-1)

    with pytest.raises(ValueError, match="nonnegative integer"):
        asyncio.run(EndChannelHandler(context).end_channel(EmptyRequest()))

    assert context.tuple_processing_manager.current_internal_marker is None
    assert context.input_manager.get_port(ports[0]).completed is False
