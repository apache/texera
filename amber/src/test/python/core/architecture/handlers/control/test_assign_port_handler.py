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
from unittest.mock import MagicMock

import pytest

from core.architecture.handlers.control.assign_port_handler import AssignPortHandler
from core.models import Schema
from core.util.virtual_identity import get_from_actor_id_for_input_port_storage
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
    PortIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    AssignPortRequest,
    EmptyReturn,
)
from proto.org.apache.texera.amber.engine.architecture.sendsemantics import (
    OneToOnePartitioning,
    Partitioning,
)

WORKER_ID = "worker-1"

PORT_ID = PortIdentity(id=3, internal=False)

# Two attributes so ordering survives the round trip through the raw map.
RAW_SCHEMA = {"name": "STRING", "count": "INTEGER"}


def _expected_channel(uri: str) -> ChannelIdentity:
    """The channel the handler must register for one storage URI: derived
    through the same production util the handler calls, so the assertion pins
    the real MATERIALIZATION_READER_<uri><worker> identity rather than a stub."""
    to_actor_id = ActorVirtualIdentity(WORKER_ID)
    return ChannelIdentity(
        get_from_actor_id_for_input_port_storage(uri, to_actor_id),
        to_actor_id,
        False,
    )


def _make_request(**overrides) -> AssignPortRequest:
    fields = dict(
        port_id=PORT_ID,
        input=True,
        schema=RAW_SCHEMA,
        storage_uris=[],
        partitionings=[],
    )
    fields.update(overrides)
    return AssignPortRequest(**fields)


class TestAssignPortHandler:
    @pytest.fixture
    def handler(self):
        context = SimpleNamespace(
            input_manager=MagicMock(),
            output_manager=MagicMock(),
            worker_id=WORKER_ID,
        )
        return AssignPortHandler(context)

    # --- input branch -----------------------------------------------------

    def test_input_without_storage_adds_the_port_without_registering(self, handler):
        asyncio.run(handler.assign_port(_make_request()))
        handler.context.input_manager.add_input_port.assert_called_once_with(
            PORT_ID, Schema(raw_schema=RAW_SCHEMA), [], []
        )
        handler.context.input_manager.register_input.assert_not_called()

    def test_input_branch_leaves_the_output_manager_untouched(self, handler):
        asyncio.run(handler.assign_port(_make_request()))
        assert handler.context.output_manager.mock_calls == []

    def test_input_passes_storage_uris_and_partitionings_verbatim(self, handler):
        uris = ["vfs:///mat/a", "vfs:///mat/b"]
        partitionings = [
            Partitioning(one_to_one_partitioning=OneToOnePartitioning(batch_size=1)),
            Partitioning(one_to_one_partitioning=OneToOnePartitioning(batch_size=2)),
        ]
        request = _make_request(storage_uris=uris, partitionings=partitionings)
        asyncio.run(handler.assign_port(request))
        handler.context.input_manager.add_input_port.assert_called_once_with(
            PORT_ID, Schema(raw_schema=RAW_SCHEMA), uris, partitionings
        )

    def test_input_registers_one_channel_per_uri_in_uri_order(self, handler):
        uris = ["vfs:///mat/a", "vfs:///mat/b"]
        request = _make_request(storage_uris=uris)
        asyncio.run(handler.assign_port(request))
        register_input = handler.context.input_manager.register_input
        assert register_input.call_count == 2
        assert [call.kwargs for call in register_input.call_args_list] == [
            {"channel_id": _expected_channel(uri), "port_id": PORT_ID} for uri in uris
        ]

    def test_registered_channel_is_a_data_channel_into_this_worker(self, handler):
        uri = "vfs:///mat/a"
        asyncio.run(handler.assign_port(_make_request(storage_uris=[uri])))
        register_input = handler.context.input_manager.register_input
        channel_id = register_input.call_args.kwargs["channel_id"]
        # The reader identity is derived from the URI plus this worker's own
        # identity, and the channel carries data — not control — traffic.
        to_actor_id = ActorVirtualIdentity(WORKER_ID)
        assert channel_id.to_worker_id == to_actor_id
        assert channel_id.from_worker_id == get_from_actor_id_for_input_port_storage(
            uri, to_actor_id
        )
        assert channel_id.is_control is False

    def test_input_registers_a_channel_even_for_an_empty_string_uri(self, handler):
        # Unlike the output branch, the input loop has no truthiness guard: an
        # empty-string URI still gets a channel derived from "". The Scala
        # worker behaves the same (URI.create("") succeeds and the channel is
        # registered), so this pins a cross-language symmetry. The production
        # coordinator currently sends [] for storage-less input ports
        # (RegionExecutionManager falls back to List.empty), so this path is
        # latent wire-format space, not a live message shape.
        request = _make_request(storage_uris=[""])
        asyncio.run(handler.assign_port(request))
        handler.context.input_manager.add_input_port.assert_called_once_with(
            PORT_ID, Schema(raw_schema=RAW_SCHEMA), [""], []
        )
        handler.context.input_manager.register_input.assert_called_once_with(
            channel_id=_expected_channel(""), port_id=PORT_ID
        )

    def test_input_with_unset_storage_uris_field_iterates_as_empty(self, handler):
        # betterproto materializes an unset repeated field as [] on read, so a
        # request built without storage_uris must behave exactly like one built
        # with an explicit empty list.
        request = AssignPortRequest(port_id=PORT_ID, input=True, schema=RAW_SCHEMA)
        asyncio.run(handler.assign_port(request))
        handler.context.input_manager.add_input_port.assert_called_once_with(
            PORT_ID, Schema(raw_schema=RAW_SCHEMA), [], []
        )
        handler.context.input_manager.register_input.assert_not_called()

    # --- output branch ----------------------------------------------------

    def test_output_without_storage_uris_passes_a_none_base(self, handler):
        # The Scala twin calls storageUris.head and would throw on an empty
        # list; the Python guard checks the length first, so an empty list is
        # tolerated here even though the coordinator never sends one.
        asyncio.run(handler.assign_port(_make_request(input=False)))
        handler.context.output_manager.add_output_port.assert_called_once_with(
            PORT_ID, Schema(raw_schema=RAW_SCHEMA), None
        )

    def test_output_branch_leaves_the_input_manager_untouched(self, handler):
        asyncio.run(handler.assign_port(_make_request(input=False)))
        assert handler.context.input_manager.mock_calls == []

    def test_output_treats_an_empty_string_uri_as_no_storage(self, handler):
        # The production coordinator really sends [""] for a storage-less
        # output port (RegionExecutionManager wraps the URI lookup in
        # .getOrElse("")), so the truthiness guard collapsing "" to None is
        # live protocol, not defensive coding.
        request = _make_request(input=False, storage_uris=[""])
        asyncio.run(handler.assign_port(request))
        handler.context.output_manager.add_output_port.assert_called_once_with(
            PORT_ID, Schema(raw_schema=RAW_SCHEMA), None
        )

    def test_output_uses_the_single_uri_as_storage_base(self, handler):
        request = _make_request(input=False, storage_uris=["vfs:///x"])
        asyncio.run(handler.assign_port(request))
        handler.context.output_manager.add_output_port.assert_called_once_with(
            PORT_ID, Schema(raw_schema=RAW_SCHEMA), "vfs:///x"
        )

    def test_output_silently_drops_every_uri_after_the_first(self, handler):
        # Only storage_uris[0] is consulted; "vfs:///y" vanishes without an
        # error or a log line (current behavior).
        request = _make_request(input=False, storage_uris=["vfs:///x", "vfs:///y"])
        asyncio.run(handler.assign_port(request))
        handler.context.output_manager.add_output_port.assert_called_once_with(
            PORT_ID, Schema(raw_schema=RAW_SCHEMA), "vfs:///x"
        )

    # --- both branches ----------------------------------------------------

    @pytest.mark.parametrize("is_input", [True, False])
    def test_returns_empty_return_from_either_branch(self, handler, is_input):
        result = asyncio.run(handler.assign_port(_make_request(input=is_input)))
        assert result == EmptyReturn()

    @pytest.mark.parametrize("is_input", [True, False])
    def test_wraps_the_raw_request_schema_into_a_schema_object(self, handler, is_input):
        asyncio.run(handler.assign_port(_make_request(input=is_input)))
        manager = (
            handler.context.input_manager
            if is_input
            else handler.context.output_manager
        )
        add_port = manager.add_input_port if is_input else manager.add_output_port
        schema = add_port.call_args.args[1]
        # The manager receives a real Schema built from the request's raw
        # name -> type map, with attribute order and types preserved.
        assert isinstance(schema, Schema)
        assert schema == Schema(raw_schema=RAW_SCHEMA)
        assert schema.get_attr_names() == ["name", "count"]
