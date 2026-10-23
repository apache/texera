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
from typing import Optional
from unittest.mock import MagicMock, call

import pytest

from core.architecture.handlers.control.add_input_channel_handler import (
    AddInputChannelHandler,
)
from core.architecture.packaging.input_manager import InputManager
from core.models import Schema
from core.models.internal_queue import InternalQueue

# Side-effect import: patches Message.__hash__ onto betterproto's base class.
# Already pulled in transitively; explicit so a refactor of that chain fails
# loudly here.
import core.util.proto  # noqa: F401

from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
    PortIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    AddInputChannelRequest,
    EmptyReturn,
)

# Why hash() instead of ==:
# Message.__hash__ is patched to hash(repr()), and betterproto's repr omits
# fields never assigned. proto3 omits false, so a wire-parsed ChannelIdentity
# is == to its canonical form yet hashes differently -- and InputManager keys
# its channel registry by that hash (input_manager.py:66). == cannot see the
# difference this handler exists to remove; only hash() and dict lookups can.
#
# NOTE: merely READING is_control materializes the field and fixes the hash.
# Assert on pre-handler state via hash()/repr() only.

WORKER_ID = "worker-1"

UPSTREAM_ID = ActorVirtualIdentity("upstream-worker")
SELF_ID = ActorVirtualIdentity(WORKER_ID)

# Port 1 so a hardcoded PortIdentity() cannot pass; port 0 is the degenerate
# wire case with its own test below.
PORT_ID = PortIdentity(id=1, internal=False)
DEFAULT_PORT_ID = PortIdentity(id=0, internal=False)

# Canonical forms: is_control explicitly set rather than left to default.
DATA_CHANNEL = ChannelIdentity(UPSTREAM_ID, SELF_ID, False)
CONTROL_CHANNEL = ChannelIdentity(UPSTREAM_ID, SELF_ID, True)

RAW_SCHEMA = {"name": "STRING", "count": "INTEGER"}


def _request_off_the_wire(
    channel_id: ChannelIdentity, port_id: Optional[PortIdentity] = None
) -> AddInputChannelRequest:
    """Serialize and re-parse, like a real request from the Scala controller.

    proto3 does not put false on the wire, so the data channel's is_control
    comes back UNSET -- the exact input the handler's workaround exists for.
    An in-process request would arrive already canonical and prove nothing.
    """
    # Resolved here, not as a default argument (evaluated once at import):
    # sharing one identity instance across tests is a footgun in this file.
    port_id = PORT_ID if port_id is None else port_id
    return AddInputChannelRequest().parse(
        bytes(AddInputChannelRequest(channel_id, port_id))
    )


def _build_real_handler(
    port_id: Optional[PortIdentity] = None,
) -> AddInputChannelHandler:
    """Handler over a real InputManager with the port already added --
    in production AssignPort always precedes AddInputChannel."""
    port_id = PORT_ID if port_id is None else port_id
    manager = InputManager(WORKER_ID, InternalQueue())
    manager.add_input_port(port_id, Schema(raw_schema=RAW_SCHEMA), [], [])
    return AddInputChannelHandler(SimpleNamespace(input_manager=manager))


class TestAddInputChannelHandler:
    @pytest.fixture
    def handler(self):
        return AddInputChannelHandler(SimpleNamespace(input_manager=MagicMock()))

    @pytest.fixture
    def real_handler(self):
        return _build_real_handler()

    # --- delegation -------------------------------------------------------

    def test_registers_the_requested_channel_on_the_requested_port(self, handler):
        # == is blind to the materialization, so this pins only delegation
        # and argument order; the hash tests below pin the form.
        asyncio.run(handler.add_input_channel(_request_off_the_wire(DATA_CHANNEL)))
        handler.context.input_manager.register_input.assert_called_once_with(
            DATA_CHANNEL, PORT_ID
        )

    def test_register_input_is_the_only_call_on_the_input_manager(self, handler):
        asyncio.run(handler.add_input_channel(_request_off_the_wire(DATA_CHANNEL)))
        assert handler.context.input_manager.mock_calls == [
            call.register_input(DATA_CHANNEL, PORT_ID)
        ]

    def test_returns_empty_return(self, handler):
        result = asyncio.run(
            handler.add_input_channel(_request_off_the_wire(DATA_CHANNEL))
        )
        assert result == EmptyReturn()

    # --- is_control materialization ---------------------------------------

    def test_a_data_channel_loses_its_is_control_marker_on_the_wire(self):
        # Premise check: a betterproto upgrade that materializes defaults on
        # parse reports itself here, not as a silently meaningless suite.
        arrived = _request_off_the_wire(DATA_CHANNEL).channel_id
        assert arrived == DATA_CHANNEL
        assert hash(arrived) != hash(DATA_CHANNEL)

    def test_canonicalizes_the_channel_id_before_registering_it(self, handler):
        # Snapshot the hash inside register_input: the mock keeps only a
        # reference, so a post-hoc read cannot tell canonicalize-before from
        # -after -- and "after" strands the key (see the scan test below).
        seen = []
        handler.context.input_manager.register_input.side_effect = (
            lambda channel_id, port_id: seen.append(hash(channel_id))
        )
        asyncio.run(handler.add_input_channel(_request_off_the_wire(DATA_CHANNEL)))
        assert seen == [hash(DATA_CHANNEL)]

    def test_the_registered_channel_is_interchangeable_as_a_dict_key(self, handler):
        # Equal-but-differently-hashed ids miss in BOTH directions, so both
        # lookups are asserted.
        asyncio.run(handler.add_input_channel(_request_off_the_wire(DATA_CHANNEL)))
        registered = handler.context.input_manager.register_input.call_args.args[0]
        assert {registered: "channel"}.get(DATA_CHANNEL) == "channel"
        assert {DATA_CHANNEL: "channel"}.get(registered) == "channel"

    def test_a_real_input_manager_resolves_the_registered_channel(self, real_handler):
        # main_loop resolves every incoming batch through this lookup; without
        # the handler's materialization it raises KeyError on the first tuple.
        asyncio.run(real_handler.add_input_channel(_request_off_the_wire(DATA_CHANNEL)))
        assert real_handler.context.input_manager.get_port_id(DATA_CHANNEL) == PORT_ID

    def test_the_registered_channel_survives_a_data_channel_scan(self, real_handler):
        # get_all_data_channel_ids() reads is_control on every key: on a
        # non-canonical key that read changes the hash in place and strands
        # the dict entry. ECM alignment runs this scan on every marker.
        request = _request_off_the_wire(DATA_CHANNEL)
        asyncio.run(real_handler.add_input_channel(request))
        manager = real_handler.context.input_manager

        assert manager.get_all_data_channel_ids() == {DATA_CHANNEL}
        assert manager.get_port_id(request.channel_id) == PORT_ID

    # --- port identities --------------------------------------------------

    def test_registers_a_default_port_channel_in_a_real_input_manager(self):
        # Port 0, the production default, is the worst wire shape: proto3
        # omits id=0 AND internal=False, so the port arrives as a bare
        # PortIdentity() -- non-canonical in both fields, on top of the unset
        # is_control -- and _ports is keyed by PortIdentity. It registers
        # correctly only because register_input's `is None` guards READ the
        # fields; the branches never fire (betterproto yields 0/False, never
        # None). TestPortIdentityDefaults in test_input_manager.py covers the
        # hand-built None shape, which no production caller builds --
        # complementary, not duplicates.
        handler = _build_real_handler(DEFAULT_PORT_ID)

        # Canary on a throwaway request: probing must not canonicalize the
        # instance the handler receives.
        arrived = _request_off_the_wire(DATA_CHANNEL, DEFAULT_PORT_ID).port_id
        assert arrived == DEFAULT_PORT_ID
        assert hash(arrived) != hash(DEFAULT_PORT_ID)

        asyncio.run(
            handler.add_input_channel(
                _request_off_the_wire(DATA_CHANNEL, DEFAULT_PORT_ID)
            )
        )

        # One assertion suffices: a non-canonical port strands the
        # registration inside register_input itself.
        manager = handler.context.input_manager
        assert manager.get_port_id(DATA_CHANNEL) == DEFAULT_PORT_ID

    # --- control channels -------------------------------------------------

    def test_a_control_channel_keeps_its_marker_across_the_wire(self):
        # True is serialized (not the default), so a control channel arrives
        # already canonical and the guard has nothing to do.
        arrived = _request_off_the_wire(CONTROL_CHANNEL).channel_id
        assert hash(arrived) == hash(CONTROL_CHANNEL)

    def test_registers_a_control_channel_without_clearing_its_marker(self, handler):
        # Guards the `if not` condition: always assigning False would silently
        # demote every control channel to a data channel.
        asyncio.run(handler.add_input_channel(_request_off_the_wire(CONTROL_CHANNEL)))
        registered = handler.context.input_manager.register_input.call_args.args[0]
        assert registered.is_control is True
        assert hash(registered) == hash(CONTROL_CHANNEL)

    def test_a_real_input_manager_keeps_a_control_channel_out_of_the_data_set(
        self, real_handler
    ):
        asyncio.run(
            real_handler.add_input_channel(_request_off_the_wire(CONTROL_CHANNEL))
        )
        manager = real_handler.context.input_manager
        assert manager.get_all_data_channel_ids() == set()
        assert manager.get_port_id(CONTROL_CHANNEL) == PORT_ID
