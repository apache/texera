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

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from core.architecture.managers.embedded_control_message_manager import (
    EmbeddedControlMessageManager,
)
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
    EmbeddedControlMessageIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmbeddedControlMessage,
    EmbeddedControlMessageType,
)


SELF_ID = ActorVirtualIdentity(name="self")


def _channel(from_name: str, to_name: str = "self", is_control: bool = False):
    return ChannelIdentity(
        from_worker_id=ActorVirtualIdentity(name=from_name),
        to_worker_id=ActorVirtualIdentity(name=to_name),
        is_control=is_control,
    )


def _make_ecm(
    ecm_type: EmbeddedControlMessageType,
    scope=None,
) -> EmbeddedControlMessage:
    # Each call constructs a fresh `EmbeddedControlMessageIdentity(id="ecm-1")`,
    # but the dataclass-style equality means all of them hash to the same key
    # in `EmbeddedControlMessageManager.ecm_received`, so messages built from
    # different invocations still aggregate under the single "ecm-1" entry.
    return EmbeddedControlMessage(
        id=EmbeddedControlMessageIdentity(id="ecm-1"),
        ecm_type=ecm_type,
        scope=scope or [],
    )


def _gateway_with_data_channels(*data_channels: ChannelIdentity):
    """Stub InputManager that exposes only `get_all_data_channel_ids`."""
    gw = MagicMock()
    gw.get_all_data_channel_ids.return_value = set(data_channels)
    gw.get_all_channel_ids.return_value = set(data_channels)
    return gw


def _gateway_with_ports(port_layout: dict, all_channels: set):
    """Stub InputManager that supports per-port lookups for PORT_ALIGNMENT.

    `port_layout` maps PortIdentity-key (use any hashable) -> set of channels.
    `get_port_id(channel)` resolves channel -> port.
    """
    gw = MagicMock()
    channel_to_port = {ch: pid for pid, chs in port_layout.items() for ch in chs}
    gw.get_port_id.side_effect = lambda ch: channel_to_port[ch]
    gw.get_port.side_effect = lambda pid: SimpleNamespace(
        get_channels=lambda chs=port_layout[pid]: chs
    )
    gw.get_all_data_channel_ids.return_value = set(all_channels)
    gw.get_all_channel_ids.return_value = set(all_channels)
    return gw


class TestEcmAllAlignment:
    def test_returns_false_until_all_channels_received(self):
        c1, c2, c3 = _channel("a"), _channel("b"), _channel("c")
        gw = _gateway_with_data_channels(c1, c2, c3)
        mgr = EmbeddedControlMessageManager(SELF_ID, gw)
        ecm = _make_ecm(EmbeddedControlMessageType.ALL_ALIGNMENT)

        assert mgr.is_ecm_aligned(c1, ecm) is False
        assert mgr.is_ecm_aligned(c2, ecm) is False
        # The third (last) channel completes the alignment.
        assert mgr.is_ecm_aligned(c3, ecm) is True

    def test_dict_is_cleaned_up_after_full_alignment(self):
        # Pin the cleanup contract: once every expected channel has reported,
        # the per-id entry must be deleted so a recycled id cannot bleed
        # state into the next ECM round.
        c1, c2 = _channel("a"), _channel("b")
        gw = _gateway_with_data_channels(c1, c2)
        mgr = EmbeddedControlMessageManager(SELF_ID, gw)
        ecm = _make_ecm(EmbeddedControlMessageType.ALL_ALIGNMENT)

        mgr.is_ecm_aligned(c1, ecm)
        mgr.is_ecm_aligned(c2, ecm)
        assert ecm.id not in mgr.ecm_received


class TestEcmNoAlignment:
    def test_first_message_completes_subsequent_do_not(self):
        c1, c2 = _channel("a"), _channel("b")
        gw = _gateway_with_data_channels(c1, c2)
        mgr = EmbeddedControlMessageManager(SELF_ID, gw)
        ecm = _make_ecm(EmbeddedControlMessageType.NO_ALIGNMENT)

        # First channel: ecm_received={c1}, len==1 → True.
        assert mgr.is_ecm_aligned(c1, ecm) is True
        # Second channel: ecm_received={c1,c2}, len==2 → False.
        # (And on this call from_all_channels=True so the dict is dropped.)
        assert mgr.is_ecm_aligned(c2, ecm) is False
        assert ecm.id not in mgr.ecm_received


class TestEcmPortAlignment:
    def test_completes_when_a_ports_channels_have_all_arrived(self):
        a1, a2 = _channel("a1"), _channel("a2")
        b1 = _channel("b1")
        ports = {"portA": {a1, a2}, "portB": {b1}}
        gw = _gateway_with_ports(ports, all_channels={a1, a2, b1})
        mgr = EmbeddedControlMessageManager(SELF_ID, gw)
        ecm = _make_ecm(EmbeddedControlMessageType.PORT_ALIGNMENT)

        # Port A needs both a1 and a2.
        assert mgr.is_ecm_aligned(a1, ecm) is False
        assert mgr.is_ecm_aligned(a2, ecm) is True
        # Port B is single-channel, so b1 alone completes its port.
        assert mgr.is_ecm_aligned(b1, ecm) is True

    def test_unsupported_ecm_type_raises_value_error(self):
        # The `else: raise ValueError(...)` branch — guard against any new
        # enum value silently falling through.
        c1 = _channel("a")
        gw = _gateway_with_data_channels(c1)
        mgr = EmbeddedControlMessageManager(SELF_ID, gw)
        # Use a sentinel that is not one of the three known values. The enum
        # type is an IntEnum, so an unused integer won't match any branch.
        ecm = _make_ecm(EmbeddedControlMessageType.ALL_ALIGNMENT)
        ecm.ecm_type = 999  # type: ignore[assignment]

        with pytest.raises(ValueError, match="Unsupported ECM type"):
            mgr.is_ecm_aligned(c1, ecm)


class TestEcmScope:
    def test_scope_intersects_with_all_channel_ids(self):
        # When `ecm.scope` is set, get_channels_within_scope filters the
        # gateway's known channels to only those whose `to_worker_id == self`
        # AND that appear in the gateway's `get_all_channel_ids`.
        c_in_scope = _channel("a", to_name="self")
        c_other_target = _channel("b", to_name="someone_else")
        c_not_in_gateway = _channel("c", to_name="self")

        gw = MagicMock()
        gw.get_all_channel_ids.return_value = {c_in_scope, c_other_target}
        gw.get_all_data_channel_ids.return_value = {c_in_scope, c_other_target}
        mgr = EmbeddedControlMessageManager(SELF_ID, gw)

        ecm = _make_ecm(
            EmbeddedControlMessageType.ALL_ALIGNMENT,
            scope=[c_in_scope, c_not_in_gateway],
        )

        # Only c_in_scope is in scope AND known to the gateway. After
        # receiving it, alignment should complete.
        assert mgr.is_ecm_aligned(c_in_scope, ecm) is True

    def test_no_scope_falls_back_to_all_data_channels(self):
        # When scope is empty, the manager uses get_all_data_channel_ids()
        # rather than get_all_channel_ids() — control vs data routing.
        c_data = _channel("a", is_control=False)
        c_control = _channel("b", is_control=True)

        gw = MagicMock()
        gw.get_all_data_channel_ids.return_value = {c_data}
        gw.get_all_channel_ids.return_value = {c_data, c_control}
        mgr = EmbeddedControlMessageManager(SELF_ID, gw)

        ecm = _make_ecm(EmbeddedControlMessageType.ALL_ALIGNMENT, scope=[])
        assert mgr.is_ecm_aligned(c_data, ecm) is True
