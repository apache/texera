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

from collections import defaultdict
from typing import Set, Dict

from core.architecture.packaging.input_manager import Channel
from proto.edu.uci.ics.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
    PortIdentity,
)
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    ChannelMarkerPayload,
    ChannelMarkerType,
)
from pampy import match


class ChannelMarkerManager:
    def __init__(self, actor_id: ActorVirtualIdentity, input_gateway):
        self.actor_id = actor_id
        self.input_gateway = input_gateway
        self.marker_received: Dict[str, Dict[PortIdentity, Set[ChannelIdentity]]] = (
            defaultdict(lambda: defaultdict(set))
        )

    def is_marker_aligned(
        self, from_channel: ChannelIdentity, marker: ChannelMarkerPayload
    ) -> bool:
        """
        Checks whether a channel marker has been received from all expected
        input channels, determining whether further processing can proceed.

        Args:
            from_channel (ChannelIdentity): The channel from which the marker
                was received.
            marker (ChannelMarkerPayload): The marker payload containing its
                type and scope.

        Returns:
            bool: True if the marker is considered aligned and processing can
                  continue, False otherwise.
        """
        marker_id = marker.id
        port_id = self.input_gateway.get_port_id(from_channel)
        port_map = self.marker_received[marker_id]
        port_map[port_id].add(from_channel)

        if marker.marker_type == ChannelMarkerType.ALL_ALIGNMENT:
            marker_received_from_all_channels = self.get_channels_within_scope(
                marker
            ).issubset(set().union(*port_map.values()))
            epoch_marker_completed = marker_received_from_all_channels
            if marker_received_from_all_channels:
                del self.marker_received[
                    marker_id
                ]  # Clean up if all markers are received
        elif marker.marker_type == ChannelMarkerType.PORT_ALIGNMENT:
            marker_received_from_current_port = self.get_channels_within_port(
                marker, port_id
            ).issubset(port_map[port_id])
            epoch_marker_completed = marker_received_from_current_port
            if marker_received_from_current_port:
                del port_map[port_id]
        elif marker.marker_type == ChannelMarkerType.NO_ALIGNMENT:
            epoch_marker_completed = (
                len(self.marker_received[marker_id]) == 1
            )  # Only the first marker triggers
        else:
            raise ValueError(f"Unsupported marker type: {marker.marker_type}")

        return epoch_marker_completed

    def _filter_channels_within_scope(
        self, channels: set, marker: ChannelMarkerPayload
    ) -> set:
        if marker.scope:
            upstreams = {
                channel_id
                for channel_id in marker.scope
                if channel_id.to_worker_id == self.actor_id
            }
            return channels & upstreams
        return channels

    def get_channels_within_scope(
        self, marker: ChannelMarkerPayload
    ) -> Dict["ChannelIdentity", "Channel"].keys:
        return self._filter_channels_within_scope(
            set(self.input_gateway.get_all_channel_ids()), marker
        )

    def get_channels_within_port(
        self, marker: ChannelMarkerPayload, port_id: PortIdentity
    ) -> Dict["ChannelIdentity", "Channel"].keys:
        return self._filter_channels_within_scope(
            self.input_gateway.get_port(port_id).get_channels(), marker
        )
