from collections import defaultdict
from typing import Set, Dict

from core.architecture.packaging.input_manager import Channel
from proto.edu.uci.ics.amber.core import ActorVirtualIdentity, ChannelIdentity
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    ChannelMarkerPayload,
    ChannelMarkerType,
)


class ChannelMarkerManager:
    def __init__(self, actor_id: ActorVirtualIdentity, input_gateway):
        self.actor_id = actor_id
        self.input_gateway = input_gateway
        self.marker_received: Dict[str, Set[ChannelIdentity]] = defaultdict(set)

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
        self.marker_received[marker_id].add(from_channel)

        marker_received_from_all_channels = self.get_channels_within_scope(
            marker
        ).issubset(self.marker_received[marker_id])

        if marker.marker_type == ChannelMarkerType.REQUIRE_ALIGNMENT:
            epoch_marker_completed = marker_received_from_all_channels
        elif marker.marker_type == ChannelMarkerType.NO_ALIGNMENT:
            epoch_marker_completed = (
                len(self.marker_received[marker_id]) == 1
            )  # Only the first marker triggers
        else:
            raise ValueError(f"Unsupported marker type: {marker.marker_type}")

        if marker_received_from_all_channels:
            del self.marker_received[marker_id]  # Clean up if all markers are received

        return epoch_marker_completed

    def get_channels_within_scope(
        self, marker: ChannelMarkerPayload
    ) -> Dict["ChannelIdentity", "Channel"].keys:
        upstreams = {
            channel_id
            for channel_id in marker.scope
            if channel_id.to_worker_id == self.actor_id
        }
        return self.input_gateway.get_all_channel_ids() & upstreams
