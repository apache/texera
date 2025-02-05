from collections import defaultdict
from typing import Set, Dict

from proto.edu.uci.ics.amber.core import ActorVirtualIdentity, ChannelIdentity
from proto.edu.uci.ics.amber.engine.architecture.rpc import ChannelMarkerPayload, ChannelMarkerType


class ChannelMarkerManager:
    def __init__(self, actor_id: ActorVirtualIdentity, input_gateway):
        self.actor_id = actor_id
        self.input_gateway = input_gateway
        self.marker_received: Dict[str, Set[ChannelIdentity]] = defaultdict(set)

    def is_marker_aligned(
        self, from_channel: ChannelIdentity, marker: ChannelMarkerPayload
    ):
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

    def get_channels_within_scope(self, marker: ChannelMarkerPayload):
        upstreams = {
            scope for scope in marker.scope if scope.to_worker_id == self.actor_id
        }
        return {
            channel_id
            for channel_id in self.input_gateway.get_all_channel_ids()
            if channel_id in upstreams
        }
