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
from proto.edu.uci.ics.amber.core import ActorVirtualIdentity, ChannelIdentity
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmbeddedControlMessage,
    EmbeddedControlMessageType,
)


class EmbeddedControlMessageManager:
    def __init__(self, actor_id: ActorVirtualIdentity, input_gateway):
        self.actor_id = actor_id
        self.input_gateway = input_gateway
        self.ecm_received: Dict[str, Set[ChannelIdentity]] = defaultdict(set)

    def is_ecm_aligned(
        self, from_channel: ChannelIdentity, ecm: EmbeddedControlMessage
    ) -> bool:
        """
        Checks whether an ECM has been received from all expected
        input channels, determining whether further processing can proceed.

        Args:
            from_channel (ChannelIdentity): The channel from which the ECM was received.
            ecm (EmbeddedControlMessage): The ECM payload containing its type and scope.

        Returns:
            bool: True if the ECM is considered aligned and processing can
                  continue, False otherwise.
        """

        self.ecm_received[ecm.id].add(from_channel)
        ecm_received_from_all_channels = self.get_channels_within_scope(ecm).issubset(
            self.ecm_received[ecm.id]
        )

        if ecm.ecm_type == EmbeddedControlMessageType.ALL_ALIGNMENT:
            ecm_completed = ecm_received_from_all_channels
        elif ecm.ecm_type == EmbeddedControlMessageType.PORT_ALIGNMENT:
            port_id = self.input_gateway.get_port_id(from_channel)
            ecm_completed = (
                self.input_gateway.get_port(port_id)
                .get_channels()
                .issubset(self.ecm_received[ecm.id])
            )
        elif ecm.ecm_type == EmbeddedControlMessageType.NO_ALIGNMENT:
            ecm_completed = (
                len(self.ecm_received[ecm.id]) == 1
            )  # Only the first ECM triggers
        else:
            raise ValueError(f"Unsupported ECM type: {ecm.ecm_type}")

        if ecm_received_from_all_channels:
            del self.ecm_received[ecm.id]  # Clean up if all ECMs are received

        return ecm_completed

    def get_channels_within_scope(
        self, ecm: EmbeddedControlMessage
    ) -> Dict["ChannelIdentity", "Channel"].keys:
        if ecm.scope:
            upstreams = {
                channel_id
                for channel_id in ecm.scope
                if channel_id.to_worker_id == self.actor_id
            }
            return self.input_gateway.get_all_channel_ids() & upstreams
        return self.input_gateway.get_all_data_channel_ids()
