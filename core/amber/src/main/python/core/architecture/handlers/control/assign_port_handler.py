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

from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.models import Schema
from core.util.virtual_identity import get_from_actor_id_for_input_port_storage
from proto.edu.uci.ics.amber.core import ActorVirtualIdentity, ChannelIdentity
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    AssignPortRequest,
)


class AssignPortHandler(ControlHandler):

    async def assign_port(self, req: AssignPortRequest) -> EmptyReturn:
        if req.input:
            self.context.input_manager.add_input_port(
                req.port_id,
                Schema(raw_schema=req.schema),
                req.storage_uris,
                req.partitionings,
            )
            for uri in req.storage_uris:
                to_actor_id = ActorVirtualIdentity(self.context.worker_id)
                from_actor_id = get_from_actor_id_for_input_port_storage(
                    uri, to_actor_id
                )
                channel_id = ChannelIdentity(from_actor_id, to_actor_id, False)
                self.context.input_manager.register_input(
                    channel_id=channel_id, port_id=req.port_id
                )
        else:
            storage_uri = None
            if len(req.storage_uris) > 0 and req.storage_uris[0]:
                storage_uri = req.storage_uris[0]
            self.context.output_manager.add_output_port(
                req.port_id, Schema(raw_schema=req.schema), storage_uri
            )
        return EmptyReturn()
