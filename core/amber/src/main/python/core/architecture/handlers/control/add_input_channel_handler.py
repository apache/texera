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
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    AddInputChannelRequest,
)


class AddInputChannelHandler(ControlHandler):
    async def add_input_channel(self, req: AddInputChannelRequest) -> EmptyReturn:
        if not req.channel_id.is_control:
            # Explicitly set is_control to trigger lazy computation.
            # If not set, it may be computed at different times,
            # causing hash inconsistencies.
            req.channel_id.is_control = False
        self.context.input_manager.register_input(req.channel_id, req.port_id)
        return EmptyReturn()
