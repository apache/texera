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
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    AssignPortRequest,
)


class AssignPortHandler(ControlHandler):

    async def assign_port(self, req: AssignPortRequest) -> EmptyReturn:
        if req.input:
            self.context.input_manager.add_input_port(
                req.port_id, Schema(raw_schema=req.schema)
            )
        else:
            storage_uri = None
            if req.storage_uri != "":
                storage_uri = req.storage_uri
            self.context.output_manager.add_output_port(
                req.port_id, Schema(raw_schema=req.schema), storage_uri
            )
        return EmptyReturn()
