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

from proto.edu.uci.ics.amber.core import (
    ChannelIdentity,
    ActorVirtualIdentity
)
from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.packaging.input_manager import InputManager
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    EmptyRequest,
)
from core.models.internal_queue import DataElement
from core.models import MarkerFrame
from core.models.marker import EndOfInputChannel


class EndWorkerHandler(ControlHandler):

    async def end_worker(self, req: EmptyRequest) -> EmptyReturn:
        input_channel_id = (
            ChannelIdentity(InputManager.SOURCE_STARTER, ActorVirtualIdentity(self.context.worker_id), False)
            if self.context.executor_manager.executor.is_source
            else self.context.current_input_channel_id
        )

        self.context.input_queue.put(DataElement(tag=input_channel_id, payload=MarkerFrame(EndOfInputChannel())))
        return EmptyReturn()
