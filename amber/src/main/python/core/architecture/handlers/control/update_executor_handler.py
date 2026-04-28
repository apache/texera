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
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmptyReturn,
    UpdateExecutorRequest,
)
from core.util import get_one_of
from proto.org.apache.texera.amber.core import OpExecWithCode


class UpdateExecutorHandler(ControlHandler):
    async def update_executor(self, req: UpdateExecutorRequest) -> EmptyReturn:
        op_exec_with_code: OpExecWithCode = get_one_of(req.new_exec_init_info)
        self.context.executor_manager.update_executor(
            op_exec_with_code.code, self.context.executor_manager.executor.is_source
        )
        return EmptyReturn()
