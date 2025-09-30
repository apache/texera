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
from core.util import get_one_of
from proto.edu.uci.ics.amber.core import OpExecWithCode
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    InitializeExecutorRequest,
)


class InitializeExecutorHandler(ControlHandler):

    async def initialize_executor(self, req: InitializeExecutorRequest) -> EmptyReturn:
        op_exec_with_code: OpExecWithCode = get_one_of(req.op_exec_init_info)
        self.context.executor_manager.initialize_executor(
            op_exec_with_code.code, req.is_source, op_exec_with_code.language
        )
        return EmptyReturn()
