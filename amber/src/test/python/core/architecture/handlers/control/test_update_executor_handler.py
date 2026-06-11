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

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from core.architecture.handlers.control.update_executor_handler import (
    UpdateExecutorHandler,
)
from proto.org.apache.texera.amber.core import OpExecInitInfo, OpExecWithCode
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmptyReturn,
    UpdateExecutorRequest,
)


def make_request(code: str) -> UpdateExecutorRequest:
    """Build an UpdateExecutorRequest carrying inline Python code."""
    return UpdateExecutorRequest(
        new_exec_init_info=OpExecInitInfo(op_exec_with_code=OpExecWithCode(code=code))
    )


def make_handler(executor_is_source: bool = False) -> UpdateExecutorHandler:
    """Wire a handler with a SimpleNamespace context exposing executor_manager."""
    executor_manager = MagicMock()
    executor_manager.executor = SimpleNamespace(is_source=executor_is_source)
    context = SimpleNamespace(executor_manager=executor_manager)
    handler = UpdateExecutorHandler(context)
    return handler


class TestUpdateExecutorHandler:
    def test_returns_empty_return(self):
        handler = make_handler(executor_is_source=False)
        result = asyncio.run(handler.update_executor(make_request("# code")))
        assert isinstance(result, EmptyReturn)

    def test_delegates_extracted_code_to_executor_manager(self):
        handler = make_handler(executor_is_source=False)
        asyncio.run(handler.update_executor(make_request("user-code-v2")))
        handler.context.executor_manager.update_executor.assert_called_once_with(
            "user-code-v2", False
        )

    def test_propagates_current_executor_is_source_not_request_field(self):
        # The handler passes the *current* executor's is_source flag forward,
        # not anything derived from the request payload. Pin this so a future
        # change that reads is_source from the request is reviewed.
        handler = make_handler(executor_is_source=True)
        asyncio.run(handler.update_executor(make_request("source-code")))
        handler.context.executor_manager.update_executor.assert_called_once_with(
            "source-code", True
        )

    def test_extracts_code_via_get_one_of_for_op_exec_with_code(self):
        # OpExecInitInfo is a sealed-oneof of {with_class_name, with_code,
        # source}. The handler relies on get_one_of to surface the populated
        # variant; if the request carries a different variant the handler must
        # not silently call the manager with stale data — instead the call
        # surfaces an attribute error on `.code`. Pin the contract explicitly.
        from proto.org.apache.texera.amber.core import OpExecWithClassName

        handler = make_handler(executor_is_source=False)
        request = UpdateExecutorRequest(
            new_exec_init_info=OpExecInitInfo(
                op_exec_with_class_name=OpExecWithClassName(class_name="X")
            )
        )
        with pytest.raises(AttributeError):
            asyncio.run(handler.update_executor(request))
        handler.context.executor_manager.update_executor.assert_not_called()
