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

from core.architecture.handlers.control.initialize_executor_handler import (
    InitializeExecutorHandler,
)
from proto.org.apache.texera.amber.core import OpExecInitInfo, OpExecWithCode
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmptyReturn,
    InitializeExecutorRequest,
)


def make_request(**kwargs) -> InitializeExecutorRequest:
    """Build an InitializeExecutorRequest carrying inline Python code."""
    return InitializeExecutorRequest(
        total_worker_count=1,
        op_exec_init_info=OpExecInitInfo(
            op_exec_with_code=OpExecWithCode(code="# code", language="python")
        ),
        is_source=False,
        **kwargs,
    )


def make_handler() -> InitializeExecutorHandler:
    """Wire a handler with a SimpleNamespace context exposing the fields the
    handler writes: executor_manager and loop_start_state_uris."""
    context = SimpleNamespace(
        executor_manager=MagicMock(),
        loop_start_state_uris={},
    )
    return InitializeExecutorHandler(context)


class TestInitializeExecutorHandler:
    def test_returns_empty_return_and_initializes_executor(self):
        handler = make_handler()
        result = asyncio.run(handler.initialize_executor(make_request()))
        assert isinstance(result, EmptyReturn)
        handler.context.executor_manager.initialize_executor.assert_called_once_with(
            "# code", False, "python"
        )

    def test_stores_loop_start_state_uris_on_context(self):
        # The loop-back write addresses (LoopStart op id -> its input port's
        # state URI) are per-operator setup config delivered on this RPC; the
        # handler must expose them on the context for a Loop End's
        # _jump_to_loop_start to select by the frame-carried loop_start_id.
        handler = make_handler()
        asyncio.run(
            handler.initialize_executor(
                make_request(loop_start_state_uris={"loop-start-1": "vfs:///x/state"})
            )
        )
        assert handler.context.loop_start_state_uris == {
            "loop-start-1": "vfs:///x/state"
        }

    def test_defaults_to_empty_map_for_plans_without_loops(self):
        # betterproto defaults an absent map field to {}; the handler must
        # store that default rather than leaving stale config behind (a
        # recreated worker re-runs initialize_executor every region run).
        handler = make_handler()
        handler.context.loop_start_state_uris = {"stale": "vfs:///old"}
        asyncio.run(handler.initialize_executor(make_request()))
        assert handler.context.loop_start_state_uris == {}
