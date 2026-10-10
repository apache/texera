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

from core.architecture.handlers.control.open_executor_handler import (
    OpenExecutorHandler,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmptyReturn,
    EmptyRequest,
)


def make_handler() -> OpenExecutorHandler:
    """Wire a handler with a SimpleNamespace context exposing executor_manager."""
    executor_manager = MagicMock()
    context = SimpleNamespace(executor_manager=executor_manager)
    return OpenExecutorHandler(context)


class TestOpenExecutorHandler:
    def test_opens_the_current_executor(self):
        handler = make_handler()
        asyncio.run(handler.open_executor(EmptyRequest()))
        executor = handler.context.executor_manager.executor
        executor.open.assert_called_once_with()
        # `open` is the only lifecycle call this handler is allowed to make;
        # pin that it does not also close or otherwise disturb the executor.
        executor.close.assert_not_called()

    def test_returns_empty_return(self):
        handler = make_handler()
        result = asyncio.run(handler.open_executor(EmptyRequest()))
        assert isinstance(result, EmptyReturn)
