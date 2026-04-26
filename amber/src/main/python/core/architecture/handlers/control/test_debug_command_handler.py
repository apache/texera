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

from core.architecture.handlers.control.debug_command_handler import (
    WorkerDebugCommandHandler,
)
from core.architecture.managers.pause_manager import PauseType
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    DebugCommandRequest,
    EmptyReturn,
)


class TestTranslateDebugCommand:
    @pytest.fixture
    def context(self):
        return SimpleNamespace(
            executor_manager=SimpleNamespace(operator_module_name="my_udf")
        )

    def test_break_with_lineno_prepends_module(self, context):
        assert (
            WorkerDebugCommandHandler.translate_debug_command("b 5", context)
            == "b my_udf:5"
        )

    def test_long_break_with_lineno_prepends_module(self, context):
        assert (
            WorkerDebugCommandHandler.translate_debug_command("break 12", context)
            == "break my_udf:12"
        )

    def test_break_preserves_condition_arg(self, context):
        assert (
            WorkerDebugCommandHandler.translate_debug_command(
                "b 7 x > 0", context
            )
            == "b my_udf:7 x > 0"
        )

    def test_break_with_no_args_passes_through(self, context):
        # No args → falls through to the else branch (no module rewriting).
        assert (
            WorkerDebugCommandHandler.translate_debug_command("b", context) == "b"
        )

    def test_non_break_command_passes_through(self, context):
        assert (
            WorkerDebugCommandHandler.translate_debug_command("n", context) == "n"
        )

    def test_non_break_command_with_args_is_rejoined(self, context):
        assert (
            WorkerDebugCommandHandler.translate_debug_command(
                "p some_var", context
            )
            == "p some_var"
        )

    def test_leading_and_trailing_whitespace_is_stripped(self, context):
        assert (
            WorkerDebugCommandHandler.translate_debug_command("  c  ", context)
            == "c"
        )

    def test_internal_whitespace_is_collapsed_to_single_space(self, context):
        # split() with no args collapses any run of whitespace, so the rejoined
        # form has single spaces regardless of how many the user typed.
        assert (
            WorkerDebugCommandHandler.translate_debug_command(
                "p   foo    bar", context
            )
            == "p foo bar"
        )

    def test_break_with_only_lineno_has_no_trailing_space(self, context):
        # The implementation joins the (empty) tail with " "; the final strip()
        # must remove the trailing whitespace so the command stays valid pdb.
        result = WorkerDebugCommandHandler.translate_debug_command("b 5", context)
        assert result == "b my_udf:5"
        assert not result.endswith(" ")


class TestDebugCommandAsyncFlow:
    @pytest.fixture
    def handler(self):
        # ControlHandler.__init__ just stashes context; bypass the protobuf
        # base class' __init__ by constructing via __new__.
        instance = WorkerDebugCommandHandler.__new__(WorkerDebugCommandHandler)
        instance.context = SimpleNamespace(
            executor_manager=SimpleNamespace(operator_module_name="my_udf"),
            debug_manager=MagicMock(),
            pause_manager=MagicMock(),
        )
        return instance

    def test_translates_then_forwards_to_debug_manager(self, handler):
        asyncio.run(handler.debug_command(DebugCommandRequest(cmd="b 5")))
        handler.context.debug_manager.put_debug_command.assert_called_once_with(
            "b my_udf:5"
        )

    def test_resumes_all_three_pause_types(self, handler):
        asyncio.run(handler.debug_command(DebugCommandRequest(cmd="c")))
        actual = [
            call.args[0]
            for call in handler.context.pause_manager.resume.call_args_list
        ]
        assert actual == [
            PauseType.USER_PAUSE,
            PauseType.EXCEPTION_PAUSE,
            PauseType.DEBUG_PAUSE,
        ]

    def test_returns_empty_return(self, handler):
        result = asyncio.run(
            handler.debug_command(DebugCommandRequest(cmd="n"))
        )
        assert isinstance(result, EmptyReturn)

    def test_passes_through_non_break_command_unchanged(self, handler):
        asyncio.run(handler.debug_command(DebugCommandRequest(cmd="p x")))
        handler.context.debug_manager.put_debug_command.assert_called_once_with(
            "p x"
        )
