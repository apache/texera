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

from types import SimpleNamespace

import pytest

from core.architecture.handlers.control.debug_command_handler import (
    WorkerDebugCommandHandler,
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
