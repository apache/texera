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
from unittest.mock import patch

import pytest

from core.architecture.handlers.control.evaluate_expression_handler import (
    EvaluateExpressionHandler,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EvaluatedValue,
    EvaluatePythonExpressionRequest,
    TypedValue,
)


class TestEvaluateExpressionHandler:
    @pytest.fixture
    def executor(self):
        # A stand-in for the user's UDF instance — anything addressable as
        # `self` from the evaluated expression will do.
        return SimpleNamespace(state="alive")

    @pytest.fixture
    def handler(self, executor):
        instance = EvaluateExpressionHandler.__new__(EvaluateExpressionHandler)
        instance.context = SimpleNamespace(
            executor_manager=SimpleNamespace(executor=executor),
            tuple_processing_manager=SimpleNamespace(
                current_input_tuple={"col": 42},
                current_input_port_id="port-0",
            ),
        )
        return instance

    def test_returns_what_the_evaluator_returns(self, handler):
        sentinel = EvaluatedValue(
            value=TypedValue(expression="1+1", value_ref="2", value_type="int")
        )
        with patch(
            "core.architecture.handlers.control.evaluate_expression_handler"
            ".ExpressionEvaluator.evaluate",
            return_value=sentinel,
        ) as evaluate:
            result = asyncio.run(
                handler.evaluate_python_expression(
                    EvaluatePythonExpressionRequest(expression="1+1")
                )
            )

        assert result is sentinel
        evaluate.assert_called_once()

    def test_runtime_context_exposes_self_tuple_input(self, handler, executor):
        with patch(
            "core.architecture.handlers.control.evaluate_expression_handler"
            ".ExpressionEvaluator.evaluate",
            return_value=EvaluatedValue(),
        ) as evaluate:
            asyncio.run(
                handler.evaluate_python_expression(
                    EvaluatePythonExpressionRequest(expression="self.state")
                )
            )

        expression, runtime_context = evaluate.call_args.args
        assert expression == "self.state"
        assert runtime_context["self"] is executor
        assert runtime_context["tuple_"] == {"col": 42}
        assert runtime_context["input_"] == "port-0"

    def test_runtime_context_reflects_current_tuple_at_call_time(
        self, handler, executor
    ):
        # The handler must read the *current* tuple/port out of the context on
        # each call — not snapshot them at construction. Drive two calls with
        # different intermediate state.
        captured: list = []

        def capture(_expression, runtime_context):
            captured.append((runtime_context["tuple_"], runtime_context["input_"]))
            return EvaluatedValue()

        with patch(
            "core.architecture.handlers.control.evaluate_expression_handler"
            ".ExpressionEvaluator.evaluate",
            side_effect=capture,
        ):
            asyncio.run(
                handler.evaluate_python_expression(
                    EvaluatePythonExpressionRequest(expression="x")
                )
            )
            handler.context.tuple_processing_manager.current_input_tuple = {"col": 99}
            handler.context.tuple_processing_manager.current_input_port_id = "port-1"
            asyncio.run(
                handler.evaluate_python_expression(
                    EvaluatePythonExpressionRequest(expression="x")
                )
            )

        assert captured == [({"col": 42}, "port-0"), ({"col": 99}, "port-1")]

    def test_handles_none_input_tuple_and_port(self, handler):
        # Before the worker has received any input, current_input_tuple and
        # current_input_port_id are None. The handler must still build a
        # context (the user might be evaluating `self.foo`).
        handler.context.tuple_processing_manager.current_input_tuple = None
        handler.context.tuple_processing_manager.current_input_port_id = None
        with patch(
            "core.architecture.handlers.control.evaluate_expression_handler"
            ".ExpressionEvaluator.evaluate",
            return_value=EvaluatedValue(),
        ) as evaluate:
            asyncio.run(
                handler.evaluate_python_expression(
                    EvaluatePythonExpressionRequest(expression="self.state")
                )
            )

        _expression, runtime_context = evaluate.call_args.args
        assert runtime_context["tuple_"] is None
        assert runtime_context["input_"] is None

    def test_evaluator_exception_propagates(self, handler):
        # If the evaluator raises (bad syntax, attribute error in the user's
        # expression, etc.), the handler must not swallow it — the RPC layer
        # is responsible for surfacing the failure to the frontend.
        with patch(
            "core.architecture.handlers.control.evaluate_expression_handler"
            ".ExpressionEvaluator.evaluate",
            side_effect=AttributeError("no such attribute"),
        ):
            with pytest.raises(AttributeError, match="no such attribute"):
                asyncio.run(
                    handler.evaluate_python_expression(
                        EvaluatePythonExpressionRequest(expression="self.missing")
                    )
                )
