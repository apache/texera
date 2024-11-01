from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.util.expression_evaluator import ExpressionEvaluator
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EvaluatedValue,
    EvaluatePythonExpressionRequest,
)


class EvaluateExpressionHandler(ControlHandler):

    async def evaluate_python_expression(
        self, req: EvaluatePythonExpressionRequest
    ) -> EvaluatedValue:
        runtime_context = {
            r"self": self.context.executor_manager.executor,
            r"tuple_": self.context.tuple_processing_manager.current_input_tuple,
            r"input_": self.context.tuple_processing_manager.current_input_port_id,
        }

        evaluated_value: EvaluatedValue = ExpressionEvaluator.evaluate(
            req.expression, runtime_context
        )

        return evaluated_value
