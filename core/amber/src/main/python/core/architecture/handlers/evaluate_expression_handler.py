from proto.edu.uci.ics.amber.engine.architecture.worker import (
    EvaluateExpressionV2,
    EvaluatedValue,
)
from .handler_base import Handler
from ..managers.context import Context
from ...util.expression_evaluator import ExpressionEvaluator


class EvaluateExpressionHandler(Handler):
    cmd = EvaluateExpressionV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        runtime_context = {
            r"self": context.dp._operator,
            r"tuple_": context.dp._current_input_tuple,
            r"input_": context.dp._current_input_link,
        }

        evaluated_value: EvaluatedValue = ExpressionEvaluator.evaluate(
            command.expression, runtime_context
        )

        return evaluated_value
