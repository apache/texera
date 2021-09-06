import re

from proto.edu.uci.ics.amber.engine.architecture.worker import EvaluateExpressionV2, EvaluatedValue, TypedValue
from .handler_base import Handler
from ..managers.context import Context


class EvaluateExpressionHandler(Handler):
    cmd = EvaluateExpressionV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        contextualized_expression = re.sub(r'self', r'context.dp._udf_operator', command.expression)
        value = eval(contextualized_expression)
        value_str = repr(value)
        type_str = type(value).__name__

        attributes = list()
        if hasattr(value, "__dict__"):
            for k, v in value.__dict__.items():
                attributes.append(TypedValue(expression=k, value_str=repr(v), value_type=type(v).__name__,
                                             expandable=hasattr(v, "__dict__")))

        return EvaluatedValue(
            value=TypedValue(expression=command.expression, value_str=value_str, value_type=type_str, expandable=True),
            attributes=attributes)
