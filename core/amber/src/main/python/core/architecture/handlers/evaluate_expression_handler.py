import re

from proto.edu.uci.ics.amber.engine.architecture.worker import EvaluateExpressionV2, EvaluatedValue
from .handler_base import Handler
from ..managers.context import Context


class EvaluateExpressionHandler(Handler):
    cmd = EvaluateExpressionV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        contextualized_expression = re.sub(r'self', r'context.dp._udf_operator', command.expression)
        value = eval(contextualized_expression)
        value_str = str(value)
        type_ = str(type(value))
        if isinstance(value, object):
            attributes = list(value.__dict__.keys())
        else:
            attributes = []
        return EvaluatedValue(value=value_str, expression_type=type_, attributes=attributes)
