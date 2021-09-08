import re

from proto.edu.uci.ics.amber.engine.architecture.worker import EvaluateExpressionV2, EvaluatedValue, TypedValue
from .handler_base import Handler
from ..managers.context import Context


class EvaluateExpressionHandler(Handler):
    cmd = EvaluateExpressionV2

    @staticmethod
    def is_expandable(obj) -> bool:
        return ((hasattr(obj, "__dict__") and len(obj.__dict__) > 0) or hasattr(obj, "__getitem__")) and (
            not (hasattr(obj, "__len__") and len(obj) == 0))

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        contextualized_expression = re.sub(r'self', r'context.dp._udf_operator', command.expression)
        value = eval(contextualized_expression)
        value_str = repr(value)
        type_str = type(value).__name__

        attributes = list()

        # add attributes
        if hasattr(value, "__dict__"):
            for k, v in value.__dict__.items():
                attributes.append(TypedValue(expression=k, value_str=repr(v), value_type=type(v).__name__,
                                             expandable=self.is_expandable(v)))

        # add container items
        if hasattr(value, "__getitem__"):
            if hasattr(value, "items"):
                for k, v in value.items():
                    attributes.append(
                        TypedValue(expression=f"__getitem__({k})", value_str=repr(v),
                                   value_type=type(v).__name__,
                                   expandable=self.is_expandable(v)))
            elif hasattr(value, "__len__"):
                for i in range(len(value)):
                    attributes.append(
                        TypedValue(expression=f"__getitem__({i})", value_str=repr(value[i]),
                                   value_type=type(value[i]).__name__,
                                   expandable=self.is_expandable(value[i])))
        return EvaluatedValue(
            value=TypedValue(expression=command.expression, value_str=value_str, value_type=type_str, expandable=True),
            attributes=attributes)
