import re

from proto.edu.uci.ics.amber.engine.architecture.worker import EvaluateExpressionV2, EvaluatedValue, TypedValue
from .handler_base import Handler
from ..managers.context import Context


def is_expandable(obj) -> bool:
    return contains_attributes(obj) or (is_iterable(obj) and not is_empty_container(obj))


def is_iterable(obj) -> bool:
    return hasattr(obj, "__iter__")


def contains_attributes(obj) -> bool:
    return hasattr(obj, "__dict__") and len(obj.__dict__) > 0


def is_empty_container(obj) -> bool:
    return hasattr(obj, "__len__") and len(obj) == 0


class EvaluateExpressionHandler(Handler):
    cmd = EvaluateExpressionV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        contextualized_expression = re.sub(r'self', r'context.dp._udf_operator', command.expression)
        value = eval(contextualized_expression)
        value_str = repr(value)
        type_str = type(value).__name__

        attributes = list()

        # add attributes
        if hasattr(value, "__dict__"):
            for k, v in value.__dict__.items():
                attributes.append(TypedValue(expression=k,
                                             value_ref=k,
                                             value_str=repr(v),
                                             value_type=type(v).__name__,
                                             expandable=is_expandable(v)))

        # add container items
        if is_iterable(value):
            if hasattr(value, "items"):
                for k, v in value.items():
                    attributes.append(
                        TypedValue(expression=f"__getitem__({repr(k)})",
                                   value_ref=repr(k),
                                   value_str=repr(v),
                                   value_type=type(v).__name__,
                                   expandable=is_expandable(v)))
            else:
                value_tuple = tuple(value)
                for i, item in enumerate(value_tuple):
                    attributes.append(
                        TypedValue(expression=f"__getitem__({i})",
                                   value_ref=repr(i),
                                   value_str=repr(value_tuple[i]),
                                   value_type=type(value_tuple[i]).__name__,
                                   expandable=is_expandable(value_tuple[i])))
        return EvaluatedValue(
            value=TypedValue(expression=command.expression,
                             value_ref=command.expression,
                             value_str=value_str,
                             value_type=type_str,
                             expandable=True),
            attributes=attributes)
