import re
from typing import Any, List

from proto.edu.uci.ics.amber.engine.architecture.worker import EvaluateExpressionV2, EvaluatedValue, TypedValue
from .handler_base import Handler
from ..managers.context import Context


class ExpressionEvaluator:

    @staticmethod
    def is_expandable(obj) -> bool:
        return ExpressionEvaluator.contains_attributes(obj) or \
               (ExpressionEvaluator.is_iterable(obj) and not ExpressionEvaluator.is_empty_container(obj))

    @staticmethod
    def is_iterable(obj) -> bool:
        return hasattr(obj, "__iter__")

    @staticmethod
    def contains_attributes(obj) -> bool:
        return hasattr(obj, "__dict__") and len(obj.__dict__) > 0

    @staticmethod
    def is_empty_container(obj) -> bool:
        return hasattr(obj, "__len__") and len(obj) == 0

    @staticmethod
    def evaluate(expression: str, context: Context) -> EvaluatedValue:
        contextualized_expression = ExpressionEvaluator.contextualize_expression(expression)

        value = eval(contextualized_expression)
        value_str = repr(value)
        type_str = type(value).__name__

        attributes = list()

        # add attributes
        attributes += ExpressionEvaluator.extract_attributes(value)

        # add container items
        attributes += ExpressionEvaluator.extract_container_items(value)
        return EvaluatedValue(
            value=TypedValue(
                expression=expression,
                value_ref=expression,
                value_str=value_str,
                value_type=type_str,
                expandable=True),
            attributes=attributes
        )

    @staticmethod
    def contextualize_expression(expression: str) -> str:
        context_replacements = [
            (r'self', r'context.dp._udf_operator'),
            (r'tuple_', r'context.dp._current_input_tuple'),
            (r'link', r'context.dp._current_input_link'),

        ]
        contextualized_expression = expression
        for pattern, contextualized_pattern in context_replacements:
            contextualized_expression = re.sub(pattern, contextualized_pattern, contextualized_expression)
        return contextualized_expression

    @staticmethod
    def extract_container_items(value: Any) -> List[TypedValue]:
        contained_items = []
        if ExpressionEvaluator.is_iterable(value):
            if hasattr(value, "items"):
                for k, v in value.items():
                    contained_items.append(
                        TypedValue(
                            expression=f"__getitem__({repr(k)})",
                            value_ref=repr(k),
                            value_str=repr(v),
                            value_type=type(v).__name__,
                            expandable=ExpressionEvaluator.is_expandable(v)
                        )
                    )

            else:
                value_tuple = tuple(value)
                for i, item in enumerate(value_tuple):
                    contained_items.append(
                        TypedValue(
                            expression=f"__getitem__({i})",
                            value_ref=repr(i),
                            value_str=repr(value_tuple[i]),
                            value_type=type(value_tuple[i]).__name__,
                            expandable=ExpressionEvaluator.is_expandable(value_tuple[i])
                        )
                    )
        return contained_items

    @staticmethod
    def extract_attributes(value: Any) -> List[TypedValue]:
        attributes = []
        if hasattr(value, "__dict__"):
            for k, v in value.__dict__.items():
                attributes.append(
                    TypedValue(
                        expression=k,
                        value_ref=k,
                        value_str=repr(v),
                        value_type=type(v).__name__,
                        expandable=ExpressionEvaluator.is_expandable(v)
                    )
                )
        return attributes


class EvaluateExpressionHandler(Handler):
    cmd = EvaluateExpressionV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        evaluated_value = ExpressionEvaluator.evaluate(command.expression, context)

        return evaluated_value
