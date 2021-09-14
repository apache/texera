import re
from typing import Any, Dict, List, Optional, Pattern

from proto.edu.uci.ics.amber.engine.architecture.worker import EvaluatedValue, TypedValue


class RuntimeContext:
    """
    A context container for explicit exposure of defined attributes.
    It also provides a regex-based mapping to inject any expressions within the context.
    """

    def __init__(self, elements: Dict[str, Any]):
        for name, value in elements.items():
            self.__setattr__(name, value)

    def to_mapping(self) -> Dict[Pattern[str], str]:
        return {re.compile(name): f"runtime_context.{name}" for name in self.__dict__.keys()}


class ExpressionEvaluator:
    """
    Provides a series of static evaluation methods of a given expression, with an optional context.
    """

    @staticmethod
    def evaluate(expression: str, runtime_context: Optional[RuntimeContext] = None) -> EvaluatedValue:
        if runtime_context:
            assert isinstance(runtime_context, RuntimeContext), \
                "needs a valid RuntimeContext to evaluate mapped expression"

            context_mapping = runtime_context.to_mapping()
            contextualized_expression = ExpressionEvaluator._contextualize_expression(expression, context_mapping)
            value = eval(contextualized_expression)
        else:
            value = eval(expression)

        value_str = repr(value)
        type_str = type(value).__name__

        attributes = list()

        # add attributes
        attributes += ExpressionEvaluator._extract_attributes(value)

        # add container items
        attributes += ExpressionEvaluator._extract_container_items(value)
        return EvaluatedValue(
            value=TypedValue(
                expression=expression,
                value_ref=expression,
                value_str=value_str,
                value_type=type_str,
                expandable=ExpressionEvaluator._is_expandable(value)),
            attributes=attributes
        )

    @staticmethod
    def _is_expandable(obj) -> bool:
        return ExpressionEvaluator._contains_attributes(obj) or \
               (ExpressionEvaluator._is_iterable(obj) and not ExpressionEvaluator._is_empty_container(obj))

    @staticmethod
    def _is_iterable(obj) -> bool:
        return hasattr(obj, "__iter__")

    @staticmethod
    def _contains_attributes(obj) -> bool:
        return hasattr(obj, "__dict__") and len(obj.__dict__) > 0

    @staticmethod
    def _is_empty_container(obj) -> bool:
        return hasattr(obj, "__len__") and len(obj) == 0

    @staticmethod
    def _contextualize_expression(expression: str, context_replacements: Dict[Pattern[str], str]) -> str:

        contextualized_expression = expression
        for pattern, contextualized_pattern in context_replacements.items():
            contextualized_expression = re.sub(pattern, contextualized_pattern, contextualized_expression)
        return contextualized_expression

    @staticmethod
    def _extract_container_items(value: Any) -> List[TypedValue]:
        contained_items = []
        if ExpressionEvaluator._is_iterable(value):
            if hasattr(value, "items"):
                for k, v in value.items():
                    contained_items.append(
                        TypedValue(
                            expression=f"__getitem__({repr(k)})",
                            value_ref=repr(k),
                            value_str=repr(v),
                            value_type=type(v).__name__,
                            expandable=ExpressionEvaluator._is_expandable(v)
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
                            expandable=ExpressionEvaluator._is_expandable(value_tuple[i])
                        )
                    )
        return contained_items

    @staticmethod
    def _extract_attributes(value: Any) -> List[TypedValue]:
        attributes = []
        if hasattr(value, "__dict__"):
            for k, v in value.__dict__.items():
                attributes.append(
                    TypedValue(
                        expression=k,
                        value_ref=k,
                        value_str=repr(v),
                        value_type=type(v).__name__,
                        expandable=ExpressionEvaluator._is_expandable(v)
                    )
                )
        return attributes
