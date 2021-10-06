import inspect
import re
from typing import Any, Dict, List, Optional, Pattern

from proto.edu.uci.ics.amber.engine.architecture.worker import EvaluatedValue, TypedValue


class ExpressionEvaluator:
    """
    Provides a series of static evaluation methods of a given expression, with an optional context.
    """

    @staticmethod
    def evaluate(expression: str, runtime_context: Optional[Dict[str, Any]] = None) -> EvaluatedValue:

        value = eval(expression, runtime_context)
        value_str = repr(value)
        type_str = type(value).__name__

        attributes = list()

        # add attributes
        attributes += ExpressionEvaluator._extract_attributes(value)

        if ExpressionEvaluator._is_iterable(value):
            if ExpressionEvaluator._is_generator(value):
                attributes += ExpressionEvaluator._extract_generator_locals(value)
            else:
                attributes += ExpressionEvaluator._extract_container_items(value)
        return EvaluatedValue(
            value=TypedValue(
                expression=expression,
                value_ref=expression,
                value_str=value_str,
                value_type=type_str,
                expandable=ExpressionEvaluator._is_expandable(value)
            ),
            attributes=attributes
        )

    @staticmethod
    def _is_expandable(obj, parent=None) -> bool:
        # for set and set-like subclasses, the internal values cannot be expanded easily, disable for now
        return not isinstance(parent, set) and (ExpressionEvaluator._contains_attributes(obj) or \
                                                (ExpressionEvaluator._is_iterable(
                                                    obj) and not ExpressionEvaluator._is_empty_container(obj)))

    @staticmethod
    def _is_generator(obj) -> bool:
        return inspect.isgenerator(obj)

    @staticmethod
    def _is_iterable(obj) -> bool:
        """
        According to https://www.pythonlikeyoumeanit.com/Module2_EssentialsOfPython/Iterables.html#Iterables,
        an iterable is any Python object with an __iter__() method or with a __getitem__() method that
        implements Sequence semantics.
        """
        return hasattr(obj, "__iter__") or hasattr(obj, "__getitem__")

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
        for i, item in enumerate(value):
            contained_items.append(
                TypedValue(
                    expression=f"__getitem__({i})",
                    value_ref=repr(i),
                    value_str=repr(item),
                    value_type=type(item).__name__,
                    expandable=ExpressionEvaluator._is_expandable(item, parent=value)
                )
            )
        return contained_items

    @staticmethod
    def _extract_attributes(value: Any) -> List[TypedValue]:
        attributes = []
        if hasattr(value, "__dict__"):
            for k, v in vars(value).items():
                attributes.append(
                    TypedValue(
                        expression=k,
                        value_ref=k,
                        value_str=repr(v),
                        value_type=type(v).__name__,
                        expandable=ExpressionEvaluator._is_expandable(v, parent=value)
                    )
                )
        return attributes

    @staticmethod
    def _extract_generator_locals(value: Any) -> List[TypedValue]:
        locals = []
        for k, v in filter(lambda t: t[0] != '.0', inspect.getgeneratorlocals(value).items()):
            locals.append(
                TypedValue(
                    expression=k,
                    value_ref=k,
                    value_str=repr(v),
                    value_type=type(v).__name__,
                    expandable=False
                )
            )
        return locals
