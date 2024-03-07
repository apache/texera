import inspect
import re
from collections.abc import Iterator, Mapping
from typing import Any, Dict, List, Optional, Pattern, Tuple

from proto.edu.uci.ics.amber.engine.architecture.worker import (
    EvaluatedValue,
    TypedValue,
)


class ExpressionEvaluator:
    """
    Provides a series of static evaluation methods of a given expression, with an
    optional context.
    """

    @staticmethod
    def evaluate(
        expression: str, runtime_context: Optional[Dict[str, Any]] = None
    ) -> EvaluatedValue:
        """
        Evaluates the given expression and return a EvaluatedValue.

        Right now, there is no validation performed on the input expression. User
        takes full
        responsibility of using this method.
        :param expression: a python statement string
        :param runtime_context: a Mapping of expressions to values, to be used for
            evaluation
        :return: EvaluatedValue which contains the current value and its children's
            value, all in the format of TypedValue.

            A TypedValue contains:
                - expression: str, to match the request expression being evaluated;
                - value_ref: str, the reference of this value, can be used to
                    construct the next expression which expands the current value
                    further;
                - value_str: str, the value in string format, to be displayed;
                - value_type: str, the type of this value, in string format,
                    to be displayed;
                - expandable: bool, whether this value can be expanded or not.

        The TypedValue could be expanded. For now it supports the following types:
         - Primitives (expandable = False);
         - Collections
            - Array/Tuple like (expandable = True);
            - Dict/Mapping like (expandable = True);
            - Set like (expandable = True, but its elements' expandable = False);
         - Iterables (expandable = True);
         - Iterators (expandable = False);
         - Generators (expandable = True).

         See test cases for more usage details.
        """

        value = eval(expression, runtime_context)
        value_str = repr(value)
        type_str = type(value).__name__

        to_be_expanded = list()

        if ExpressionEvaluator._has_attributes(value):
            to_be_expanded += ExpressionEvaluator._extract_attributes(value)

        if ExpressionEvaluator._is_iterable(value):
            if ExpressionEvaluator._is_generator(value):
                to_be_expanded += ExpressionEvaluator._extract_generator_locals(value)
            elif ExpressionEvaluator._is_iterator(value):
                pass
            else:
                to_be_expanded += ExpressionEvaluator._extract_container_items(value)

        return EvaluatedValue(
            value=TypedValue(
                expression=expression,
                value_ref=expression,
                value_str=value_str,
                value_type=type_str,
                expandable=ExpressionEvaluator._is_expandable(value),
            ),
            attributes=to_be_expanded,
        )

    @staticmethod
    def _has_attributes(value: Any) -> bool:
        return hasattr(value, "__dict__")

    @staticmethod
    def _is_expandable(obj, parent=None) -> bool:
        # for set and set-like subclasses, the internal values cannot be expanded
        # easily, disable for now
        return (
            not isinstance(parent, set)
            and not (
                ExpressionEvaluator._is_iterator(obj)
                and not ExpressionEvaluator._is_generator(obj)
            )
            and (
                ExpressionEvaluator._contains_attributes(obj)
                or (
                    ExpressionEvaluator._is_iterable(obj)
                    and not ExpressionEvaluator._is_empty_container(obj)
                )
            )
        )

    @staticmethod
    def _is_mapping(obj) -> bool:
        return isinstance(obj, Mapping)

    @staticmethod
    def _is_generator(obj) -> bool:
        return inspect.isgenerator(obj)

    @staticmethod
    def _is_iterator(obj) -> bool:
        return isinstance(obj, Iterator)

    @staticmethod
    def _is_iterable(obj) -> bool:
        """
        According to
        https://www.pythonlikeyoumeanit.com/Module2_EssentialsOfPython/Iterables.html#Iterables,
        an iterable is any Python object with an __iter__() method or with a
        __getitem__() method that implements Sequence semantics.
        """
        return hasattr(obj, "__iter__") or hasattr(obj, "__getitem__")

    @staticmethod
    def _contains_attributes(obj) -> bool:
        return hasattr(obj, "__dict__") and len(obj.__dict__) > 0

    @staticmethod
    def _is_empty_container(obj) -> bool:
        return hasattr(obj, "__len__") and len(obj) == 0

    @staticmethod
    def _contextualize_expression(
        expression: str, context_replacements: Dict[Pattern[str], str]
    ) -> str:
        contextualized_expression = expression
        for pattern, contextualized_pattern in context_replacements.items():
            contextualized_expression = re.sub(
                pattern, contextualized_pattern, contextualized_expression
            )
        return contextualized_expression

    @staticmethod
    def _extract_container_items(value: Any) -> List[TypedValue]:
        return ExpressionEvaluator._to_typed_values(
            (
                value.items()
                if ExpressionEvaluator._is_mapping(value)
                else enumerate(value)
            ),
            parent=value,
            to_getitem=True,
            ref_as_repr=True,
        )

    @staticmethod
    def _extract_attributes(value: Any) -> List[TypedValue]:
        return ExpressionEvaluator._to_typed_values(vars(value).items())

    @staticmethod
    def _extract_generator_locals(value: Any) -> List[TypedValue]:
        return ExpressionEvaluator._to_typed_values(
            filter(lambda t: t[0] != ".0", inspect.getgeneratorlocals(value).items()),
            check_expandable=False,
        )

    @staticmethod
    def _to_typed_values(
        kv_iter: List[Tuple[str, Any]],
        parent=None,
        to_getitem=False,
        ref_as_repr=False,
        check_expandable=True,
    ):
        return [
            TypedValue(
                expression=f"__getitem__({repr(k)})" if to_getitem else k,
                value_ref=repr(k) if ref_as_repr else k,
                value_str=repr(v),
                value_type=type(v).__name__,
                expandable=(
                    ExpressionEvaluator._is_expandable(v, parent=parent)
                    if check_expandable
                    else False
                ),
            )
            for k, v in kv_iter
        ]
