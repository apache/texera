# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from core.util.expression_evaluator import ExpressionEvaluator
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EvaluatedValue,
    TypedValue,
)


def _expected_value(value_ref, value_str, value_type, expandable, *, item=False):
    return TypedValue(
        expression=f"__getitem__({value_ref})" if item else value_ref,
        value_ref=value_ref,
        value_str=value_str,
        value_type=value_type,
        expandable=expandable,
    )


class TestExpressionEvaluator:
    def test_evaluate_basic_expressions(self):
        i = 10
        assert ExpressionEvaluator.evaluate(
            "i", runtime_context={"i": i}
        ) == EvaluatedValue(
            value=_expected_value("i", "10", "int", False),
            attributes=[],
        )

        f = 1.1
        assert ExpressionEvaluator.evaluate(
            "f", runtime_context={"f": f}
        ) == EvaluatedValue(
            value=_expected_value("f", "1.1", "float", False),
            attributes=[],
        )

    def test_evaluate_str_expression(self):
        s = "hello world"
        assert ExpressionEvaluator.evaluate(
            "s", runtime_context={"s": s}
        ) == EvaluatedValue(
            value=_expected_value("s", "'hello world'", "str", True),
            attributes=[
                _expected_value("0", "'h'", "str", True, item=True),
                _expected_value("1", "'e'", "str", True, item=True),
                _expected_value("2", "'l'", "str", True, item=True),
                _expected_value("3", "'l'", "str", True, item=True),
                _expected_value("4", "'o'", "str", True, item=True),
                _expected_value("5", "' '", "str", True, item=True),
                _expected_value("6", "'w'", "str", True, item=True),
                _expected_value("7", "'o'", "str", True, item=True),
                _expected_value("8", "'r'", "str", True, item=True),
                _expected_value("9", "'l'", "str", True, item=True),
                _expected_value("10", "'d'", "str", True, item=True),
            ],
        )
        assert ExpressionEvaluator.evaluate(
            "s[4]", runtime_context={"s": s}
        ) == EvaluatedValue(
            value=_expected_value("s[4]", "'o'", "str", True),
            attributes=[_expected_value("0", "'o'", "str", True, item=True)],
        )

        assert ExpressionEvaluator.evaluate(
            "s.__getitem__(2)", runtime_context={"s": s}
        ) == EvaluatedValue(
            value=_expected_value("s.__getitem__(2)", "'l'", "str", True),
            attributes=[_expected_value("0", "'l'", "str", True, item=True)],
        )

    def test_evaluate_object_expression(self):
        class A:
            def __init__(self):
                self.i = 10
                self.j = 1.1

        a = A()

        assert ExpressionEvaluator.evaluate(
            "a", runtime_context={"a": a}
        ) == EvaluatedValue(
            value=_expected_value(
                "a",
                f"<{A.__module__}.TestExpressionEvaluator.test_evaluate_object_expression.<locals>.A object at {hex(id(a))}>",
                "A",
                True,
            ),
            attributes=[
                _expected_value("i", "10", "int", False),
                _expected_value("j", "1.1", "float", False),
            ],
        )

    def test_evaluate_container_expressions(self):
        i = 10
        f = 1.1

        a_list = [i, f, (i, f)]
        assert ExpressionEvaluator.evaluate(
            "a_list", runtime_context={"a_list": a_list}
        ) == EvaluatedValue(
            value=_expected_value("a_list", "[10, 1.1, (10, 1.1)]", "list", True),
            attributes=[
                _expected_value("0", "10", "int", False, item=True),
                _expected_value("1", "1.1", "float", False, item=True),
                _expected_value("2", "(10, 1.1)", "tuple", True, item=True),
            ],
        )
        t = (i, f, {i, f})
        assert ExpressionEvaluator.evaluate(
            "t", runtime_context={"t": t}
        ) == EvaluatedValue(
            value=_expected_value("t", "(10, 1.1, {1.1, 10})", "tuple", True),
            attributes=[
                _expected_value("0", "10", "int", False, item=True),
                _expected_value("1", "1.1", "float", False, item=True),
                _expected_value("2", "{1.1, 10}", "set", True, item=True),
            ],
        )
        s = {i, f, (i, f)}
        assert ExpressionEvaluator.evaluate(
            "s", runtime_context={"s": s}
        ) == EvaluatedValue(
            value=_expected_value("s", "{1.1, 10, (10, 1.1)}", "set", True),
            attributes=[
                _expected_value("0", "1.1", "float", False, item=True),
                _expected_value("1", "10", "int", False, item=True),
                _expected_value("2", "(10, 1.1)", "tuple", False, item=True),
            ],
        )

        d = {1: "a", "b": [{i, f}], (i,): f}
        assert ExpressionEvaluator.evaluate(
            "d", runtime_context={"d": d}
        ) == EvaluatedValue(
            value=_expected_value(
                "d", "{1: 'a', 'b': [{1.1, 10}], (10,): 1.1}", "dict", True
            ),
            attributes=[
                _expected_value("1", "'a'", "str", True, item=True),
                TypedValue(
                    expression="__getitem__('b')",
                    value_ref="'b'",
                    value_str="[{1.1, 10}]",
                    value_type="list",
                    expandable=True,
                ),
                TypedValue(
                    expression="__getitem__((10,))",
                    value_ref="(10,)",
                    value_str="1.1",
                    value_type="float",
                    expandable=False,
                ),
            ],
        )

        g = (i for i in range(10))
        assert ExpressionEvaluator.evaluate(
            "g", runtime_context={"g": g}
        ) == EvaluatedValue(
            value=_expected_value(
                "g",
                f"<generator object TestExpressionEvaluator.test_evaluate_container_expressions.<locals>.<genexpr> at {hex(id(g))}>",
                "generator",
                True,
            ),
            attributes=[],
        )

        def gen():
            for i in range(10):
                yield i

        g = gen()
        next(g)
        assert ExpressionEvaluator.evaluate(
            "g", runtime_context={"g": g}
        ) == EvaluatedValue(
            value=_expected_value(
                "g",
                f"<generator object TestExpressionEvaluator.test_evaluate_container_expressions.<locals>.gen at {hex(id(g))}>",
                "generator",
                True,
            ),
            attributes=[_expected_value("i", "0", "int", False)],
        )

        it = iter([1, 2, 3])
        assert ExpressionEvaluator.evaluate(
            "it", runtime_context={"it": it}
        ) == EvaluatedValue(
            value=_expected_value(
                "it", f"<list_iterator object at {hex(id(it))}>", "list_iterator", False
            ),
            attributes=[],
        )

        it = iter([1, 2, 3])
        next(it)
        assert ExpressionEvaluator.evaluate(
            "it", runtime_context={"it": it}
        ) == EvaluatedValue(
            value=_expected_value(
                "it", f"<list_iterator object at {hex(id(it))}>", "list_iterator", False
            ),
            attributes=[],
        )

    def test_evaluate_in_another_context(self):
        i = 10
        j = 20
        assert ExpressionEvaluator.evaluate(
            "j", runtime_context={"j": i, "i": j}
        ) == EvaluatedValue(
            value=_expected_value("j", "10", "int", False),
            attributes=[],
        )

        assert ExpressionEvaluator.evaluate(
            "i", runtime_context={"j": i, "i": j}
        ) == EvaluatedValue(
            value=_expected_value("i", "20", "int", False),
            attributes=[],
        )
