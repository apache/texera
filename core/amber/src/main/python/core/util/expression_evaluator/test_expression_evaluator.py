from core.util.expression_evaluator import ExpressionEvaluator
from proto.edu.uci.ics.amber.engine.architecture.rpc import EvaluatedValue, TypedValue


class TestExpressionEvaluator:
    def test_evaluate_basic_expressions(self):
        i = 10
        assert ExpressionEvaluator.evaluate(
            "i", runtime_context={"i": i}
        ) == EvaluatedValue(
            value=TypedValue(
                expression="i",
                value_ref="i",
                value_str="10",
                value_type="int",
                expandable=False,
            ),
            attributes=[],
        )

        f = 1.1
        assert ExpressionEvaluator.evaluate(
            "f", runtime_context={"f": f}
        ) == EvaluatedValue(
            value=TypedValue(
                expression="f",
                value_ref="f",
                value_str="1.1",
                value_type="float",
                expandable=False,
            ),
            attributes=[],
        )

    def test_evaluate_str_expression(self):
        s = "hello world"
        assert ExpressionEvaluator.evaluate(
            "s", runtime_context={"s": s}
        ) == EvaluatedValue(
            value=TypedValue(
                expression="s",
                value_ref="s",
                value_str="'hello world'",
                value_type="str",
                expandable=True,
            ),
            attributes=[
                TypedValue(
                    expression="__getitem__(0)",
                    value_ref="0",
                    value_str="'h'",
                    value_type="str",
                    expandable=True,
                ),
                TypedValue(
                    expression="__getitem__(1)",
                    value_ref="1",
                    value_str="'e'",
                    value_type="str",
                    expandable=True,
                ),
                TypedValue(
                    expression="__getitem__(2)",
                    value_ref="2",
                    value_str="'l'",
                    value_type="str",
                    expandable=True,
                ),
                TypedValue(
                    expression="__getitem__(3)",
                    value_ref="3",
                    value_str="'l'",
                    value_type="str",
                    expandable=True,
                ),
                TypedValue(
                    expression="__getitem__(4)",
                    value_ref="4",
                    value_str="'o'",
                    value_type="str",
                    expandable=True,
                ),
                TypedValue(
                    expression="__getitem__(5)",
                    value_ref="5",
                    value_str="' '",
                    value_type="str",
                    expandable=True,
                ),
                TypedValue(
                    expression="__getitem__(6)",
                    value_ref="6",
                    value_str="'w'",
                    value_type="str",
                    expandable=True,
                ),
                TypedValue(
                    expression="__getitem__(7)",
                    value_ref="7",
                    value_str="'o'",
                    value_type="str",
                    expandable=True,
                ),
                TypedValue(
                    expression="__getitem__(8)",
                    value_ref="8",
                    value_str="'r'",
                    value_type="str",
                    expandable=True,
                ),
                TypedValue(
                    expression="__getitem__(9)",
                    value_ref="9",
                    value_str="'l'",
                    value_type="str",
                    expandable=True,
                ),
                TypedValue(
                    expression="__getitem__(10)",
                    value_ref="10",
                    value_str="'d'",
                    value_type="str",
                    expandable=True,
                ),
            ],
        )
        assert ExpressionEvaluator.evaluate(
            "s[4]", runtime_context={"s": s}
        ) == EvaluatedValue(
            value=TypedValue(
                expression="s[4]",
                value_ref="s[4]",
                value_str="'o'",
                value_type="str",
                expandable=True,
            ),
            attributes=[
                TypedValue(
                    expression="__getitem__(0)",
                    value_ref="0",
                    value_str="'o'",
                    value_type="str",
                    expandable=True,
                )
            ],
        )

        assert ExpressionEvaluator.evaluate(
            "s.__getitem__(2)", runtime_context={"s": s}
        ) == EvaluatedValue(
            value=TypedValue(
                expression="s.__getitem__(2)",
                value_ref="s.__getitem__(2)",
                value_str="'l'",
                value_type="str",
                expandable=True,
            ),
            attributes=[
                TypedValue(
                    expression="__getitem__(0)",
                    value_ref="0",
                    value_str="'l'",
                    value_type="str",
                    expandable=True,
                )
            ],
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
            value=TypedValue(
                expression="a",
                value_ref="a",
                value_str=(
                    "<core.util.expression_evaluator.test_expression_evaluator."
                    "TestExpressionEvaluator.test_evaluate_object_expression.<locals>.A"
                    f" object at {hex(id(a))}>"
                ),
                value_type="A",
                expandable=True,
            ),
            attributes=[
                TypedValue(
                    expression="i",
                    value_ref="i",
                    value_str="10",
                    value_type="int",
                    expandable=False,
                ),
                TypedValue(
                    expression="j",
                    value_ref="j",
                    value_str="1.1",
                    value_type="float",
                    expandable=False,
                ),
            ],
        )

    def test_evaluate_container_expressions(self):
        i = 10
        f = 1.1

        a_list = [i, f, (i, f)]
        assert ExpressionEvaluator.evaluate(
            "a_list", runtime_context={"a_list": a_list}
        ) == EvaluatedValue(
            value=TypedValue(
                expression="a_list",
                value_ref="a_list",
                value_str="[10, 1.1, (10, 1.1)]",
                value_type="list",
                expandable=True,
            ),
            attributes=[
                TypedValue(
                    expression="__getitem__(0)",
                    value_ref="0",
                    value_str="10",
                    value_type="int",
                    expandable=False,
                ),
                TypedValue(
                    expression="__getitem__(1)",
                    value_ref="1",
                    value_str="1.1",
                    value_type="float",
                    expandable=False,
                ),
                TypedValue(
                    expression="__getitem__(2)",
                    value_ref="2",
                    value_str="(10, 1.1)",
                    value_type="tuple",
                    expandable=True,
                ),
            ],
        )
        t = (i, f, {i, f})
        assert ExpressionEvaluator.evaluate(
            "t", runtime_context={"t": t}
        ) == EvaluatedValue(
            value=TypedValue(
                expression="t",
                value_ref="t",
                value_str="(10, 1.1, {1.1, 10})",
                value_type="tuple",
                expandable=True,
            ),
            attributes=[
                TypedValue(
                    expression="__getitem__(0)",
                    value_ref="0",
                    value_str="10",
                    value_type="int",
                    expandable=False,
                ),
                TypedValue(
                    expression="__getitem__(1)",
                    value_ref="1",
                    value_str="1.1",
                    value_type="float",
                    expandable=False,
                ),
                TypedValue(
                    expression="__getitem__(2)",
                    value_ref="2",
                    value_str="{1.1, 10}",
                    value_type="set",
                    expandable=True,
                ),
            ],
        )
        s = {i, f, (i, f)}
        assert ExpressionEvaluator.evaluate(
            "s", runtime_context={"s": s}
        ) == EvaluatedValue(
            value=TypedValue(
                expression="s",
                value_ref="s",
                value_str="{1.1, 10, (10, 1.1)}",
                value_type="set",
                expandable=True,
            ),
            attributes=[
                TypedValue(
                    expression="__getitem__(0)",
                    value_ref="0",
                    value_str="1.1",
                    value_type="float",
                    expandable=False,
                ),
                TypedValue(
                    expression="__getitem__(1)",
                    value_ref="1",
                    value_str="10",
                    value_type="int",
                    expandable=False,
                ),
                TypedValue(
                    expression="__getitem__(2)",
                    value_ref="2",
                    value_str="(10, 1.1)",
                    value_type="tuple",
                    expandable=False,
                ),
            ],
        )

        d = {1: "a", "b": [{i, f}], (i,): f}
        assert ExpressionEvaluator.evaluate(
            "d", runtime_context={"d": d}
        ) == EvaluatedValue(
            value=TypedValue(
                expression="d",
                value_ref="d",
                value_str="{1: 'a', 'b': [{1.1, 10}], (10,): 1.1}",
                value_type="dict",
                expandable=True,
            ),
            attributes=[
                TypedValue(
                    expression="__getitem__(1)",
                    value_ref="1",
                    value_str="'a'",
                    value_type="str",
                    expandable=True,
                ),
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
            value=TypedValue(
                expression="g",
                value_ref="g",
                value_str=(
                    "<generator object"
                    " TestExpressionEvaluator.test_evaluate_container_expressions."
                    f"<locals>.<genexpr> at {hex(id(g))}>"
                ),
                value_type="generator",
                expandable=True,
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
            value=TypedValue(
                expression="g",
                value_ref="g",
                value_str=(
                    "<generator object"
                    " TestExpressionEvaluator.test_evaluate_container_expressions."
                    f"<locals>.gen at {hex(id(g))}>"
                ),
                value_type="generator",
                expandable=True,
            ),
            attributes=[
                TypedValue(
                    expression="i",
                    value_ref="i",
                    value_str="0",
                    value_type="int",
                    expandable=False,
                )
            ],
        )

        it = iter([1, 2, 3])
        assert ExpressionEvaluator.evaluate(
            "it", runtime_context={"it": it}
        ) == EvaluatedValue(
            value=TypedValue(
                expression="it",
                value_ref="it",
                value_str=f"<list_iterator object at {hex(id(it))}>",
                value_type="list_iterator",
                expandable=False,
            ),
            attributes=[],
        )

        it = iter([1, 2, 3])
        next(it)
        assert ExpressionEvaluator.evaluate(
            "it", runtime_context={"it": it}
        ) == EvaluatedValue(
            value=TypedValue(
                expression="it",
                value_ref="it",
                value_str=f"<list_iterator object at {hex(id(it))}>",
                value_type="list_iterator",
                expandable=False,
            ),
            attributes=[],
        )

    def test_evaluate_in_another_context(self):
        i = 10
        j = 20
        assert ExpressionEvaluator.evaluate(
            "j", runtime_context={"j": i, "i": j}
        ) == EvaluatedValue(
            value=TypedValue(
                expression="j",
                value_ref="j",
                value_str="10",
                value_type="int",
                expandable=False,
            ),
            attributes=[],
        )

        assert ExpressionEvaluator.evaluate(
            "i", runtime_context={"j": i, "i": j}
        ) == EvaluatedValue(
            value=TypedValue(
                expression="i",
                value_ref="i",
                value_str="20",
                value_type="int",
                expandable=False,
            ),
            attributes=[],
        )
