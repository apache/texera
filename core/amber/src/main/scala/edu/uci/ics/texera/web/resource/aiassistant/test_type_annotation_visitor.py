import pytest

from type_annotation_visitor import find_untyped_variables


class TestFunctionsAndMethods:

    @pytest.fixture
    def global_functions_code(self):
        """This is the test for global function"""
        return """def global_function(a, b=2, /, c=3, *, d, e=5, **kwargs):
    pass

def global_function_no_return(a, b):
    return a + b

def global_function_with_return(a: int, b: int) -> int:
    return a + b
"""

    def test_global_functions(self, global_functions_code):
        expected_result = [
            ["c", 1, 32, 1, 33],
            ["a", 1, 21, 1, 22],
            ["b", 1, 24, 1, 25],
            ["d", 1, 40, 1, 41],
            ["e", 1, 43, 1, 44],
            ["kwargs", 1, 50, 1, 56],
            ["a", 4, 31, 4, 32],
            ["b", 4, 34, 4, 35]

        ]
        untyped_vars = find_untyped_variables(global_functions_code, 1)
        assert untyped_vars == expected_result

    @pytest.fixture
    def class_methods_code(self):
        """This is the test for class methods and static methods"""
        return """class MyClass:
    def instance_method_no_annotation(self, x, y):
        pass

    @staticmethod
    def static_method(a, b, /, c=3, *, d, **kwargs):
        pass

    @staticmethod
    def static_method_with_annotation(a: int, b: int, /, *, c: int = 5) -> int:
        return a + b + c

    @classmethod
    def class_method(cls, value, /, *, option=True):
        pass

    @classmethod
    def class_method_with_annotation(cls, value: str, /, *, flag: bool = False) -> str:
        return value.upper()
"""

    def test_class_methods(self, class_methods_code):
        expected_result = [
            ["x", 2, 45, 2, 46],
            ["y", 2, 48, 2, 49],
            ["c", 6, 32, 6, 33],
            ["a", 6, 23, 6, 24],
            ["b", 6, 26, 6, 27],
            ["d", 6, 40, 6, 41],
            ["kwargs", 6, 45, 6, 51],
            ["value", 14, 27, 14, 32],
            ["option", 14, 40, 14, 46]
        ]
        untyped_vars = find_untyped_variables(class_methods_code, 1)
        assert untyped_vars == expected_result

    @pytest.fixture
    def lambda_code(self):
        """This is the test for lambda function"""
        return """lambda_function = lambda x, y, /, z=0, *, w=1: x + y + z + w
lambda_function_with_annotation = lambda x: x * 2
"""

    def test_lambda_functions(self, lambda_code):
        with pytest.raises(ValueError) as exc_info:
            find_untyped_variables(lambda_code, 1)
        assert "Lambda functions do not support type annotation" in str(exc_info.value)

    @pytest.fixture
    def comprehensive_functions_code(self):
        """This is the test for comprehensive function"""
        return """def default_args_function(a, b=2, /, c=3, *, d=4):
    pass

def args_kwargs_function(*args, **kwargs):
    pass

def function_with_return_annotation(a: int, b: int, /, *, c: int = 0) -> int:
    return a + b + c

def function_without_return_annotation(a, b):
    return a + b
"""

    def test_comprehensive(self, comprehensive_functions_code):
        expected_result = [
            ["c", 1, 38, 1, 39],
            ["a", 1, 27, 1, 28],
            ["b", 1, 30, 1, 31],
            ["d", 1, 46, 1, 47],
            ["args", 4, 27, 4, 31],
            ["kwargs", 4, 35, 4, 41],
            ["a", 10, 40, 10, 41],
            ["b", 10, 43, 10, 44]
        ]
        untyped_vars = find_untyped_variables(comprehensive_functions_code, 1)
        assert untyped_vars == expected_result

    @pytest.fixture
    def multi_line_function_code(self):
        """This is the test for multi-line function"""
        return """def multi_line_function(
    a,
    b: int = 10,
    /,
    c: str = "hello",
    *,
    d,
    e=20,
    **kwargs
):
    pass
"""

    def test_multi_lines_argument(self, multi_line_function_code):
        expected_result = [
            ["a", 2, 5, 2, 6],
            ["d", 7, 5, 7, 6],
            ["e", 8, 5, 8, 6],
            ["kwargs", 9, 7, 9, 13]
        ]
        untyped_vars = find_untyped_variables(multi_line_function_code, 1)
        assert untyped_vars == expected_result
