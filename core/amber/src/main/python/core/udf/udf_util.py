import importlib.util
import inspect

from core.udf import UDFOperator

user_module_spec = importlib.util.spec_from_loader('udf_module', loader=None)
udf_module = importlib.util.module_from_spec(user_module_spec)


def load_udf(udf_code: str) -> type(UDFOperator):
    """
    Load the given udf code in string into a class definition
    :param udf_code: str, python code that defines a UDFOperator, should contain one
            and only one UDFOperator definition.
    :return: a UDFOperator sub-class definition
    """
    exec(udf_code, udf_module.__dict__)
    operators = list(filter(is_concrete_udf, udf_module.__dict__.values()))
    assert len(operators) == 1, "There should be one and only one UDFOperator defined"
    return operators[0]


def is_concrete_udf(cls: type) -> bool:
    """
    checks if the class is a non-abstract UDFOperator
    :param cls: a target class to be evaluated
    :return: bool
    """
    return inspect.isclass(cls) and issubclass(cls, UDFOperator) and not inspect.isabstract(cls)
