import importlib.util
import inspect

from core.udf import UDFOperator

user_module_spec = importlib.util.spec_from_loader('udf_module', loader=None)
udf_module = importlib.util.module_from_spec(user_module_spec)


def load_udf(udf: str) -> type(UDFOperator):
    exec(udf, udf_module.__dict__)
    operators = list(filter(is_concrete_udf, udf_module.__dict__.values()))
    assert len(operators) == 1, "There should be one and only one UDFOperator defined"
    return operators[0]


def is_concrete_udf(instance) -> bool:
    return inspect.isclass(instance) and issubclass(instance, UDFOperator) and not inspect.isabstract(instance)
