import importlib.util
import inspect

from core.models import Operator

operators_module_spec = importlib.util.spec_from_loader("operators", loader=None)
operators_module = importlib.util.module_from_spec(operators_module_spec)


def load_operator(code: str) -> type(Operator):
    """
    Load the given operator code in string into a class definition
    :param code: str, python code that defines an Operator, should contain one
            and only one Operator definition.
    :return: an Operator sub-class definition
    """
    exec(code, operators_module.__dict__)
    operators = list(filter(is_concrete_operator, operators_module.__dict__.values()))
    assert len(operators) == 1, "There should be one and only one Operator defined"
    return operators[0]


def is_concrete_operator(cls: type) -> bool:
    """
    checks if the class is a non-abstract Operator
    :param cls: a target class to be evaluated
    :return: bool
    """
    return (
        inspect.isclass(cls)
        and issubclass(cls, Operator)
        and not inspect.isabstract(cls)
    )
