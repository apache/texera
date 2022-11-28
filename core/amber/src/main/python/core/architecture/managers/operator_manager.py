import inspect
import sys
import uuid

from typing import Tuple, Optional, Mapping

import fs

from core.models import Operator


def gen_uuid(prefix=""):
    return f"{prefix}{'-' if prefix else ''}{uuid.uuid4().hex}"


class OperatorManager:
    def __init__(self):
        self.fs = fs.open_fs("temp://")
        self.root = self.fs.getsyspath("/")
        sys.path.append(self.root)
        self.operator: Optional[Operator] = None

    @staticmethod
    def gen_module_file_name() -> Tuple[str, str]:
        module_name = gen_uuid("udf")
        file_name = f"{module_name}.py"
        return module_name, file_name

    def load_operator(self, code: str) -> type(Operator):
        """
        Load the given operator code in string into a class definition
        :param code: str, python code that defines an Operator, should contain one
                and only one Operator definition.
        :return: an Operator sub-class definition
        """
        module_name, file_name = self.gen_module_file_name()

        with self.fs.open(file_name, "w") as file:
            file.write(code)
        import importlib

        operator_module = importlib.import_module(module_name)

        operators = list(
            filter(self.is_concrete_operator, operator_module.__dict__.values()))
        assert len(operators) == 1, "There should be one and only one Operator defined"
        return operators[0]

    def close(self):
        self.fs.close()


    def is_concrete_operator(self, cls: type) -> bool:
        """
        checks if the class is a non-abstract Operator
        :param cls: a target class to be evaluated
        :return: bool
        """

        return (inspect.isclass(cls) and issubclass(cls,
                                                    Operator) and not inspect.isabstract(
            cls))

    def initialize_operator(self, code: str, is_source: bool,
                            output_schema: Mapping[str, str]) -> None:
        operator: type(Operator) = self.load_operator(code)
        self.operator = operator()
        self.operator.is_source = is_source
        self.operator.output_schema = output_schema

    def update_operator(self, code, is_source: bool)-> None:
        original_internal_state = self.operator.__dict__
        operator: type(Operator) = self.load_operator(code)
        self.operator = operator()
        self.operator.is_source = is_source
        # overwrite the internal state
        self.operator.value.__dict__ = original_internal_state
