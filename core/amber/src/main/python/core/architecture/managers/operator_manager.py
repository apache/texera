import importlib
import inspect
import sys
from cached_property import cached_property

import fs
from pathlib import Path
from typing import Tuple, Optional, Mapping

from fs.base import FS
from loguru import logger
from core.models import Operator, SourceOperator


class OperatorManager:
    def __init__(self):
        self.operator: Optional[Operator] = None
        self.operator_module_name: Optional[str] = None
        self.operator_version: int = 0  # incremental only

    @cached_property
    def fs(self) -> FS:
        """
        Creates a tmp fs for storing source code, which will be removed when the
        workflow is completed.
        :return:
        """
        # TODO:
        #       For various reasons when the workflow is not completed successfully,
        #  the tmp fs could not be closed properly. This means it may leave files
        #  in the /var/tmp folder after a partially started or failed execution.
        #       A full-life-cycle management of tmp fs is required to consider all
        #  possible errors happened during execution. However, the full-life-cycle
        #  management could be hard due to errors from JAVA side which causes force
        #  kill on the Python process.
        #       As each python file is usually tiny in size, and the OS can
        #  periodically clean up /var/tmp anyway, the full-life-cycle management is
        #  not a priority to be fixed.
        temp_fs = fs.open_fs("temp://")
        root = Path(temp_fs.getsyspath("/"))
        logger.debug(f"Opening a tmp directory at {root}.")
        sys.path.append(str(root))
        return temp_fs

    def gen_module_file_name(self) -> Tuple[str, str]:
        """
        Generate a UUID to be used as udf source code file.
        :return Tuple[str, str]: the pair of module_name and file_name.
        """
        self.operator_version += 1
        module_name = f"udf-v{self.operator_version}"
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
        logger.debug(
            f"A tmp py file is written to "
            f"{Path(self.fs.getsyspath('/')).joinpath(file_name)}."
        )

        if module_name in sys.modules:
            operator_module = importlib.import_module(module_name)
            operator_module.__dict__.clear()
            operator_module.__dict__["__name__"] = module_name
            operator_module = importlib.reload(operator_module)
        else:
            operator_module = importlib.import_module(module_name)
        self.operator_module_name = module_name

        operators = list(
            filter(self.is_concrete_operator, operator_module.__dict__.values())
        )
        assert len(operators) == 1, "There should be one and only one Operator defined"
        return operators[0]

    def close(self) -> None:
        """
        Close the tmp fs and release all resources created within it.
        :return:
        """
        self.fs.close()
        logger.debug(f"Tmp directory {self.fs.getsyspath('/')} is closed and cleared.")

    @staticmethod
    def is_concrete_operator(cls: type) -> bool:
        """
        Check if the class is a non-abstract Operator.
        :param cls: a target class to be evaluated
        :return: bool
        """

        return (
            inspect.isclass(cls)
            and issubclass(cls, Operator)
            and not inspect.isabstract(cls)
        )

    def initialize_operator(
        self, code: str, is_source: bool, output_schema: Mapping[str, str]
    ) -> None:
        """
        Initialize the operator logic with the given code. The output schema is
        decided by the user.

        :param code: The string version of python code, containing one Operator
            class declaration.
        :param is_source: Indicating if the operator is used as a source operator.
        :param output_schema: the raw mapping of output schema, name -> type_str.
        :return:
        """
        operator: type(Operator) = self.load_operator(code)
        self.operator = operator()
        self.operator.is_source = is_source
        self.operator.output_schema = output_schema
        assert (
            isinstance(self.operator, SourceOperator) == self.operator.is_source
        ), "Please use SourceOperator API for source operators."

    def update_operator(self, code: str, is_source: bool) -> None:
        """
        Update the operator logic, preserving its state in the __dict__.
        The user is responsible to make sure the state can be used by the new logic.

        :param code: The string version of python code, containing one Operator
            class declaration.
        :param is_source: Indicating if the operator is used as a source operator.
        :return:
        """
        original_internal_state = self.operator.__dict__
        operator: type(Operator) = self.load_operator(code)
        self.operator = operator()
        self.operator.is_source = is_source
        assert (
            isinstance(self.operator, SourceOperator) == self.operator.is_source
        ), "Please use SourceOperator API for source operators."
        # overwrite the internal state
        self.operator.__dict__ = original_internal_state
        # TODO:
        #   it may be an interesting idea to preserve versions of code and versions
        #   of states whenever the operator logic is being updated.
