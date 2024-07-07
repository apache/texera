import importlib
import inspect
import sys
from cached_property import cached_property

import fs
from pathlib import Path
from typing import Tuple, Optional

from fs.base import FS
from loguru import logger
from core.models import Operator, SourceOperator


class ExecutorManager:
    def __init__(self):
        self.executor: Optional[Operator] = None
        self.operator_module_name: Optional[str] = None
        self.executor_version: int = 0  # incremental only

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
        self.executor_version += 1
        module_name = f"udf-v{self.executor_version}"
        file_name = f"{module_name}.py"
        return module_name, file_name

    def load_executor_definition(self, code: str) -> type(Operator):
        """
        Load the given executor code in string into a class definition
        :param code: str, python code that defines an Operator, should contain one
                and only one Executor definition.
        :return: an Operator sub-class definition
        """
        module_name, file_name = self.gen_module_file_name()

        with self.fs.open(file_name, "w") as file:
            file.write(code)
        logger.debug(
            "A tmp py file is written to "
            f"{Path(self.fs.getsyspath('/')).joinpath(file_name)}."
        )

        if module_name in sys.modules:
            executor_module = importlib.import_module(module_name)
            executor_module.__dict__.clear()
            executor_module.__dict__["__name__"] = module_name
            executor_module = importlib.reload(executor_module)
        else:
            executor_module = importlib.import_module(module_name)
        self.operator_module_name = module_name

        executors = list(
            filter(self.is_concrete_operator, executor_module.__dict__.values())
        )
        assert len(executors) == 1, "There should be one and only one Operator defined"
        return executors[0]

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

    def initialize_executor(self, code: str, is_source: bool, language: str) -> None:
        """
        Initialize the executor with the given code. The output schema is
        decided by the user.

        :param code: The string version of the code, containing one Operator
            class declaration.
        :param is_source: Indicating if the operator is used as a source operator.
        :param language: The language of the operator code.
        :param output_schema: the raw mapping of output schema, name -> type_str.
        :return:
        """
        if language == "r-tuple":
            # Have to import it here and not at the top in case R_HOME from udf.conf
            # is not defined, otherwise an error will occur
            # If R_HOME is not defined and rpy2 cannot find the
            # R_HOME environment variable, an error will occur here
            from core.models.RTupleExecutor import RTupleSourceExecutor, RTupleExecutor

            self.executor = (
                RTupleSourceExecutor(code) if is_source else RTupleExecutor(code)
            )
        elif language == "r-table":
            # Have to import it here and not at the top in case R_HOME from udf.conf
            # is not defined, otherwise an error will occur
            # If R_HOME is not defined and rpy2 cannot find the
            # R_HOME environment variable, an error will occur here
            from core.models.RTableExecutor import RTableSourceExecutor, RTableExecutor

            self.executor = (
                RTableSourceExecutor(code) if is_source else RTableExecutor(code)
            )
        else:
            executor: type(Operator) = self.load_executor_definition(code)
            self.executor = executor()
            self.executor.is_source = is_source
        assert (
            isinstance(self.executor, SourceOperator) == self.executor.is_source
        ), "Please use SourceOperator API for source operators."

    def update_executor(self, code: str, is_source: bool) -> None:
        """
        Update the executor, preserving its state in the __dict__.
        The user is responsible to make sure the state can be used by the new logic.

        :param code: The string version of python code, containing one Operator
            class declaration.
        :param is_source: Indicating if the operator is used as a source operator.
        :return:
        """
        original_internal_state = self.executor.__dict__
        executor: type(Operator) = self.load_executor_definition(code)
        self.executor = executor()
        self.executor.is_source = is_source
        assert (
            isinstance(self.executor, SourceOperator) == self.executor.is_source
        ), "Please use SourceOperator API for source operators."
        # overwrite the internal state
        self.executor.__dict__ = original_internal_state
        # TODO:
        #   it may be an interesting idea to preserve versions of code and versions
        #   of states whenever the operator logic is being updated.
