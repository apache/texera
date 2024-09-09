import importlib
import inspect
import sys
from cached_property import cached_property

from bdb import Breakpoint

import fs
from pathlib import Path
from typing import Tuple, Optional, Mapping

from fs.base import FS
from loguru import logger
from core.models import Operator, SourceOperator


class OperatorManager:
    def __init__(self):
        self._operator: Optional[Operator] = None
        self._operator_with_bp: Optional[Operator] = None
        self.operator_module_name: Optional[str] = None
        self.operator_version: int = 0  # incremental only
        self._static = False
        self.operator_source_code = ""
        self.scheduled_updates = dict()

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
        logger.debug("loading code:\n" + code)
        self.operator_source_code = code
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
        self._operator = operator()
        self._operator.is_source = is_source
        self._operator.output_schema = output_schema
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
        self._operator = operator()
        self._operator.is_source = is_source
        assert (
            isinstance(self.operator, SourceOperator) == self.operator.is_source
        ), "Please use SourceOperator API for source operators."
        # overwrite the internal state
        self._operator.__dict__ = original_internal_state
        # TODO:
        #   it may be an interesting idea to preserve versions of code and versions
        #   of states whenever the operator logic is being updated.

    def add_breakpoint(self, bp: Breakpoint) -> str:
        """
        Add a static breakpoint() line into the source code.
        :param bp: the breakpoint to be converted into a static code line.
        :return : the updated source code string
        """

        lineno = bp.line
        condition = bp.cond

        code_lines = self.operator_source_code.splitlines()
        target_line = code_lines[lineno - 1]
        code_before = code_lines[: lineno - 1]
        code_after = code_lines[lineno:]

        indentation = " " * (len(target_line) - len(target_line.lstrip()))
        bp_line = f"{indentation}{f'if {condition}:' if condition else ''}breakpoint()"
        new_code = "\n".join(code_before + [bp_line, target_line] + code_after)

        logger.debug("code with breakpoint:\n" + new_code)

        return new_code

    def add_as(self, lineno, state):
        lineno = lineno

        old_code = self.operator_source_code.splitlines()
        target_line = old_code[lineno - 1]
        code_before = old_code[: lineno - 1]
        code_after = old_code[lineno:]

        indentation = " " * (len(target_line) - len(target_line.lstrip()))
        bp_line = f"{indentation}tuple_['{state}'] = {state}"

        new_code = "\n".join(code_before + [bp_line, target_line] + code_after)
        return new_code

    def add_rs(self, lineno, req_lineno, req_state, target_worker_id):
        lineno = lineno

        old_code = self.operator_source_code.splitlines()
        target_line = old_code[lineno - 1]
        code_before = old_code[: lineno - 1]
        code_after = old_code[lineno:]

        indentation = " " * (len(target_line) - len(target_line.lstrip()))
        bp_line = (
            f"{indentation}yield 'request({target_worker_id}, {req_lineno},"
            f" {req_state})'"
        )

        new_code = "\n".join(code_before + [bp_line, target_line] + code_after)
        logger.info("new code \n:" + new_code)
        # print(new_code, file=sys.stdout)
        return new_code

    def add_ss(self, lineno, state):
        old_code = self.operator_source_code.splitlines()
        target_line = old_code[lineno - 1]
        code_before = old_code[: lineno - 1]
        code_after = old_code[lineno:]

        indentation = " " * (len(target_line) - len(target_line.lstrip()))
        bp_line = f"{indentation}yield 'store({lineno}, {state})'"

        new_code = "\n".join(code_before + [bp_line, target_line] + code_after)

        return new_code

    def schedule_update_code(self, when: str, change: str):
        if change[:2] == "ss":  # store state
            ss, lineno, state = change.split()
            self.scheduled_updates[when] = (
                self.add_ss(int(lineno), state),
                self.operator.is_source,
            )

        if change[:2] == "rs":  # request state
            ss, lineno, req_lineno, req_state, target_worker_id = change.split()
            self.scheduled_updates[when] = (
                self.add_rs(int(lineno), int(req_lineno), req_state, target_worker_id),
                self.operator.is_source,
            )

        if change[:2] == "as":  # append state
            ss, lineno, state = change.split()
            self.scheduled_updates[when] = (
                self.add_as(int(lineno), state),
                self.operator.is_source,
            )
        logger.info("updated code:\n" + self.scheduled_updates[when][0])

    def apply_available_code_update(self):
        if self.scheduled_updates:
            self.update_operator(*self.scheduled_updates["tuple"])
            del self.scheduled_updates["tuple"]

    def add_operator_with_bp(self, code, is_source):
        original_internal_state = self._operator.__dict__
        operator: type(Operator) = self.load_operator(code)
        self._operator_with_bp = operator()
        self._operator_with_bp.is_source = is_source
        # overwrite the internal state
        self._operator_with_bp.__dict__ = original_internal_state

    @property
    def operator(self):
        if self._static:
            return self._operator_with_bp
        else:
            return self._operator
