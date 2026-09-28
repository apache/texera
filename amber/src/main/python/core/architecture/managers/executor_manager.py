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

import importlib
import inspect
import itertools
import sys
import tempfile
import traceback
from cached_property import cached_property
from loguru import logger
from pathlib import Path
from typing import Tuple, Optional

from core.models import Operator, SourceOperator


def pytexera_names() -> set:
    """Names that `from pytexera import *` brings into a UDF."""
    import pytexera

    return set(pytexera.__all__)


class ExecutorManager:
    # Process-wide monotonically increasing counter used to generate the
    # tmp module names ExecutorManager hands to importlib. Making this a
    # class-level counter (rather than a per-instance counter that always
    # restarts at 1) guarantees that no two ExecutorManager instances in
    # the same Python process can collide on `udf-v1`. Without that
    # guarantee, the second instance hits the "module already loaded"
    # branch of importlib and the post-clear+reload path can return a
    # stale class on Python 3.11 (see #4705).
    #
    # Single-process counters are atomic in CPython under the GIL; we
    # don't expect cross-thread contention on this anyway.
    _module_name_counter = itertools.count(1)

    def __init__(self):
        self.executor: Optional[Operator] = None
        self.operator_module_name: Optional[str] = None
        # Owns the tmp directory's lifetime; see `tmp_dir`.
        self._tmp_dir_handle: Optional[tempfile.TemporaryDirectory] = None

    @cached_property
    def tmp_dir(self) -> Path:
        """
        Creates a tmp directory for storing source code, which will be removed
        when the workflow is completed.
        :return:
        """
        # TODO:
        #       For various reasons when the workflow is not completed successfully,
        #  the tmp directory could not be removed properly. This means it may leave
        #  files in the /var/tmp folder after a partially started or failed
        #  execution.
        #       A full-life-cycle management of the tmp directory is required to
        #  consider all possible errors happened during execution. However, the
        #  full-life-cycle management could be hard due to errors from JAVA side
        #  which causes force kill on the Python process.
        #       As each python file is usually tiny in size, and the OS can
        #  periodically clean up /var/tmp anyway, the full-life-cycle management is
        #  not a priority to be fixed.
        #       `TemporaryDirectory` is held on the instance rather than using a
        #  bare `mkdtemp` so that a manager abandoned without `close()` still has
        #  its directory reclaimed when it is garbage-collected. `fs` gave us that
        #  for free (`FS.__del__` -> `TempFS.close()` -> `clean()`); dropping it
        #  would turn every abandoned manager into a permanent leak. Force kill
        #  runs no finalizers either way, which is the case the TODO above is about.
        #  `ignore_cleanup_errors=True` matches TempFS's `ignore_clean_errors=True`.
        self._tmp_dir_handle = tempfile.TemporaryDirectory(
            prefix="texera-udf-", ignore_cleanup_errors=True
        )
        root = Path(self._tmp_dir_handle.name)
        logger.debug(f"Opening a tmp directory at {root}.")
        sys.path.append(str(root))
        return root

    def gen_module_file_name(self) -> Tuple[str, str]:
        """
        Generate a unique module name and corresponding tmp file name.
        Names come from a process-wide monotonic counter so they never
        collide with any module already in `sys.modules`, even when
        multiple ExecutorManager instances live in the same process.
        :return Tuple[str, str]: the pair of module_name and file_name.
        """
        module_name = f"udf-v{next(ExecutorManager._module_name_counter)}"
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

        file_path = self.tmp_dir.joinpath(file_name)
        # Pin the encoding: importlib always decodes source as UTF-8 (PEP
        # 3120), while the builtin open() writes in the locale encoding, so a
        # UDF containing non-ASCII text would fail to write under a non-UTF-8
        # locale (cp1252, LC_ALL=C). `fs` was passing encoding="utf-8" and
        # newline="" to io.open on our behalf; both are now explicit.
        with open(file_path, "w", encoding="utf-8", newline="\n") as file:
            file.write(code)
        logger.debug(f"A tmp py file is written to {file_path}.")

        # Clear importlib's directory listing cache so freshly written
        # temporary modules are discoverable on systems with coarse mtime.
        importlib.invalidate_caches()
        # gen_module_file_name guarantees module_name is unique across
        # the process, so import_module will always cleanly load source
        # from the tmp directory we just wrote — no re-import / reload dance.
        try:
            executor_module = importlib.import_module(module_name)
        except SyntaxError:
            # SyntaxError already carries the user's line number.
            raise
        except Exception as error:
            raise self._with_user_code_hint(error, str(file_path)) from error
        self.operator_module_name = module_name

        executors = list(
            filter(self.is_concrete_operator, executor_module.__dict__.values())
        )
        assert len(executors) == 1, (
            "There should be one and only one Operator defined. "
            + self._explain_operator_count(executors, executor_module)
        )
        return executors[0]

    # Each pytexera base class and the one method a subclass must implement.
    _BASE_CLASS_METHODS = {
        "UDFOperatorV2": "process_tuple",
        "UDFBatchOperator": "process_batch",
        "UDFTableOperator": "process_table",
        "UDFSourceOperator": "produce",
    }

    @classmethod
    def _base_class_pairing(cls) -> str:
        return ", ".join(
            f"{base} -> {method}" for base, method in cls._BASE_CLASS_METHODS.items()
        )

    @classmethod
    def _explain_operator_count(cls, executors: list, module) -> str:
        """
        Explain why a UDF module does not define exactly one concrete Operator,
        naming the classes involved and how to fix them.
        """
        if len(executors) > 1:
            names = ", ".join(executor.__name__ for executor in executors)
            return (
                f"Found {len(executors)}: {names}. Keep one and delete or comment "
                "out the others. Helper classes must not subclass an Operator."
            )

        abstract_classes = [
            value
            for value in module.__dict__.values()
            if inspect.isclass(value)
            and issubclass(value, Operator)
            and value.__module__ == module.__name__
        ]
        if not abstract_classes:
            return (
                "No Operator class was found. Uncomment or write exactly one class, "
                "e.g. `class ProcessTupleOperator(UDFOperatorV2):`, and keep "
                "`from pytexera import *` as the first line."
            )

        explanations = []
        for abstract_class in abstract_classes:
            base_names = [
                base.__name__
                for base in abstract_class.__mro__
                if base.__name__ in cls._BASE_CLASS_METHODS
            ]
            base_name = base_names[0] if base_names else "Operator"
            missing = ", ".join(sorted(abstract_class.__abstractmethods__))
            explanation = (
                f"{abstract_class.__name__} subclasses {base_name} but does not "
                f"implement {missing}."
            )
            for other_base, method in cls._BASE_CLASS_METHODS.items():
                if other_base != base_name and method in abstract_class.__dict__:
                    explanation += (
                        f" If you meant {method}, subclass {other_base} instead."
                    )
            explanations.append(explanation)
        return (
            " ".join(explanations)
            + f" Each base class needs its own method: {cls._base_class_pairing()}."
        )

    @classmethod
    def _with_user_code_hint(cls, error: Exception, file_path: str) -> Exception:
        """
        Rebuild an error raised while importing user code so its message says
        which line of the UDF failed and, for common mistakes, how to fix it.
        Returns the original error if it cannot be rebuilt with a new message.
        """
        message = str(error)
        user_lines = [
            frame.lineno
            for frame in traceback.extract_tb(error.__traceback__)
            if frame.filename == file_path
        ]
        if user_lines:
            message += f" (line {user_lines[-1]} of the UDF code)"
        if isinstance(error, NameError) and error.name in pytexera_names():
            message += ". Add `from pytexera import *` at the top of the UDF."
        elif "No super class method found" in message:
            message += (
                f". The method must match the base class: {cls._base_class_pairing()}."
            )
        if message == str(error):
            return error
        try:
            return type(error)(message)
        except Exception:
            return error

    def close(self) -> None:
        """
        Remove the tmp directory and release all resources created within it.
        This also evicts the loaded operator module from ``sys.modules``
        and removes the tmp directory from ``sys.path`` so a single call
        fully reverses every global side-effect performed by ``tmp_dir`` and
        ``load_executor_definition``.
        :return:
        """
        if "tmp_dir" not in self.__dict__:
            # the tmp directory was never materialized; nothing to clean up.
            return
        root = self.tmp_dir
        self._tmp_dir_handle.cleanup()
        self._tmp_dir_handle = None
        try:
            sys.path.remove(str(root))
        except ValueError:
            pass
        if self.operator_module_name is not None:
            sys.modules.pop(self.operator_module_name, None)
        logger.debug(f"Tmp directory {root} is closed and cleared.")

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
        :return:
        """
        if language in ("r-tuple", "r-table"):
            # R support is provided by an optional plugin (texera-rudf)
            executor_type = "Tuple" if language == "r-tuple" else "Table"
            try:
                import texera_r

                class_suffix = "SourceExecutor" if is_source else "Executor"
                executor_class = getattr(texera_r, f"R{executor_type}{class_suffix}")
            except ImportError as e:
                raise ImportError(
                    "R operators require the texera-rudf package.\n"
                    "Install with: pip install git+https://github.com/Texera/texera-rudf.git\n"
                    f"Import error: {e}"
                )
            self.executor = executor_class(code)
        else:
            executor: type(Operator) = self.load_executor_definition(code)
            self.executor = executor()
            self.executor.is_source = is_source
        assert isinstance(self.executor, SourceOperator) == self.executor.is_source, (
            "Please use SourceOperator API for source operators."
        )

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
        assert isinstance(self.executor, SourceOperator) == self.executor.is_source, (
            "Please use SourceOperator API for source operators."
        )
        # overwrite the internal state
        self.executor.__dict__ = original_internal_state
        # TODO:
        #   it may be an interesting idea to preserve versions of code and versions
        #   of states whenever the operator logic is being updated.
