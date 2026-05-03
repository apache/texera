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

import sys
import pytest
from unittest.mock import MagicMock

from core.architecture.managers.executor_manager import ExecutorManager


# Sample operator code for testing
SAMPLE_OPERATOR_CODE = """
from pytexera import *

class TestOperator(UDFOperatorV2):
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield tuple_
"""

SAMPLE_SOURCE_OPERATOR_CODE = """
from pytexera import *

class TestSourceOperator(UDFSourceOperator):
    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
        yield Tuple({"test": "data"})
"""


class TestExecutorManager:
    """Test suite for ExecutorManager, focusing on R UDF plugin support."""

    @pytest.fixture
    def executor_manager(self):
        """Create a fresh ExecutorManager instance for each test."""
        manager = ExecutorManager()
        yield manager
        # Cleanup: close the temp filesystem
        if hasattr(manager, "_fs"):
            manager.close()

    def _mock_r_plugin(self, executor_class_name, is_source):
        """
        Helper to mock the texera_r plugin module.

        :param executor_class_name: Name of the executor class (e.g., 'RTupleExecutor')
        :param is_source: Whether the executor is a source operator
        :return: Tuple of (mock_texera_r, mock_executor_instance)
        """
        from core.models import SourceOperator, Operator

        mock_texera_r = MagicMock()
        mock_executor_class = MagicMock()
        setattr(mock_texera_r, executor_class_name, mock_executor_class)

        # Use appropriate spec based on operator type
        spec_class = SourceOperator if is_source else Operator
        mock_executor_instance = MagicMock(spec=spec_class)
        mock_executor_instance.is_source = is_source
        mock_executor_class.return_value = mock_executor_instance

        sys.modules["texera_r"] = mock_texera_r
        return mock_texera_r, mock_executor_instance

    def _cleanup_r_plugin(self):
        """Remove the mocked texera_r module from sys.modules."""
        if "texera_r" in sys.modules:
            del sys.modules["texera_r"]

    def test_initialization(self, executor_manager):
        """Test that ExecutorManager initializes correctly."""
        assert executor_manager.executor is None
        assert executor_manager.operator_module_name is None

    def test_reject_r_tuple_language(self, executor_manager):
        """Test that 'r-tuple' language is rejected with ImportError when plugin is not available."""
        with pytest.raises(ImportError) as exc_info:
            executor_manager.initialize_executor(
                code=SAMPLE_OPERATOR_CODE, is_source=False, language="r-tuple"
            )

        # Verify the error message mentions R operators require the texera-rudf package
        assert "texera-rudf" in str(exc_info.value) or "R operators require" in str(
            exc_info.value
        )

    def test_reject_r_table_language(self, executor_manager):
        """Test that 'r-table' language is rejected with ImportError when plugin is not available."""
        with pytest.raises(ImportError) as exc_info:
            executor_manager.initialize_executor(
                code=SAMPLE_OPERATOR_CODE, is_source=False, language="r-table"
            )

        # Verify the error message mentions R operators require the texera-rudf package
        assert "texera-rudf" in str(exc_info.value) or "R operators require" in str(
            exc_info.value
        )

    def test_accept_r_tuple_language_with_plugin(self, executor_manager):
        """Test that 'r-tuple' language is accepted when plugin is available."""
        _, mock_executor = self._mock_r_plugin("RTupleExecutor", is_source=False)
        try:
            executor_manager.initialize_executor(
                code="# R code", is_source=False, language="r-tuple"
            )
            assert executor_manager.executor == mock_executor
        finally:
            self._cleanup_r_plugin()

    def test_accept_r_table_language_with_plugin(self, executor_manager):
        """Test that 'r-table' language is accepted when plugin is available."""
        _, mock_executor = self._mock_r_plugin("RTableExecutor", is_source=False)
        try:
            executor_manager.initialize_executor(
                code="# R code", is_source=False, language="r-table"
            )
            assert executor_manager.executor == mock_executor
        finally:
            self._cleanup_r_plugin()

    def test_accept_r_tuple_source_with_plugin(self, executor_manager):
        """Test that 'r-tuple' source operators work when plugin is available."""
        _, mock_executor = self._mock_r_plugin("RTupleSourceExecutor", is_source=True)
        try:
            executor_manager.initialize_executor(
                code="# R code", is_source=True, language="r-tuple"
            )
            assert executor_manager.executor == mock_executor
        finally:
            self._cleanup_r_plugin()

    def test_accept_r_table_source_with_plugin(self, executor_manager):
        """Test that 'r-table' source operators work when plugin is available."""
        _, mock_executor = self._mock_r_plugin("RTableSourceExecutor", is_source=True)
        try:
            executor_manager.initialize_executor(
                code="# R code", is_source=True, language="r-table"
            )
            assert executor_manager.executor == mock_executor
        finally:
            self._cleanup_r_plugin()

    def test_accept_python_language_regular_operator(self, executor_manager):
        """Test that 'python' language is accepted for regular operators."""
        # This should not raise any assertion error
        executor_manager.initialize_executor(
            code=SAMPLE_OPERATOR_CODE, is_source=False, language="python"
        )

        # Verify executor was initialized
        assert executor_manager.executor is not None
        # Module name comes from a process-wide counter, so it has the
        # right shape but its exact value depends on what other tests
        # have run in the same pytest session.
        assert executor_manager.operator_module_name is not None
        assert executor_manager.operator_module_name.startswith("udf-v")
        assert executor_manager.executor.is_source is False

    def test_accept_python_language_source_operator(self, executor_manager):
        """Test that 'python' language is accepted for source operators."""
        # This should not raise any assertion error
        executor_manager.initialize_executor(
            code=SAMPLE_SOURCE_OPERATOR_CODE, is_source=True, language="python"
        )

        # Verify executor was initialized
        assert executor_manager.executor is not None
        assert executor_manager.operator_module_name is not None
        assert executor_manager.operator_module_name.startswith("udf-v")
        assert executor_manager.executor.is_source is True

    def test_reject_other_unsupported_languages(self, executor_manager):
        """Test that other arbitrary languages still work (no R-specific check)."""
        # Languages other than r-tuple and r-table should be allowed to pass
        # the assertion, though they may fail at code execution
        try:
            executor_manager.initialize_executor(
                code=SAMPLE_OPERATOR_CODE,
                is_source=False,
                language="javascript",  # arbitrary language
            )
            # If we get here, the assertion passed (which is correct behavior)
            # But the code execution might fail, which is fine
        except AssertionError:
            # Should NOT raise AssertionError for non-R languages
            pytest.fail("Should not raise AssertionError for non-R languages")
        except Exception:
            # Other exceptions (like import errors) are expected and acceptable
            pass

    def test_gen_module_file_name_increments(self, executor_manager):
        """Test that module file names increment monotonically.

        The counter is process-wide so the absolute starting value
        depends on prior tests in the same pytest session; only the
        relative ordering matters for correctness.
        """
        module1, file1 = executor_manager.gen_module_file_name()
        module2, file2 = executor_manager.gen_module_file_name()
        module3, file3 = executor_manager.gen_module_file_name()

        def version(module_name: str) -> int:
            return int(module_name.removeprefix("udf-v"))

        v1 = version(module1)
        assert version(module2) == v1 + 1
        assert version(module3) == v1 + 2

        assert file1 == f"{module1}.py"
        assert file2 == f"{module2}.py"
        assert file3 == f"{module3}.py"

    def test_is_concrete_operator_static_method(self):
        """Test the is_concrete_operator static method."""
        from core.models import TupleOperatorV2

        # Should return True for concrete operator classes
        # Note: We can't easily test with actual concrete classes here without imports
        # This test just verifies the method exists and is callable
        assert hasattr(ExecutorManager, "is_concrete_operator")
        assert callable(ExecutorManager.is_concrete_operator)

        # Test with non-class
        assert ExecutorManager.is_concrete_operator("not a class") is False
        assert ExecutorManager.is_concrete_operator(123) is False

        # Test with abstract base classes (TupleOperatorV2 has abstract methods)
        assert ExecutorManager.is_concrete_operator(TupleOperatorV2) is False

    def test_regular_operator_is_not_source(self, executor_manager):
        """Test that regular operator with is_source=False works correctly."""
        executor_manager.initialize_executor(
            code=SAMPLE_OPERATOR_CODE, is_source=False, language="python"
        )
        assert executor_manager.executor.is_source is False

    def test_source_operator_mismatch_raises_error(self, executor_manager):
        """Test that mismatched source operator flag raises AssertionError."""
        with pytest.raises(AssertionError) as exc_info:
            executor_manager.initialize_executor(
                code=SAMPLE_OPERATOR_CODE,
                is_source=True,  # Wrong: regular operator but marked as source
                language="python",
            )
        assert "SourceOperator API" in str(exc_info.value)


REPLACEMENT_OPERATOR_CODE = """
from pytexera import *

class ReplacementOperator(UDFOperatorV2):
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield tuple_
"""

NO_OPERATOR_CODE = """
def helper():
    return 42
"""

TWO_OPERATORS_CODE = """
from pytexera import *

class FirstOperator(UDFOperatorV2):
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield tuple_

class SecondOperator(UDFOperatorV2):
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield tuple_
"""


class TestUpdateExecutor:
    """Test suite for ExecutorManager.update_executor.

    Notes on test isolation: the existing TestExecutorManager fixture cannot
    fully clean up the udf-vN modules it imports (its `hasattr(manager, "_fs")`
    cleanup guard is buggy — the actual cached_property key is `fs`), so a
    given udf-v1 module may already live in sys.modules with a path attached
    to a previous test's tmp filesystem. These tests therefore avoid asserting
    on attributes baked into a specific operator class and instead use
    setattr/getattr-only semantics that hold regardless of which cached
    module satisfies the import.
    """

    @pytest.fixture
    def initialized_manager(self):
        manager = ExecutorManager()
        manager.initialize_executor(
            code=SAMPLE_OPERATOR_CODE, is_source=False, language="python"
        )
        # Stamp custom attributes on the live instance so the dict-preservation
        # check works even if the underlying class came from a cached module.
        manager.executor.runtime_field = "set-after-init"
        manager.executor.counter = 6
        yield manager
        manager.close()

    def test_update_preserves_pre_update_dict_state(self, initialized_manager):
        before = initialized_manager.executor
        before_dict = dict(before.__dict__)

        initialized_manager.update_executor(
            code=REPLACEMENT_OPERATOR_CODE, is_source=False
        )

        # update_executor reuses the prior __dict__ on a freshly instantiated
        # operator — verify both halves: a NEW instance, but the OLD state.
        assert initialized_manager.executor is not before
        assert initialized_manager.executor.runtime_field == "set-after-init"
        assert initialized_manager.executor.counter == 6
        # Assert key presence explicitly so a missing key with an expected
        # value of None doesn't slip past via dict.get()'s default.
        after_dict = initialized_manager.executor.__dict__
        for key, value in before_dict.items():
            assert key in after_dict, f"key {key!r} missing after update"
            assert after_dict[key] == value

    def test_update_advances_module_name_monotonically(self, initialized_manager):
        # The module-name counter is process-wide, so absolute values
        # depend on prior tests in the same pytest session; only the
        # relative bump matters.
        before = initialized_manager.operator_module_name
        assert before is not None and before.startswith("udf-v")

        initialized_manager.update_executor(
            code=REPLACEMENT_OPERATOR_CODE, is_source=False
        )

        after = initialized_manager.operator_module_name
        assert after is not None and after.startswith("udf-v")
        assert int(after.removeprefix("udf-v")) == int(before.removeprefix("udf-v")) + 1

    def test_update_with_source_mismatch_raises_assertion(self, initialized_manager):
        # The replacement code is a regular operator, but is_source=True asks
        # the manager to treat it as a source operator. Same guardrail as
        # initialize_executor.
        with pytest.raises(AssertionError) as exc_info:
            initialized_manager.update_executor(
                code=REPLACEMENT_OPERATOR_CODE, is_source=True
            )
        assert "SourceOperator API" in str(exc_info.value)

    def test_update_with_no_operator_class_raises_assertion(self, initialized_manager):
        # load_executor_definition asserts exactly one Operator subclass exists
        # in the module — an empty module trips that assertion.
        with pytest.raises(AssertionError) as exc_info:
            initialized_manager.update_executor(code=NO_OPERATOR_CODE, is_source=False)
        assert "one and only one Operator" in str(exc_info.value)

    def test_update_with_multiple_operator_classes_raises_assertion(
        self, initialized_manager
    ):
        with pytest.raises(AssertionError) as exc_info:
            initialized_manager.update_executor(
                code=TWO_OPERATORS_CODE, is_source=False
            )
        assert "one and only one Operator" in str(exc_info.value)

    def test_repeated_updates_keep_carrying_the_running_state(
        self, initialized_manager
    ):
        # Update once, mutate the new instance, then update again — the second
        # update must see the *latest* state, not the snapshot from before
        # the first update.
        initialized_manager.update_executor(
            code=REPLACEMENT_OPERATOR_CODE, is_source=False
        )
        initialized_manager.executor.counter = 42
        initialized_manager.executor.added_after_update = True

        initialized_manager.update_executor(
            code=REPLACEMENT_OPERATOR_CODE, is_source=False
        )

        assert initialized_manager.executor.counter == 42
        assert initialized_manager.executor.added_after_update is True
