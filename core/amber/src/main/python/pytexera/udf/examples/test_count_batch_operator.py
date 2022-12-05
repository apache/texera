from collections import deque

import pandas
import pytest

from pytexera import Batch, Tuple
from .count_batch_operator import CountBatchOperator


class TestEchoTableOperator:
    @pytest.fixture
    def count_batch_operator(self):
        return CountBatchOperator()

    def test_count_batch_operator(self, count_batch_operator):
        count_batch_operator.open()
        for i in range(27):
            deque(
                count_batch_operator.process_tuple(
                    Tuple({"test-1": "hello", "test-2": 10}), 0
                )
            )
        deque(count_batch_operator.on_finish(0))
        batch_counter = count_batch_operator.count
        assert batch_counter == 3
        count_batch_operator.close()

    def test_count_batch_operator_simple(self, count_batch_operator):
        count_batch_operator.open()
        for i in range(20):
            deque(
                count_batch_operator.process_tuple(
                    Tuple({"test-1": "hello", "test-2": 10}), 0
                )
            )
        deque(count_batch_operator.on_finish(0))
        batch_counter = count_batch_operator.count
        assert batch_counter == 2
        count_batch_operator.close()

    def test_count_batch_operator_medium(self, count_batch_operator):
        count_batch_operator.open()
        for i in range(27):
            deque(
                count_batch_operator.process_tuple(
                    Tuple({"test-1": "hello", "test-2": 10}), 0
                )
            )
        deque(count_batch_operator.on_finish(0))
        batch_counter = count_batch_operator.count
        assert batch_counter == 3
        count_batch_operator.close()

    def test_count_batch_operator_hard(self, count_batch_operator):
        count_batch_operator.open()
        count_batch_operator.BATCH_SIZE = 10
        for i in range(27):
            deque(
                count_batch_operator.process_tuple(
                    Tuple({"test-1": "hello", "test-2": 10}), 0
                )
            )
        count_batch_operator.BATCH_SIZE = 5
        for i in range(27):
            deque(
                count_batch_operator.process_tuple(
                    Tuple({"test-1": "hello", "test-2": 10}), 0
                )
            )
        deque(count_batch_operator.on_finish(0))
        batch_counter = count_batch_operator.count
        assert batch_counter == 9
        count_batch_operator.close()

    def test_edge_case_string(self):
        with pytest.raises(ValueError) as exc_info:
            CountBatchOperator("test")
            assert (
                exc_info.value.args[0]
                == "BATCH_SIZE cannot be " + str(type("test")) + "."
            )

    def test_edge_case_non_positive(self, count_batch_operator):
        with pytest.raises(ValueError) as exc_info:
            CountBatchOperator(-20)
            assert exc_info.value.args[0] == "BATCH_SIZE should be positive."

    def test_edge_case_none(self, count_batch_operator):
        with pytest.raises(ValueError) as exc_info:
            CountBatchOperator(None)
            assert exc_info.value.args[0] == "BATCH_SIZE cannot be None."
