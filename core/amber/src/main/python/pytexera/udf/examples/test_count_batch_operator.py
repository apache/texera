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
        for i in range(20):
            deque(count_batch_operator.process_tuple(Tuple({"test-1": "hello", "test-2": 10}), 0))
        count_batch_operator.on_finish(0)
        batch_counter = count_batch_operator.BATCH_COUNT
        assert batch_counter == 2
        count_batch_operator.close()
