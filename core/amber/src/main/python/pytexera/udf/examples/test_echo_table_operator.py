from collections import deque

import pytest

from core.models.table import all_output_to_tuple
from pytexera import Tuple
from .echo_table_operator import EchoTableOperator


class TestEchoTableOperator:
    @pytest.fixture
    def echo_table_operator(self):
        return EchoTableOperator()

    def test_echo_table_operator(self, echo_table_operator):
        echo_table_operator.open()
        tuple_ = Tuple({"test-1": "hello", "test-2": 10})
        print(tuple_)
        deque(echo_table_operator.process_tuple(tuple_, 0))
        outputs = echo_table_operator.on_finish(0)
        output_tuple = next(all_output_to_tuple(next(outputs)))
        assert output_tuple == tuple_
        with pytest.raises(StopIteration):
            next(outputs)
        echo_table_operator.close()
