import pytest

from pytexera import Tuple
from .echo_operator import EchoOperator


class TestEchoOperator:
    @pytest.fixture
    def echo_operator(self):
        return EchoOperator()

    def test_echo_operator(self, echo_operator):
        echo_operator.open()
        tuple_ = Tuple({"test-1": "hello", "test-2": 10})

        outputs = echo_operator.process_tuple(tuple_, 0)
        output_tuple = next(outputs)

        assert output_tuple == tuple_
        with pytest.raises(StopIteration):
            next(outputs)
