import pytest

from pytexera import Tuple
from .generator_operator import GeneratorOperator


class TestEchoOperator:
    @pytest.fixture
    def generator_operator(self):
        return GeneratorOperator()

    def test_generator_operator(self, generator_operator):
        generator_operator.open()
        outputs = generator_operator.produce()
        output_tuple = Tuple(next(outputs))
        assert output_tuple == Tuple({"test": [1, 2, 3]})
        generator_operator.close()
