import pytest

from pytexera import Tuple
from .echo_operator import EchoOperator
from .generator_operator import GeneratorOperator


class TestEchoOperator:
    @pytest.fixture
    def generator_operator(self):
        return GeneratorOperator()

    def test_generator_operator(self, generator_operator):
        generator_operator.open()
        assert generator_operator.produce() == Tuple({"test": [1, 2, 3]})
        generator_operator.close()
