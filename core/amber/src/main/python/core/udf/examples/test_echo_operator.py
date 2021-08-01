import pandas
import pytest

from core.udf.examples.echo_operator import EchoOperator
from proto.edu.uci.ics.amber.engine.common import LayerIdentity, LinkIdentity


class TestEchoOperator:

    @pytest.fixture
    def echo_operator(self):
        return EchoOperator()

    def test_echo_operator(self, echo_operator):
        echo_operator.open()
        tuple_ = pandas.Series({"test-1": "hello", "test-2": 10})
        link = LinkIdentity(from_=LayerIdentity("from", "from", "from"), to=LayerIdentity("to", "to", "to"))
        outputs = echo_operator.process_texera_tuple(tuple_, link)
        output_tuple = next(outputs)

        assert (output_tuple == tuple_).all()
        with pytest.raises(StopIteration):
            next(outputs)
