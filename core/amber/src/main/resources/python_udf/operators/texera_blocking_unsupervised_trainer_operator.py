import logging

import pandas

from operators.texera_udf_operator_base import TexeraUDFOperator, exception

logger = logging.getLogger(__name__)


class TexeraBlockingUnsupervisedTrainerOperator(TexeraUDFOperator):

    @exception
    def __init__(self):
        super().__init__()
        self._data = []
        self._train_args = dict()

    @exception
    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        self._data.append(row[0])

    @exception
    def close(self) -> None:
        pass

    @staticmethod
    @exception
    def train(data, *args, **kwargs):
        raise NotImplementedError

    @exception
    def report(self, model) -> None:
        pass

    @exception
    def input_exhausted(self, *args):
        model = self.train(self._data, **self._train_args)
        self.report(model)
