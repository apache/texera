import logging

import pandas

from operators.texera_udf_operator_base import TexeraUDFOperator, log_exception

logger = logging.getLogger(__name__)


class TexeraBlockingUnsupervisedTrainerOperator(TexeraUDFOperator):

    @log_exception
    def __init__(self):
        super().__init__()
        self._data = []
        self._train_args = dict()

    @log_exception
    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        self._data.append(row[0])

    @log_exception
    def close(self) -> None:
        pass

    @staticmethod
    @log_exception
    def train(data, *args, **kwargs):
        raise NotImplementedError

    @log_exception
    def report(self, model) -> None:
        pass

    @log_exception
    def input_exhausted(self, *args):
        model = self.train(self._data, **self._train_args)
        self.report(model)
