import pandas

from operators.texera_udf_operator_base import TexeraUDFOperator


class TexeraBlockingUnsupervisedTrainerOperator(TexeraUDFOperator):

    def __init__(self):
        super().__init__()
        self._data = []
        self._train_args = dict()

    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        self._data.append(row[0])

    def close(self) -> None:
        pass

    @staticmethod
    def train(data, *args, **kwargs):
        raise NotImplementedError

    def report(self, model) -> None:
        pass

    def input_exhausted(self, *args):
        model = self.train(self._data, **self._train_args)
        self.report(model)
