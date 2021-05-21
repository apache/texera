from operators.texera_udf_operator_base import TexeraUDFOperator


class DemoOperator(TexeraUDFOperator):

    def __init__(self):
        super().__init__()
        self._result_tuples = []

    def accept(self, row, nth_child=0):
        self._result_tuples.append(row)  # must take args
        self._result_tuples.append(row)

    def has_next(self):
        return len(self._result_tuples) != 0

    def next(self):
        return self._result_tuples.pop()

    def close(self):
        pass


operator_instance = DemoOperator()
