from operators.texera_udf_operator_base import TexeraUDFOperator, log_exception


class DemoOperator(TexeraUDFOperator):
    @log_exception
    def __init__(self):
        super().__init__()
        self._result_tuples = []

    @log_exception
    def accept(self, row, nth_child=0):
        self._result_tuples.append(row)  # must take args
        self._result_tuples.append(row)

    @log_exception
    def has_next(self):
        return len(self._result_tuples) != 0

    @log_exception
    def next(self):
        return self._result_tuples.pop()

    @log_exception
    def close(self):
        pass


operator_instance = DemoOperator()
