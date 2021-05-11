import logging

from operators.texera_udf_operator_base import TexeraUDFOperator, exception


class DemoOperator(TexeraUDFOperator):
    logger = logging.getLogger("PythonUDF.DemoOperator")

    @exception(logger)
    def __init__(self):
        super().__init__()
        self._result_tuples = []

    @exception(logger)
    def accept(self, row, nth_child=0):
        self._result_tuples.append(row)  # must take args
        self._result_tuples.append(row)

    @exception(logger)
    def has_next(self):
        return len(self._result_tuples) != 0

    @exception(logger)
    def next(self):
        return self._result_tuples.pop()

    @exception(logger)
    def close(self):
        pass


operator_instance = DemoOperator()
