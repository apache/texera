import contextlib
import random
import sys
import time
from threading import Thread
from typing import Iterator, Union

import pandas
from loguru import logger
from overrides import overrides

from core.models.tuple import InputExhausted, Tuple
from core.python_worker import PythonWorker
from core.udf import UDFOperator
from proto.edu.uci.ics.amber.engine.common import LinkIdentity

new_level = logger.level("PRINT", no=38)


class StreamToLogger(object):
    """
    This class is used to redirect `print` to loguru's logger, instead of stdout.
    """

    def __init__(self, level=new_level):
        self._level = level

    def write(self, buffer):
        for line in buffer.rstrip().splitlines():
            logger.opt(depth=1).log("PRINT", line.rstrip())

    def flush(self):
        pass


class EchoOperator(UDFOperator):
    @overrides
    def process_texera_tuple(self, tuple_: Union[Tuple, InputExhausted], link: LinkIdentity) -> Iterator[Tuple]:
        if isinstance(tuple_, Tuple):
            # time.sleep(0.1)
            yield tuple_


class TrainOperator(UDFOperator):
    def __init__(self):
        super(TrainOperator, self).__init__()
        self.records = list()

    def train(self) -> "model":
        return {"predict": True, "f1-score": random.random()}

    @overrides
    def process_texera_tuple(self, tuple_: Union[Tuple, InputExhausted], link: LinkIdentity) -> Iterator[Tuple]:
        if isinstance(tuple_, Tuple):
            self.records.append(tuple_)
            yield
            time.sleep(0.01)
            yield
        elif isinstance(tuple_, InputExhausted):
            model = self.train()
            yield pandas.concat([self.records[-1], pandas.Series(model)])


if __name__ == '__main__':
    # redirect user's print into logger
    with contextlib.redirect_stdout(StreamToLogger()):
        python_worker = PythonWorker(host="localhost", input_port=int(sys.argv[1]), output_port=int(sys.argv[2]),
                                     udf_operator=EchoOperator())
        python_worker_thread = Thread(target=python_worker.run)
        python_worker_thread.start()
        python_worker_thread.join()
