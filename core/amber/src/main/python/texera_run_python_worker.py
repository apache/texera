import contextlib
import sys
from typing import IO, Optional, TypeVar

from loguru import logger

from core.python_worker import PythonWorker

_T_io = TypeVar("_T_io", bound=Optional[IO[str]])


class StreamToLogger(_T_io):
    """
    This class is used to redirect `print` to loguru's logger, instead of stdout.
    """

    def __init__(self, level=logger.level("PRINT", no=38)):
        self._level = level

    def write(self, buffer):
        for line in buffer.rstrip().splitlines():
            logger.opt(depth=1).log("PRINT", line.rstrip())

    def flush(self):
        pass


if __name__ == '__main__':
    # redirect user's print into logger
    with contextlib.redirect_stdout(StreamToLogger()):
        PythonWorker(host="localhost", input_port=int(sys.argv[1]), output_port=int(sys.argv[2])).run()
