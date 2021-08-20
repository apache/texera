import contextlib
import sys

from core.python_worker import PythonWorker
from core.util.print_writer.stream_to_logger_adaptor import StreamToLoggerAdaptor

if __name__ == '__main__':
    # redirect user's print into logger
    with contextlib.redirect_stdout(StreamToLoggerAdaptor()):
        PythonWorker(host="localhost", input_port=int(sys.argv[1]), output_port=int(sys.argv[2])).run()
