import contextlib
import sys

from loguru import logger

from core.python_worker import PythonWorker
from core.util.print_writer.stream_to_logger_adaptor import StreamToLoggerAdaptor


def init_loguru_logger(stream_log_level) -> None:
    """
    initialize the loguru's logger with the given configurations
    :param stream_log_level: level to be output to stdout/stderr
    :return:
    """

    # loguru has default configuration which includes stderr as the handler. In order to
    # change the configuration, the easiest way is to remove any existing handlers and
    # re-configure them.
    logger.remove()

    # set up stream handler, which outputs to stderr
    logger.add(sys.stderr, level=stream_log_level)

    # initialize a custom logger level for print to use
    logger.level("PRINT", no=38)


if __name__ == "__main__":
    init_loguru_logger(sys.argv[3])
    # redirect user's print into logger
    with contextlib.redirect_stdout(StreamToLoggerAdaptor()):
        PythonWorker(
            host="localhost", input_port=int(sys.argv[1]), output_port=int(sys.argv[2])
        ).run()
