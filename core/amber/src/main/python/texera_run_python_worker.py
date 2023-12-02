import sys

from loguru import logger

from core.python_worker import PythonWorker


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


if __name__ == "__main__":
    _, worker_id, output_port, logger_level = sys.argv
    init_loguru_logger(logger_level)

    PythonWorker(
        worker_id=worker_id, host="localhost", output_port=int(output_port)
    ).run()
