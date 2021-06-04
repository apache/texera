import contextlib
import importlib.util
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from loguru import logger

from operators.texera_filter_operator import TexeraFilterOperator
from operators.texera_map_operator import TexeraMapOperator
from server.udf_server import UDFServer


class InterceptHandler(logging.Handler):
    """
    This class intercepts the standard logging module's handlers, by forwarding any
    log messages to loguru's singleton logger

    With this InterceptHandler, one can use standard logging module just as normal,
    and the log messages will be outputted through loguru:
    ```
    import logging
    logging.getLogger(name).info("some log")
    ```
    """

    def emit(self, record):
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


class StreamToLogger(object):
    """
    This class is used to redirect `print` to loguru's logger, instead of stdout.
    """

    def __init__(self, level="INFO"):
        self._level = level

    def write(self, buffer):
        for line in buffer.rstrip().splitlines():
            logger.opt(depth=1).log(self._level, "[print] " + line.rstrip())

    def flush(self):
        pass


def init_loguru_logger(stream_log_level: str, stream_log_fmt: str,
                       file_log_dir: str, file_log_level: str, file_log_fmt: str) -> None:
    """
    initialize the loguru's logger with the given configurations
    :param stream_log_level: level to be output to stdout/stderr
    :param stream_log_fmt: log format to stdout/stderr
    :param file_log_dir: log file directory
    :param file_log_level: level to be output to file
    :param file_log_fmt: log format to fil
    :return:
    """

    # loguru has default configuration which includes stderr as the handler. In order to
    # change the configuration, the easiest way is to remove any existing handlers and
    # re-configure them.
    logger.remove()

    # set up stream handler, which outputs to stdout and stderr
    logger.add(sys.stderr, format=stream_log_fmt, level=stream_log_level)

    # set up file handler, which outputs log file
    file_name = f"texera-python_udf-{datetime.utcnow().isoformat()}-{os.getpid()}.log"
    file_path = Path(file_log_dir).joinpath(file_name)
    logger.info(f"Attaching a FileHandler to logger, file path: {file_path}")
    logger.add(file_name, format=file_log_fmt, level=file_log_level)
    logger.info(f"Logger FileHandler is now attached, previous logs are in StreamHandler only.")

    # intercept standard logging module to loguru, so that standard logging module will be
    # handled by the loguru as well.
    logging.basicConfig(handlers=[InterceptHandler()], level=0)


if __name__ == '__main__':

    # TODO: find a better way to pass arguments
    _, port, \
    stream_log_level, stream_log_fmt, \
    file_log_dir, file_log_level, file_log_fmt, \
    UDF_operator_script_path, *__ = sys.argv

    # initialize root logger before doing anything
    init_loguru_logger(stream_log_level, stream_log_fmt,
                       file_log_dir, file_log_level, file_log_fmt)

    # Dynamically import operator from user-defined script.

    # Spec is used to load a spec based on a file location (the UDF script)
    spec = importlib.util.spec_from_file_location('user_module', UDF_operator_script_path)
    # Dynamically load the user script as module
    user_module = importlib.util.module_from_spec(spec)
    # Execute the module so that its attributes can be loaded.
    spec.loader.exec_module(user_module)

    # The UDF that will be used in the server. It will be either an inherited operator instance, or created by passing
    # map_func/filter_func to a TexeraMapOperator/TexeraFilterOperator instance.
    final_UDF = None

    if hasattr(user_module, 'operator_instance'):
        final_UDF = user_module.operator_instance
    elif hasattr(user_module, 'map_function'):
        final_UDF = TexeraMapOperator(user_module.map_function)
    elif hasattr(user_module, 'filter_function'):
        final_UDF = TexeraFilterOperator(user_module.filter_function)
    else:
        raise ValueError("Unsupported UDF definition!")

    location = "grpc+tcp://localhost:" + port

    # redirect user's print into logger
    with contextlib.redirect_stdout(StreamToLogger()):
        UDFServer(final_UDF, "localhost", location).serve()
