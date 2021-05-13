import importlib.util
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from operators.texera_filter_operator import TexeraFilterOperator
from operators.texera_map_operator import TexeraMapOperator
from server.udf_server import UDFServer


def init_root_logger(log_level: str, log_dir: str, log_fmt: str, datefmt: str) -> None:
    """
    initialize root logger with the given configurations
    :param log_level: lowest log level to be outputted
    :param log_dir: directory path for log files to generate
    """

    logger = logging.getLogger()
    logger.setLevel(log_level)

    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        log_fmt,
        datefmt=datefmt)

    stream_handler.setLevel(log_level)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    file_name = f"texera-python_udf-{datetime.utcnow().isoformat()}-{os.getpid()}.log"
    file_path = Path(log_dir).joinpath(file_name)
    file_handler = logging.FileHandler(file_path)

    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.info(f"Attaching a FileHandler to logger, file path: {file_path}")
    logger.addHandler(file_handler)
    logger.info(f"Logger FileHandler is now attached, previous logs are in StreamHandler only.")


if __name__ == '__main__':

    _, port, log_level, log_dir, log_fmt, datefmt, UDF_operator_script_path, *__ = sys.argv

    # initialize root logger before doing anything
    init_root_logger(log_level, log_dir, log_fmt, datefmt)

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

    UDFServer(final_UDF, "localhost", location).serve()
