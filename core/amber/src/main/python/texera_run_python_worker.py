# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys

from loguru import logger

from core.python_worker import PythonWorker
from core.storage.storage_config import StorageConfig


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
    (
        _,
        worker_id,
        output_port,
        logger_level,
        r_path,
        iceberg_postgres_catalog_uri_without_scheme,
        iceberg_postgres_catalog_username,
        iceberg_postgres_catalog_password,
        iceberg_table_namespace,
        iceberg_file_storage_directory_path,
        iceberg_table_commit_batch_size,
    ) = sys.argv
    init_loguru_logger(logger_level)
    StorageConfig.initialize(
        iceberg_postgres_catalog_uri_without_scheme,
        iceberg_postgres_catalog_username,
        iceberg_postgres_catalog_password,
        iceberg_table_namespace,
        iceberg_file_storage_directory_path,
        iceberg_table_commit_batch_size,
    )

    # Setting R_HOME environment variable for R-UDF usage
    if r_path:
        import os

        os.environ["R_HOME"] = r_path

    PythonWorker(
        worker_id=worker_id, host="localhost", output_port=int(output_port)
    ).run()
