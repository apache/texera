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

import base64
import json
import sys
from loguru import logger

try:
    from core.python_worker import PythonWorker
    from core.storage.storage_config import StorageConfig
except ModuleNotFoundError as e:
    if e.name == "proto" or (e.name or "").startswith("proto."):
        sys.exit(
            "Python proto bindings are missing (amber/src/main/python/proto/). "
            "They are generated, not checked in. Generate them by running "
            "`bash bin/python-proto-gen.sh` from the repo root (requires protoc and "
            '`pip install "betterproto[compiler]"`), or build the engine with sbt, '
            "which regenerates them on compile."
        )
    raise


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


# Keys the JVM side (PythonWorkflowWorker) sends in the startup-config JSON.
# Declared here so any drift between the two sides fails loudly instead of being
# silently misassigned, as could happen with the previous positional unpacking.
EXPECTED_CONFIG_KEYS = frozenset(
    {
        "workerId",
        "outputPort",
        "loggerLevel",
        "rPath",
        "icebergCatalogType",
        "icebergPostgresCatalogUriWithoutScheme",
        "icebergPostgresCatalogUsername",
        "icebergPostgresCatalogPassword",
        "icebergRestCatalogUri",
        "icebergRestCatalogWarehouseName",
        "icebergTableNamespace",
        "icebergTableStateNamespace",
        "icebergFileStorageDirectoryPath",
        "icebergTableCommitBatchSize",
        "s3Endpoint",
        "s3Region",
        "s3AuthUsername",
        "s3AuthPassword",
        "s3LargeBinariesBaseUri",
    }
)


def parse_startup_config(raw_config: str) -> dict:
    """Parse and validate the startup configuration.

    The configuration is passed by name (see PythonWorkflowWorker on the JVM
    side) as a Base64-encoded JSON object. Base64 is used so the argument carries
    no quotes or spaces and survives command-line argv quoting on every platform
    (a raw JSON string loses its quotes on Windows). The two sides must agree on
    an exact key set; key order is irrelevant since it is a JSON object. Any drift
    fails loudly:
      - a missing or unexpected key raises ValueError;
      - a non-string value raises TypeError.
    """
    config = json.loads(base64.b64decode(raw_config).decode("utf-8"))
    if not isinstance(config, dict):
        raise TypeError(
            f"startup config must be a JSON object, got {type(config).__name__}"
        )

    actual_keys = set(config)
    missing = EXPECTED_CONFIG_KEYS - actual_keys
    unexpected = actual_keys - EXPECTED_CONFIG_KEYS
    if missing or unexpected:
        raise ValueError(
            f"startup config key mismatch: missing={sorted(missing)}, "
            f"unexpected={sorted(unexpected)}"
        )

    non_string_keys = sorted(k for k, v in config.items() if not isinstance(v, str))
    if non_string_keys:
        raise TypeError(
            f"startup config values must be strings; non-string keys: {non_string_keys}"
        )

    return config


def main(raw_config: str) -> None:
    """Start a Python worker from its validated Base64-encoded JSON startup config."""
    config = parse_startup_config(raw_config)

    init_loguru_logger(config["loggerLevel"])
    StorageConfig.initialize(
        config["icebergCatalogType"],
        config["icebergPostgresCatalogUriWithoutScheme"],
        config["icebergPostgresCatalogUsername"],
        config["icebergPostgresCatalogPassword"],
        config["icebergRestCatalogUri"],
        config["icebergRestCatalogWarehouseName"],
        config["icebergTableNamespace"],
        config["icebergTableStateNamespace"],
        config["icebergFileStorageDirectoryPath"],
        config["icebergTableCommitBatchSize"],
        config["s3Endpoint"],
        config["s3Region"],
        config["s3AuthUsername"],
        config["s3AuthPassword"],
        config["s3LargeBinariesBaseUri"],
    )

    # Setting R_HOME environment variable for R-UDF usage
    r_path = config["rPath"]
    if r_path:
        import os

        os.environ["R_HOME"] = r_path

    PythonWorker(
        worker_id=config["workerId"],
        host="localhost",
        output_port=int(config["outputPort"]),
    ).run()


if __name__ == "__main__":
    main(sys.argv[1])
