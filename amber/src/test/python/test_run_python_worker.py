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

import json
from unittest import mock

import pytest

import texera_run_python_worker as entry


def _full_config() -> dict:
    """A complete startup config matching the keys PythonWorkflowWorker emits."""
    return {
        "workerId": "worker-1",
        "outputPort": "5005",
        "loggerLevel": "INFO",
        "rPath": "",
        "icebergCatalogType": "postgres",
        "icebergPostgresCatalogUriWithoutScheme": "host:5432/db",
        "icebergPostgresCatalogUsername": "pg-user",
        "icebergPostgresCatalogPassword": "pg-pass",
        "icebergRestCatalogUri": "",
        "icebergRestCatalogWarehouseName": "",
        "icebergTableNamespace": "result_ns",
        "icebergTableStateNamespace": "state_ns",
        "icebergFileStorageDirectoryPath": "/tmp/files",
        "icebergTableCommitBatchSize": "100",
        "s3Endpoint": "http://s3:9000",
        "s3Region": "us-west-2",
        "s3AuthUsername": "s3-user",
        "s3AuthPassword": "s3-pass",
        "s3LargeBinariesBaseUri": "s3://bucket/base",
    }


def test_main_maps_named_config_to_storage_and_worker():
    """Each named field reaches the correct StorageConfig.initialize argument and
    worker parameter — guarding against the silent misalignment that positional
    argv passing allowed."""
    config = _full_config()
    with (
        mock.patch.object(entry, "StorageConfig") as storage_config,
        mock.patch.object(entry, "PythonWorker") as python_worker,
        mock.patch.object(entry, "init_loguru_logger"),
    ):
        entry.main(json.dumps(config))

    storage_config.initialize.assert_called_once_with(
        "postgres",
        "host:5432/db",
        "pg-user",
        "pg-pass",
        "",
        "",
        "result_ns",
        "state_ns",
        "/tmp/files",
        "100",
        "http://s3:9000",
        "us-west-2",
        "s3-user",
        "s3-pass",
        "s3://bucket/base",
    )
    python_worker.assert_called_once_with(
        worker_id="worker-1", host="localhost", output_port=5005
    )
    python_worker.return_value.run.assert_called_once()


def test_main_sets_r_home_only_when_r_path_is_present(monkeypatch):
    monkeypatch.delenv("R_HOME", raising=False)
    config = _full_config()
    config["rPath"] = "/opt/R"
    with (
        mock.patch.object(entry, "StorageConfig"),
        mock.patch.object(entry, "PythonWorker"),
        mock.patch.object(entry, "init_loguru_logger"),
    ):
        import os

        entry.main(json.dumps(config))
        assert os.environ["R_HOME"] == "/opt/R"


@pytest.mark.parametrize("missing_key", sorted(_full_config().keys()))
def test_main_raises_keyerror_when_a_field_is_missing(missing_key):
    """A missing/renamed key fails loudly rather than being silently misassigned."""
    config = _full_config()
    del config[missing_key]
    with (
        mock.patch.object(entry, "StorageConfig"),
        mock.patch.object(entry, "PythonWorker"),
        mock.patch.object(entry, "init_loguru_logger"),
    ):
        with pytest.raises(KeyError):
            entry.main(json.dumps(config))
