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
from unittest import mock

import pytest

import texera_run_python_worker as entry


def _encode(config) -> str:
    """Encode a config the way PythonWorkflowWorker does: Base64-encoded JSON.

    The JVM side passes the startup config as Base64 so it survives command-line
    argv quoting on every platform (a raw JSON string loses its quotes on Windows).
    """
    return base64.b64encode(json.dumps(config).encode("utf-8")).decode("ascii")


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


def _patched_collaborators():
    """Patch the heavy collaborators so main() exercises only the config wiring."""
    return (
        mock.patch.object(entry, "StorageConfig"),
        mock.patch.object(entry, "PythonWorker"),
        mock.patch.object(entry, "init_loguru_logger"),
    )


def test_full_config_keys_match_the_expected_set():
    # Guards against the sample config in this test drifting from the contract.
    assert set(_full_config()) == set(entry.EXPECTED_CONFIG_KEYS)


def test_main_maps_named_config_to_storage_and_worker():
    """Each named field reaches the correct StorageConfig.initialize argument and
    worker parameter — guarding against the silent misalignment that positional
    argv passing allowed."""
    config = _full_config()
    storage_patch, worker_patch, _logger_patch = _patched_collaborators()
    with storage_patch as storage_config, worker_patch as python_worker, _logger_patch:
        entry.main(_encode(config))

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


def test_main_mapping_is_independent_of_key_order():
    """Reordering the JSON keys must not change where values land (it is a dict)."""
    reordered = dict(reversed(list(_full_config().items())))
    storage_patch, worker_patch, _logger_patch = _patched_collaborators()
    with storage_patch as storage_config, worker_patch as python_worker, _logger_patch:
        entry.main(_encode(reordered))

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


def test_main_sets_r_home_when_r_path_present(monkeypatch):
    monkeypatch.delenv("R_HOME", raising=False)
    config = _full_config()
    config["rPath"] = "/opt/R"
    storage_patch, worker_patch, _logger_patch = _patched_collaborators()
    with storage_patch, worker_patch, _logger_patch:
        import os

        entry.main(_encode(config))
        assert os.environ["R_HOME"] == "/opt/R"


@pytest.mark.parametrize("missing_key", sorted(_full_config().keys()))
def test_parse_rejects_a_missing_key(missing_key):
    """A missing key fails loudly rather than being silently misassigned."""
    config = _full_config()
    del config[missing_key]
    with pytest.raises(ValueError, match="key mismatch"):
        entry.parse_startup_config(_encode(config))


def test_parse_rejects_an_unexpected_key():
    """An extra key (e.g. the JVM side added a field) fails instead of being ignored."""
    config = _full_config()
    config["someNewField"] = "value"
    with pytest.raises(ValueError, match="key mismatch"):
        entry.parse_startup_config(_encode(config))


def test_parse_rejects_a_non_string_value():
    """A wrongly-typed value (e.g. a number instead of a string) fails."""
    config = _full_config()
    config["outputPort"] = 5005  # number instead of the expected string
    with pytest.raises(TypeError, match="must be strings"):
        entry.parse_startup_config(_encode(config))


def test_parse_rejects_a_non_object_payload():
    with pytest.raises(TypeError, match="must be a JSON object"):
        entry.parse_startup_config(_encode(["not", "an", "object"]))


def test_parse_round_trips_a_base64_encoded_config():
    """The config is passed as Base64-encoded JSON; parsing decodes it back."""
    config = _full_config()
    assert entry.parse_startup_config(_encode(config)) == config
