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

import pytest

from core.storage.storage_config import StorageConfig


_INIT_KWARGS = dict(
    catalog_type="postgres",
    postgres_uri_without_scheme="localhost:5432/texera",
    postgres_username="pg_user",
    postgres_password="pg_pass",
    rest_catalog_uri="http://rest:8181",
    rest_catalog_warehouse_name="warehouse",
    table_result_namespace="result_ns",
    table_state_namespace="state_ns",
    directory_path="/data/iceberg",
    commit_batch_size="4096",
    s3_endpoint="http://minio:9000",
    s3_region="us-west-2",
    s3_auth_username="s3_user",
    s3_auth_password="s3_pass",
    s3_large_binaries_base_uri="s3://bucket/objects/1/",
)


@pytest.fixture
def fresh_config(monkeypatch):
    """Reset the static StorageConfig to a pristine, uninitialized state.

    StorageConfig is a process-wide static holder that refuses re-init once
    initialized, so each test runs against a clean copy of its class fields.
    """
    saved = {
        name: getattr(StorageConfig, name)
        for name in vars(StorageConfig)
        if name.isupper() or name == "_initialized"
    }
    monkeypatch.setattr(StorageConfig, "_initialized", False)
    for name in saved:
        if name.isupper():
            monkeypatch.setattr(StorageConfig, name, None)
    yield StorageConfig


class TestInitialize:
    def test_populates_every_field_from_args(self, fresh_config):
        fresh_config.initialize(**_INIT_KWARGS)

        assert fresh_config.ICEBERG_CATALOG_TYPE == "postgres"
        assert (
            fresh_config.ICEBERG_POSTGRES_CATALOG_URI_WITHOUT_SCHEME
            == "localhost:5432/texera"
        )
        assert fresh_config.ICEBERG_POSTGRES_CATALOG_USERNAME == "pg_user"
        assert fresh_config.ICEBERG_POSTGRES_CATALOG_PASSWORD == "pg_pass"
        assert fresh_config.ICEBERG_REST_CATALOG_URI == "http://rest:8181"
        assert fresh_config.ICEBERG_REST_CATALOG_WAREHOUSE_NAME == "warehouse"
        assert fresh_config.ICEBERG_TABLE_RESULT_NAMESPACE == "result_ns"
        assert fresh_config.ICEBERG_TABLE_STATE_NAMESPACE == "state_ns"
        assert fresh_config.ICEBERG_FILE_STORAGE_DIRECTORY_PATH == "/data/iceberg"
        assert fresh_config.S3_ENDPOINT == "http://minio:9000"
        assert fresh_config.S3_REGION == "us-west-2"
        assert fresh_config.S3_AUTH_USERNAME == "s3_user"
        assert fresh_config.S3_AUTH_PASSWORD == "s3_pass"
        assert fresh_config.S3_LARGE_BINARIES_BASE_URI == "s3://bucket/objects/1/"

    def test_sets_initialized_flag(self, fresh_config):
        assert fresh_config._initialized is False
        fresh_config.initialize(**_INIT_KWARGS)
        assert fresh_config._initialized is True

    def test_coerces_commit_batch_size_to_int(self, fresh_config):
        # commit_batch_size arrives as a string from the Java side and must be
        # stored as an int, not left as its string form.
        fresh_config.initialize(**{**_INIT_KWARGS, "commit_batch_size": "512"})
        assert fresh_config.ICEBERG_TABLE_COMMIT_BATCH_SIZE == 512
        assert isinstance(fresh_config.ICEBERG_TABLE_COMMIT_BATCH_SIZE, int)

    def test_re_initialization_is_rejected(self, fresh_config):
        fresh_config.initialize(**_INIT_KWARGS)
        with pytest.raises(RuntimeError, match="already been initialized"):
            fresh_config.initialize(**_INIT_KWARGS)

    def test_rejected_re_init_does_not_overwrite_existing_fields(self, fresh_config):
        fresh_config.initialize(**_INIT_KWARGS)
        with pytest.raises(RuntimeError):
            fresh_config.initialize(**{**_INIT_KWARGS, "catalog_type": "rest"})
        # The failed second call must not mutate the already-set value.
        assert fresh_config.ICEBERG_CATALOG_TYPE == "postgres"


class TestStaticClass:
    def test_cannot_be_instantiated(self):
        with pytest.raises(TypeError, match="static class"):
            StorageConfig()
