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

import re

import pytest
from unittest.mock import patch
from pytexera.storage.large_binary_manager import LargeBinaryManager
from core.storage.storage_config import StorageConfig

# The manager is a singleton; bind the shared instance for the tests.
large_binary_manager = LargeBinaryManager()

# Execution-scoped base URI the coordinator hands down; create() appends a unique suffix.
TEST_BASE_URI = "s3://texera-large-binaries/objects/1/"


@pytest.fixture(autouse=True)
def _init_storage_config():
    """Initialize StorageConfig (incl. the large-binaries base URI) for every test."""
    if not StorageConfig._initialized:
        StorageConfig.initialize(
            catalog_type="postgres",
            postgres_uri_without_scheme="localhost:5432/test",
            postgres_username="test",
            postgres_password="test",
            rest_catalog_uri="http://localhost:8181/catalog/",
            rest_catalog_warehouse_name="texera",
            table_result_namespace="test",
            table_state_namespace="test-state",
            directory_path="/tmp/test",
            commit_batch_size=1000,
            s3_endpoint="http://localhost:9000",
            s3_region="us-east-1",
            s3_auth_username="rustfsadmin",
            s3_auth_password="rustfsadmin",
            s3_large_binaries_base_uri=TEST_BASE_URI,
        )


@pytest.fixture
def empty_s3_cache(monkeypatch):
    monkeypatch.setattr(large_binary_manager, "_s3_client", None)


@pytest.fixture
def s3_factory(empty_s3_cache):
    with patch("boto3.client") as factory:
        factory.return_value.exceptions.NoSuchBucket = type(
            "NoSuchBucket", (Exception,), {}
        )
        yield factory


class TestLargeBinaryManager:
    def test_get_s3_client_initializes_once(self, s3_factory):
        """Test that S3 client is initialized and cached."""
        assert large_binary_manager._get_s3_client() == s3_factory.return_value
        s3_factory.assert_called_once()

        assert large_binary_manager._get_s3_client() == s3_factory.return_value
        s3_factory.assert_called_once()

    def test_get_s3_client_without_boto3_raises_error(self, empty_s3_cache):
        """Test that missing boto3 raises RuntimeError."""

        import sys

        # Temporarily remove boto3 from sys.modules to simulate it not being installed
        boto3_backup = sys.modules.pop("boto3", None)
        try:
            # Mock the import to raise ImportError
            original_import = __import__

            def mock_import(name, *args, **kwargs):
                if name == "boto3":
                    raise ImportError("No module named boto3")
                return original_import(name, *args, **kwargs)

            with patch("builtins.__import__", side_effect=mock_import):
                with pytest.raises(RuntimeError, match="boto3 required"):
                    large_binary_manager._get_s3_client()
        finally:
            # Restore boto3 if it was there
            if boto3_backup is not None:
                sys.modules["boto3"] = boto3_backup

    def test_ensure_bucket_exists_when_bucket_exists(self, s3_factory):
        """Test that existing bucket doesn't trigger creation."""
        client = s3_factory.return_value
        client.head_bucket.return_value = None

        large_binary_manager._ensure_bucket_exists("test-bucket")
        client.head_bucket.assert_called_once_with(Bucket="test-bucket")
        client.create_bucket.assert_not_called()

    def test_ensure_bucket_exists_creates_bucket_when_missing(self, s3_factory):
        """Test that missing bucket triggers creation."""
        client = s3_factory.return_value
        client.head_bucket.side_effect = client.exceptions.NoSuchBucket()

        large_binary_manager._ensure_bucket_exists("test-bucket")
        client.head_bucket.assert_called_once_with(Bucket="test-bucket")
        client.create_bucket.assert_called_once_with(Bucket="test-bucket")

    def test_create_appends_unique_suffix_to_base_uri(self):
        """create() returns the configured base URI plus a unique suffix (no S3 call)."""
        base = StorageConfig.S3_LARGE_BINARIES_BASE_URI

        uri1 = large_binary_manager.create()
        uri2 = large_binary_manager.create()

        assert uri1.startswith(base)
        assert uri2.startswith(base)
        # A non-empty, unique suffix follows the base URI.
        assert uri1 != base
        assert uri1 != uri2


def test_create_matches_execution_scoped_key_shape(monkeypatch):
    # The base URI is execution-scoped (coordinator-named); create() only appends a uuid.
    monkeypatch.setattr(
        StorageConfig,
        "S3_LARGE_BINARIES_BASE_URI",
        "s3://texera-large-binaries/objects/42/",
    )
    uri = large_binary_manager.create()
    assert re.fullmatch(r"s3://texera-large-binaries/objects/42/[0-9a-fA-F-]+", uri)


def test_create_without_base_uri_raises(monkeypatch):
    # An unconfigured base URI should fail with a clear error, not a cryptic S3 one.
    monkeypatch.setattr(StorageConfig, "S3_LARGE_BINARIES_BASE_URI", None)
    with pytest.raises(RuntimeError):
        large_binary_manager.create()


def test_largebinarymanager_is_a_singleton():
    # Constructing the manager always returns the same shared instance.
    assert LargeBinaryManager() is LargeBinaryManager()

    # State (the cached S3 client) is shared across handles (same instance).
    mgr = LargeBinaryManager()
    original = mgr._s3_client
    sentinel = object()
    mgr._s3_client = sentinel
    try:
        assert LargeBinaryManager()._s3_client is sentinel
    finally:
        mgr._s3_client = original
