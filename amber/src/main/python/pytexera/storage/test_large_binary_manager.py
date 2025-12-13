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
from unittest.mock import patch, MagicMock
from pytexera.storage import large_binary_manager
from core.storage.storage_config import StorageConfig


class TestLargeBinaryManager:
    @pytest.fixture(autouse=True)
    def setup_storage_config(self):
        """Initialize StorageConfig for tests."""
        if not StorageConfig._initialized:
            StorageConfig.initialize(
                postgres_uri_without_scheme="localhost:5432/test",
                postgres_username="test",
                postgres_password="test",
                table_result_namespace="test",
                directory_path="/tmp/test",
                commit_batch_size=1000,
                s3_endpoint="http://localhost:9000",
                s3_region="us-east-1",
                s3_auth_username="minioadmin",
                s3_auth_password="minioadmin",
            )

    def test_get_s3_client_initializes_once(self):
        """Test that S3 client is initialized and cached."""
        # Reset the client
        large_binary_manager._s3_client = None

        with patch("boto3.client") as mock_boto3_client:
            mock_client = MagicMock()
            mock_boto3_client.return_value = mock_client

            # First call should create client
            client1 = large_binary_manager._get_s3_client()
            assert client1 == mock_client
            assert mock_boto3_client.call_count == 1

            # Second call should return cached client
            client2 = large_binary_manager._get_s3_client()
            assert client2 == mock_client
            assert mock_boto3_client.call_count == 1  # Still 1, not 2

    def test_get_s3_client_without_boto3_raises_error(self):
        """Test that missing boto3 raises RuntimeError."""
        large_binary_manager._s3_client = None

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

    def test_ensure_bucket_exists_when_bucket_exists(self):
        """Test that existing bucket doesn't trigger creation."""
        large_binary_manager._s3_client = None

        with patch("boto3.client") as mock_boto3_client:
            mock_client = MagicMock()
            mock_boto3_client.return_value = mock_client
            # head_bucket doesn't raise exception (bucket exists)
            mock_client.head_bucket.return_value = None
            mock_client.exceptions.NoSuchBucket = type("NoSuchBucket", (Exception,), {})

            large_binary_manager._ensure_bucket_exists("test-bucket")
            mock_client.head_bucket.assert_called_once_with(Bucket="test-bucket")
            mock_client.create_bucket.assert_not_called()

    def test_ensure_bucket_exists_creates_bucket_when_missing(self):
        """Test that missing bucket triggers creation."""
        large_binary_manager._s3_client = None

        with patch("boto3.client") as mock_boto3_client:
            mock_client = MagicMock()
            mock_boto3_client.return_value = mock_client
            # head_bucket raises NoSuchBucket exception
            no_such_bucket = type("NoSuchBucket", (Exception,), {})
            mock_client.exceptions.NoSuchBucket = no_such_bucket
            mock_client.head_bucket.side_effect = no_such_bucket()

            large_binary_manager._ensure_bucket_exists("test-bucket")
            mock_client.head_bucket.assert_called_once_with(Bucket="test-bucket")
            mock_client.create_bucket.assert_called_once_with(Bucket="test-bucket")

    def test_create_generates_unique_uri(self):
        """Test that create() generates a unique S3 URI."""
        large_binary_manager._s3_client = None

        with patch("boto3.client") as mock_boto3_client:
            mock_client = MagicMock()
            mock_boto3_client.return_value = mock_client
            mock_client.head_bucket.return_value = None
            mock_client.exceptions.NoSuchBucket = type("NoSuchBucket", (Exception,), {})

            uri = large_binary_manager.create()

            # Check URI format
            assert uri.startswith("s3://")
            assert uri.startswith(f"s3://{large_binary_manager.DEFAULT_BUCKET}/")
            assert "objects/" in uri

            # Verify bucket was checked/created
            mock_client.head_bucket.assert_called_once_with(
                Bucket=large_binary_manager.DEFAULT_BUCKET
            )

    def test_create_uses_default_bucket(self):
        """Test that create() uses the default bucket."""
        large_binary_manager._s3_client = None

        with patch("boto3.client") as mock_boto3_client:
            mock_client = MagicMock()
            mock_boto3_client.return_value = mock_client
            mock_client.head_bucket.return_value = None
            mock_client.exceptions.NoSuchBucket = type("NoSuchBucket", (Exception,), {})

            uri = large_binary_manager.create()
            assert large_binary_manager.DEFAULT_BUCKET in uri
