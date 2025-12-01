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
from unittest.mock import patch
from core.models.schema.big_object import BigObject


class TestBigObject:
    def test_create_with_uri(self):
        """Test creating BigObject with a valid S3 URI."""
        uri = "s3://test-bucket/path/to/object"
        big_object = BigObject(uri)
        assert big_object.uri == uri
        assert str(big_object) == uri
        assert repr(big_object) == f"BigObject('{uri}')"

    def test_create_without_uri(self):
        """Test creating BigObject without URI (should call BigObjectManager.create)."""
        with patch(
            "pytexera.storage.big_object_manager.BigObjectManager"
        ) as mock_manager:
            mock_manager.create.return_value = "s3://bucket/objects/123/uuid"
            big_object = BigObject()
            assert big_object.uri == "s3://bucket/objects/123/uuid"
            mock_manager.create.assert_called_once()

    def test_invalid_uri_raises_value_error(self):
        """Test that invalid URI (not starting with s3://) raises ValueError."""
        with pytest.raises(ValueError, match="BigObject URI must start with 's3://'"):
            BigObject("http://invalid-uri")

        with pytest.raises(ValueError, match="BigObject URI must start with 's3://'"):
            BigObject("invalid-uri")

    def test_get_bucket_name(self):
        """Test extracting bucket name from URI."""
        big_object = BigObject("s3://my-bucket/path/to/object")
        assert big_object.get_bucket_name() == "my-bucket"

    def test_get_object_key(self):
        """Test extracting object key from URI."""
        big_object = BigObject("s3://my-bucket/path/to/object")
        assert big_object.get_object_key() == "path/to/object"

    def test_get_object_key_with_leading_slash(self):
        """Test extracting object key when URI has leading slash."""
        big_object = BigObject("s3://my-bucket/path/to/object")
        # urlparse includes leading slash, but get_object_key removes it
        assert big_object.get_object_key() == "path/to/object"

    def test_equality(self):
        """Test BigObject equality comparison."""
        uri = "s3://bucket/path"
        obj1 = BigObject(uri)
        obj2 = BigObject(uri)
        obj3 = BigObject("s3://bucket/different")

        assert obj1 == obj2
        assert obj1 != obj3
        assert obj1 != "not a BigObject"

    def test_hash(self):
        """Test BigObject hashing."""
        uri = "s3://bucket/path"
        obj1 = BigObject(uri)
        obj2 = BigObject(uri)

        assert hash(obj1) == hash(obj2)
        assert hash(obj1) == hash(uri)

    def test_uri_property(self):
        """Test URI property access."""
        uri = "s3://test-bucket/test/path"
        big_object = BigObject(uri)
        assert big_object.uri == uri
