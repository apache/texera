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
from core.models.type.large_binary import largebinary


class TestLargeBinary:
    def test_create_with_uri(self):
        """Test creating largebinary with a valid S3 URI."""
        uri = "s3://test-bucket/path/to/object"
        large_binary = largebinary(uri)
        assert large_binary.uri == uri
        assert str(large_binary) == uri
        assert repr(large_binary) == f"largebinary('{uri}')"

    def test_create_without_uri(self):
        """Test creating largebinary without URI (calls large_binary_manager.create)."""
        with patch("pytexera.storage.large_binary_manager.create") as mock_create:
            mock_create.return_value = "s3://bucket/objects/123/uuid"
            large_binary = largebinary()
            assert large_binary.uri == "s3://bucket/objects/123/uuid"
            mock_create.assert_called_once()

    def test_invalid_uri_raises_value_error(self):
        """Test that invalid URI (not starting with s3://) raises ValueError."""
        with pytest.raises(ValueError, match="largebinary URI must start with 's3://'"):
            largebinary("http://invalid-uri")

        with pytest.raises(ValueError, match="largebinary URI must start with 's3://'"):
            largebinary("invalid-uri")

    def test_get_bucket_name(self):
        """Test extracting bucket name from URI."""
        large_binary = largebinary("s3://my-bucket/path/to/object")
        assert large_binary.get_bucket_name() == "my-bucket"

    def test_get_object_key(self):
        """Test extracting object key from URI."""
        large_binary = largebinary("s3://my-bucket/path/to/object")
        assert large_binary.get_object_key() == "path/to/object"

    def test_get_object_key_with_leading_slash(self):
        """Test extracting object key when URI has leading slash."""
        large_binary = largebinary("s3://my-bucket/path/to/object")
        # urlparse includes leading slash, but get_object_key removes it
        assert large_binary.get_object_key() == "path/to/object"

    def test_equality(self):
        """Test largebinary equality comparison."""
        uri = "s3://bucket/path"
        obj1 = largebinary(uri)
        obj2 = largebinary(uri)
        obj3 = largebinary("s3://bucket/different")

        assert obj1 == obj2
        assert obj1 != obj3
        assert obj1 != "not a largebinary"

    def test_hash(self):
        """Test largebinary hashing."""
        uri = "s3://bucket/path"
        obj1 = largebinary(uri)
        obj2 = largebinary(uri)

        assert hash(obj1) == hash(obj2)
        assert hash(obj1) == hash(uri)

    def test_uri_property(self):
        """Test URI property access."""
        uri = "s3://test-bucket/test/path"
        large_binary = largebinary(uri)
        assert large_binary.uri == uri
