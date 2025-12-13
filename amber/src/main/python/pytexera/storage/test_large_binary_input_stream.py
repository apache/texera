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
from io import BytesIO
from core.models.type.large_binary import largebinary
from pytexera.storage.large_binary_input_stream import LargeBinaryInputStream
from pytexera.storage import large_binary_manager


class TestLargeBinaryInputStream:
    @pytest.fixture
    def large_binary(self):
        """Create a test largebinary."""
        return largebinary("s3://test-bucket/path/to/object")

    @pytest.fixture
    def mock_s3_response(self):
        """Create a mock S3 response with a BytesIO body."""
        return {"Body": BytesIO(b"test data content")}

    def test_init_with_valid_large_binary(self, large_binary):
        """Test initialization with a valid largebinary."""
        stream = LargeBinaryInputStream(large_binary)
        try:
            assert stream._large_binary == large_binary
            assert stream._underlying is None
            assert not stream._closed
        finally:
            stream.close()

    def test_init_with_none_raises_error(self):
        """Test that initializing with None raises ValueError."""
        with pytest.raises(ValueError, match="largebinary cannot be None"):
            LargeBinaryInputStream(None)

    def test_lazy_init_downloads_from_s3(self, large_binary, mock_s3_response):
        """Test that _lazy_init downloads from S3 on first read."""
        with patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client:
            mock_s3_client = MagicMock()
            mock_s3_client.get_object.return_value = mock_s3_response
            mock_get_s3_client.return_value = mock_s3_client

            stream = LargeBinaryInputStream(large_binary)
            try:
                assert stream._underlying is None  # Not initialized yet

                # Trigger lazy init by reading
                data = stream.read()
                assert data == b"test data content"
                assert stream._underlying is not None

                # Verify S3 was called correctly
                mock_s3_client.get_object.assert_called_once_with(
                    Bucket="test-bucket", Key="path/to/object"
                )
            finally:
                stream.close()

    def test_read_all(self, large_binary, mock_s3_response):
        """Test reading all data."""
        with patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client:
            mock_s3_client = MagicMock()
            mock_s3_client.get_object.return_value = mock_s3_response
            mock_get_s3_client.return_value = mock_s3_client

            stream = LargeBinaryInputStream(large_binary)
            try:
                data = stream.read()
                assert data == b"test data content"
            finally:
                stream.close()

    def test_read_partial(self, large_binary, mock_s3_response):
        """Test reading partial data."""
        with patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client:
            mock_s3_client = MagicMock()
            mock_s3_client.get_object.return_value = mock_s3_response
            mock_get_s3_client.return_value = mock_s3_client

            stream = LargeBinaryInputStream(large_binary)
            try:
                data = stream.read(4)
                assert data == b"test"
            finally:
                stream.close()

    def test_readline(self, large_binary):
        """Test reading a line."""
        with patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client:
            response = {"Body": BytesIO(b"line1\nline2\nline3")}
            mock_s3_client = MagicMock()
            mock_s3_client.get_object.return_value = response
            mock_get_s3_client.return_value = mock_s3_client

            stream = LargeBinaryInputStream(large_binary)
            try:
                line = stream.readline()
                assert line == b"line1\n"
            finally:
                stream.close()

    def test_readlines(self, large_binary):
        """Test reading all lines."""
        with patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client:
            response = {"Body": BytesIO(b"line1\nline2\nline3")}
            mock_s3_client = MagicMock()
            mock_s3_client.get_object.return_value = response
            mock_get_s3_client.return_value = mock_s3_client

            stream = LargeBinaryInputStream(large_binary)
            try:
                lines = stream.readlines()
                assert lines == [b"line1\n", b"line2\n", b"line3"]
            finally:
                stream.close()

    def test_readable(self, large_binary):
        """Test readable() method."""
        stream = LargeBinaryInputStream(large_binary)
        try:
            assert stream.readable() is True

            stream.close()
            assert stream.readable() is False
        finally:
            if not stream._closed:
                stream.close()

    def test_seekable(self, large_binary):
        """Test seekable() method (should always return False)."""
        stream = LargeBinaryInputStream(large_binary)
        try:
            assert stream.seekable() is False
        finally:
            stream.close()

    def test_closed_property(self, large_binary):
        """Test closed property."""
        stream = LargeBinaryInputStream(large_binary)
        try:
            assert stream.closed is False

            stream.close()
            assert stream.closed is True
        finally:
            if not stream._closed:
                stream.close()

    def test_close(self, large_binary, mock_s3_response):
        """Test closing the stream."""
        with patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client:
            mock_s3_client = MagicMock()
            mock_s3_client.get_object.return_value = mock_s3_response
            mock_get_s3_client.return_value = mock_s3_client

            stream = LargeBinaryInputStream(large_binary)
            stream.read(1)  # Trigger lazy init
            assert stream._underlying is not None

            stream.close()
            assert stream._closed is True
            assert stream._underlying.closed

    def test_context_manager(self, large_binary, mock_s3_response):
        """Test using as context manager."""
        with patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client:
            mock_s3_client = MagicMock()
            mock_s3_client.get_object.return_value = mock_s3_response
            mock_get_s3_client.return_value = mock_s3_client

            with LargeBinaryInputStream(large_binary) as stream:
                data = stream.read()
                assert data == b"test data content"
                assert not stream._closed

            # Stream should be closed after context exit
            assert stream._closed

    def test_iteration(self, large_binary):
        """Test iteration over lines."""
        with patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client:
            response = {"Body": BytesIO(b"line1\nline2\nline3")}
            mock_s3_client = MagicMock()
            mock_s3_client.get_object.return_value = response
            mock_get_s3_client.return_value = mock_s3_client

            stream = LargeBinaryInputStream(large_binary)
            try:
                lines = list(stream)
                assert lines == [b"line1\n", b"line2\n", b"line3"]
            finally:
                stream.close()

    def test_read_after_close_raises_error(self, large_binary, mock_s3_response):
        """Test that reading after close raises ValueError."""
        with patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client:
            mock_s3_client = MagicMock()
            mock_s3_client.get_object.return_value = mock_s3_response
            mock_get_s3_client.return_value = mock_s3_client

            stream = LargeBinaryInputStream(large_binary)
            stream.close()

            with pytest.raises(ValueError, match="I/O operation on closed stream"):
                stream.read()
            # Stream is already closed, no need to close again
