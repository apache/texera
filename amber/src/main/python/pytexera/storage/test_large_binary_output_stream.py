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
import time
from unittest.mock import patch, MagicMock
from core.models.type.large_binary import largebinary
from pytexera.storage.large_binary_output_stream import LargeBinaryOutputStream
from pytexera.storage import large_binary_manager


class TestLargeBinaryOutputStream:
    @pytest.fixture
    def large_binary(self):
        """Create a test largebinary."""
        return largebinary("s3://test-bucket/path/to/object")

    def test_init_with_valid_large_binary(self, large_binary):
        """Test initialization with a valid largebinary."""
        stream = LargeBinaryOutputStream(large_binary)
        assert stream._large_binary == large_binary
        assert stream._bucket_name == "test-bucket"
        assert stream._object_key == "path/to/object"
        assert not stream._closed
        assert stream._upload_thread is None

    def test_init_with_none_raises_error(self):
        """Test that initializing with None raises ValueError."""
        with pytest.raises(ValueError, match="largebinary cannot be None"):
            LargeBinaryOutputStream(None)

    def test_write_starts_upload_thread(self, large_binary):
        """Test that write() starts the upload thread."""
        with (
            patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client,
            patch.object(
                large_binary_manager, "_ensure_bucket_exists"
            ) as mock_ensure_bucket,
        ):
            mock_s3 = MagicMock()
            mock_get_s3_client.return_value = mock_s3
            mock_ensure_bucket.return_value = None

            stream = LargeBinaryOutputStream(large_binary)
            assert stream._upload_thread is None

            stream.write(b"test data")
            assert stream._upload_thread is not None
            # Thread may have already completed, so just check it was created
            assert stream._upload_thread is not None

            # Wait for thread to finish
            stream.close()

    def test_write_data(self, large_binary):
        """Test writing data to the stream."""
        with (
            patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client,
            patch.object(
                large_binary_manager, "_ensure_bucket_exists"
            ) as mock_ensure_bucket,
        ):
            mock_s3 = MagicMock()
            mock_get_s3_client.return_value = mock_s3
            mock_ensure_bucket.return_value = None

            stream = LargeBinaryOutputStream(large_binary)
            bytes_written = stream.write(b"test data")
            assert bytes_written == len(b"test data")

            stream.close()

    def test_write_multiple_chunks(self, large_binary):
        """Test writing multiple chunks of data."""
        with (
            patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client,
            patch.object(
                large_binary_manager, "_ensure_bucket_exists"
            ) as mock_ensure_bucket,
        ):
            mock_s3 = MagicMock()
            mock_get_s3_client.return_value = mock_s3
            mock_ensure_bucket.return_value = None

            stream = LargeBinaryOutputStream(large_binary)
            stream.write(b"chunk1")
            stream.write(b"chunk2")
            stream.write(b"chunk3")

            stream.close()

    def test_writable(self, large_binary):
        """Test writable() method."""
        stream = LargeBinaryOutputStream(large_binary)
        assert stream.writable() is True

        stream.close()
        assert stream.writable() is False

    def test_seekable(self, large_binary):
        """Test seekable() method (should always return False)."""
        stream = LargeBinaryOutputStream(large_binary)
        assert stream.seekable() is False

    def test_closed_property(self, large_binary):
        """Test closed property."""
        stream = LargeBinaryOutputStream(large_binary)
        assert stream.closed is False

        stream.close()
        assert stream.closed is True

    def test_flush(self, large_binary):
        """Test flush() method (should be a no-op)."""
        stream = LargeBinaryOutputStream(large_binary)
        # Should not raise any exception
        stream.flush()

    def test_close_completes_upload(self, large_binary):
        """Test that close() completes the upload."""
        with (
            patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client,
            patch.object(
                large_binary_manager, "_ensure_bucket_exists"
            ) as mock_ensure_bucket,
        ):
            mock_s3 = MagicMock()
            mock_get_s3_client.return_value = mock_s3
            mock_ensure_bucket.return_value = None

            stream = LargeBinaryOutputStream(large_binary)
            stream.write(b"test data")

            # Close should wait for upload to complete
            stream.close()

            # Verify upload_fileobj was called
            assert mock_s3.upload_fileobj.called

    def test_context_manager(self, large_binary):
        """Test using as context manager."""
        with (
            patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client,
            patch.object(
                large_binary_manager, "_ensure_bucket_exists"
            ) as mock_ensure_bucket,
        ):
            mock_s3 = MagicMock()
            mock_get_s3_client.return_value = mock_s3
            mock_ensure_bucket.return_value = None

            with LargeBinaryOutputStream(large_binary) as stream:
                stream.write(b"test data")
                assert not stream._closed

            # Stream should be closed after context exit
            assert stream._closed

    def test_write_after_close_raises_error(self, large_binary):
        """Test that writing after close raises ValueError."""
        stream = LargeBinaryOutputStream(large_binary)
        stream.close()

        with pytest.raises(ValueError, match="I/O operation on closed stream"):
            stream.write(b"data")

    def test_close_handles_upload_error(self, large_binary):
        """Test that close() raises IOError if upload fails."""
        with (
            patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client,
            patch.object(
                large_binary_manager, "_ensure_bucket_exists"
            ) as mock_ensure_bucket,
        ):
            mock_s3 = MagicMock()
            mock_get_s3_client.return_value = mock_s3
            mock_ensure_bucket.return_value = None
            mock_s3.upload_fileobj.side_effect = Exception("Upload failed")

            stream = LargeBinaryOutputStream(large_binary)
            stream.write(b"test data")

            with pytest.raises(IOError, match="Failed to complete upload"):
                stream.close()

    def test_write_after_upload_error_raises_error(self, large_binary):
        """Test that writing after upload error raises IOError."""
        with (
            patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client,
            patch.object(
                large_binary_manager, "_ensure_bucket_exists"
            ) as mock_ensure_bucket,
        ):
            mock_s3 = MagicMock()
            mock_get_s3_client.return_value = mock_s3
            mock_ensure_bucket.return_value = None
            mock_s3.upload_fileobj.side_effect = Exception("Upload failed")

            stream = LargeBinaryOutputStream(large_binary)
            stream.write(b"test data")

            # Wait a bit for the error to be set
            time.sleep(0.1)

            with pytest.raises(IOError, match="Background upload failed"):
                stream.write(b"more data")

    def test_multiple_close_calls(self, large_binary):
        """Test that multiple close() calls are safe."""
        with (
            patch.object(large_binary_manager, "_get_s3_client") as mock_get_s3_client,
            patch.object(
                large_binary_manager, "_ensure_bucket_exists"
            ) as mock_ensure_bucket,
        ):
            mock_s3 = MagicMock()
            mock_get_s3_client.return_value = mock_s3
            mock_ensure_bucket.return_value = None

            stream = LargeBinaryOutputStream(large_binary)
            stream.write(b"test data")
            stream.close()
            # Second close should not raise error
            stream.close()
