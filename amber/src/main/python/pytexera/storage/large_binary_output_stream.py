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

"""
LargeBinaryOutputStream for streaming largebinary data to S3.

Usage:
    from pytexera import largebinary, LargeBinaryOutputStream

    large_binary = largebinary()
    with LargeBinaryOutputStream(large_binary) as out:
        out.write(b"data")
"""

from typing import Optional, Union
from io import IOBase
from core.models.type.large_binary import largebinary
from pytexera.storage import large_binary_manager
import threading
import queue

# Constants
_CHUNK_SIZE = 64 * 1024  # 64KB
_QUEUE_TIMEOUT = 0.1


class _QueueReader:
    """File-like object that reads from a queue."""

    def __init__(self, q: queue.Queue):
        self._queue = q
        self._buffer = b""
        self._eof = False

    def read(self, size=-1):
        """Read bytes from the queue."""
        if self._eof and not self._buffer:
            return b""

        # Collect chunks until we have enough data or reach EOF
        chunks = [self._buffer] if self._buffer else []
        total_size = len(self._buffer)
        self._buffer = b""
        needed = size if size != -1 else None

        while not self._eof and (needed is None or total_size < needed):
            try:
                chunk = self._queue.get(timeout=_QUEUE_TIMEOUT)
                if chunk is None:  # EOF marker
                    self._eof = True
                    break
                chunks.append(chunk)
                total_size += len(chunk)
            except queue.Empty:
                continue

        result = b"".join(chunks)

        # If size was specified, split and buffer remainder
        if needed is not None and len(result) > needed:
            self._buffer = result[needed:]
            result = result[:needed]

        return result


class LargeBinaryOutputStream(IOBase):
    """
    OutputStream for streaming largebinary data to S3.

    Data is uploaded in the background using multipart upload as you write.
    Call close() to complete the upload and ensure all data is persisted.

    This class follows Python's standard I/O interface (io.IOBase).

    Usage:
        from pytexera import largebinary, LargeBinaryOutputStream

        # Create a new largebinary and write to it
        large_binary = largebinary()
        with LargeBinaryOutputStream(large_binary) as out:
            out.write(b"Hello, World!")
            out.write(b"More data")
        # large_binary is now ready to be added to tuples

    Note: Not thread-safe. Do not access from multiple threads concurrently.
    """

    def __init__(self, large_binary: largebinary):
        """
        Initialize a LargeBinaryOutputStream.

        Args:
            large_binary: The largebinary reference to write to

        Raises:
            ValueError: If large_binary is None
        """
        super().__init__()
        if large_binary is None:
            raise ValueError("largebinary cannot be None")

        self._large_binary = large_binary
        self._bucket_name = large_binary.get_bucket_name()
        self._object_key = large_binary.get_object_key()
        self._closed = False

        # Background upload thread state
        self._queue: queue.Queue = queue.Queue(maxsize=_CHUNK_SIZE)
        self._upload_exception: Optional[Exception] = None
        self._upload_complete = threading.Event()
        self._upload_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def write(self, b: Union[bytes, bytearray]) -> int:
        """
        Write bytes to the stream.

        Args:
            b: Bytes to write

        Returns:
            Number of bytes written

        Raises:
            ValueError: If stream is closed
            IOError: If previous upload failed
        """
        if self._closed:
            raise ValueError("I/O operation on closed stream")

        # Check if upload has failed
        with self._lock:
            if self._upload_exception is not None:
                raise IOError(
                    f"Background upload failed: {self._upload_exception}"
                ) from self._upload_exception

        # Start upload thread on first write
        if self._upload_thread is None:

            def upload_worker():
                try:
                    large_binary_manager._ensure_bucket_exists(self._bucket_name)
                    s3 = large_binary_manager._get_s3_client()
                    reader = _QueueReader(self._queue)
                    s3.upload_fileobj(reader, self._bucket_name, self._object_key)
                except Exception as e:
                    with self._lock:
                        self._upload_exception = e
                finally:
                    self._upload_complete.set()

            self._upload_thread = threading.Thread(target=upload_worker, daemon=True)
            self._upload_thread.start()

        # Write data in chunks
        data = bytes(b)
        for offset in range(0, len(data), _CHUNK_SIZE):
            self._queue.put(data[offset : offset + _CHUNK_SIZE], block=True)

        return len(data)

    def writable(self) -> bool:
        """Return True if the stream can be written to."""
        return not self._closed

    def seekable(self) -> bool:
        """Return False - this stream does not support seeking."""
        return False

    @property
    def closed(self) -> bool:
        """Return True if the stream is closed."""
        return self._closed

    def flush(self) -> None:
        """
        Flush the write buffer.

        Note: This doesn't guarantee data is uploaded to S3 yet.
        Call close() to ensure upload completion.
        """
        # No-op: data is already being consumed by the upload thread
        pass

    def close(self) -> None:
        """
        Close the stream and complete the S3 upload.
        Blocks until upload is complete. Raises IOError if upload failed.

        Raises:
            IOError: If upload failed
        """
        if self._closed:
            return

        self._closed = True

        # Signal EOF to upload thread and wait for completion
        if self._upload_thread is not None:
            self._queue.put(None, block=True)  # EOF marker
            self._upload_thread.join()
            self._upload_complete.wait()

            # Check for errors and cleanup if needed
            with self._lock:
                exception = self._upload_exception

            if exception is not None:
                self._cleanup_failed_upload()
                raise IOError(f"Failed to complete upload: {exception}") from exception

    def _cleanup_failed_upload(self):
        """Clean up a failed upload by deleting the S3 object."""
        try:
            s3 = large_binary_manager._get_s3_client()
            s3.delete_object(Bucket=self._bucket_name, Key=self._object_key)
        except Exception:
            # Ignore cleanup errors - we're already handling an upload failure
            pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - automatically cleanup."""
        self.close()
        return False
