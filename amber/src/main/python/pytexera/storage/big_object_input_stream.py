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
BigObjectInputStream for reading BigObject data from S3.

Usage:
    with BigObjectInputStream(big_object) as stream:
        content = stream.read()
"""

from typing import BinaryIO, Optional
from functools import wraps
from io import IOBase
from core.models.schema.big_object import BigObject


def _require_open(func):
    """Decorator to ensure stream is open before reading operations."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self._closed:
            raise ValueError("I/O operation on closed stream")
        if self._underlying is None:
            self._lazy_init()
        return func(self, *args, **kwargs)

    return wrapper


class BigObjectInputStream(IOBase):
    """
    InputStream for reading BigObject data from S3.

    Lazily downloads from S3 on first read. Supports context manager and iteration.
    """

    def __init__(self, big_object: BigObject):
        """Initialize stream for reading the given BigObject."""
        super().__init__()
        if big_object is None:
            raise ValueError("BigObject cannot be None")
        self._big_object = big_object
        self._underlying: Optional[BinaryIO] = None
        self._closed = False

    def _lazy_init(self):
        """Download from S3 on first read operation."""
        from pytexera.storage.big_object_manager import BigObjectManager

        s3 = BigObjectManager._get_s3_client()
        response = s3.get_object(
            Bucket=self._big_object.get_bucket_name(),
            Key=self._big_object.get_object_key(),
        )
        self._underlying = response["Body"]

    @_require_open
    def read(self, n: int = -1) -> bytes:
        """Read and return up to n bytes (-1 reads all)."""
        return self._underlying.read(n)

    @_require_open
    def readline(self, size: int = -1) -> bytes:
        """Read and return one line from the stream."""
        return self._underlying.readline(size)

    @_require_open
    def readlines(self, hint: int = -1) -> list[bytes]:
        """Read and return a list of lines from the stream."""
        return self._underlying.readlines(hint)

    def readable(self) -> bool:
        """Return True if the stream can be read from."""
        return not self._closed

    def seekable(self) -> bool:
        """Return False - this stream does not support seeking."""
        return False

    @property
    def closed(self) -> bool:
        """Return True if the stream is closed."""
        return self._closed

    def close(self) -> None:
        """Close the stream and release resources."""
        if not self._closed:
            self._closed = True
            if self._underlying is not None:
                self._underlying.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        line = self.readline()
        if not line:
            raise StopIteration
        return line
