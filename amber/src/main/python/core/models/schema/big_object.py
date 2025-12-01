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
BigObject represents a reference to a large object stored externally (e.g., S3).
This is a schema type class used throughout the system for handling BIG_OBJECT attribute types.
"""

from typing import Optional
from urllib.parse import urlparse


class BigObject:
    """
    BigObject represents a reference to a large object stored in S3.

    Each BigObject is identified by an S3 URI (s3://bucket/path/to/object).
    BigObjects are automatically tracked and cleaned up when the workflow execution completes.

    Usage:
      from pytexera import BigObject, BigObjectInputStream, BigObjectOutputStream

      # Create a new BigObject for writing
      big_object = BigObject()
      with BigObjectOutputStream(big_object) as out:
          out.write(b"data")
      # big_object is now ready to be added to tuples

      # Read from an existing BigObject
      with BigObjectInputStream(big_object) as stream:
          content = stream.read()

      # Create from existing URI (e.g., from deserialization)
      big_object = BigObject("s3://bucket/path/to/object")
    """

    def __init__(self, uri: Optional[str] = None):
        """
        Create a BigObject.

        Args:
            uri: Optional S3 URI in the format s3://bucket/path/to/object.
                 If None, creates a new BigObject with a unique S3 URI.

        Raises:
            ValueError: If URI is provided but doesn't start with "s3://"
        """
        if uri is None:
            # Lazy import to avoid circular dependencies
            from pytexera.storage.big_object_manager import BigObjectManager

            uri = BigObjectManager.create()

        if not uri.startswith("s3://"):
            raise ValueError(f"BigObject URI must start with 's3://', got: {uri}")

        self._uri = uri

    @property
    def uri(self) -> str:
        """Get the S3 URI of this BigObject."""
        return self._uri

    def get_bucket_name(self) -> str:
        """Get the S3 bucket name from the URI."""
        return urlparse(self._uri).netloc

    def get_object_key(self) -> str:
        """Get the S3 object key (path) from the URI, without leading slash."""
        return urlparse(self._uri).path.lstrip("/")

    def __str__(self) -> str:
        return self._uri

    def __repr__(self) -> str:
        return f"BigObject('{self._uri}')"

    def __eq__(self, other) -> bool:
        return isinstance(other, BigObject) and self._uri == other._uri

    def __hash__(self) -> int:
        return hash(self._uri)
