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
Internal largebinary manager for S3 operations.

Users should not interact with this module directly. Use largebinary() constructor
and LargeBinaryInputStream/LargeBinaryOutputStream instead.
"""

import uuid
from loguru import logger
from core.storage.storage_config import StorageConfig


class LargeBinaryManager:
    """Manages execution-scoped large binaries in S3 for a worker process.

    Implemented as a singleton: ``LargeBinaryManager()`` always returns the same
    instance, so the cached S3 client and the current execution id are shared across
    all callers in the worker process. A Python worker is a single process serving one
    execution. Mirrors the JVM ``object LargeBinaryManager``.
    """

    DEFAULT_BUCKET = "texera-large-binaries"

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            instance = super().__new__(cls)
            instance._s3_client = None
            # Execution context: set at executor init and read by create() so the
            # user-facing largebinary() API stays execution-id-free.
            instance._current_execution_id = None
            cls._instance = instance
        return cls._instance

    def set_current_execution_id(self, execution_id):
        """Sets the execution id used to scope large binaries created by this worker."""
        self._current_execution_id = execution_id

    def get_current_execution_id(self):
        """Returns the execution id set for this worker, or None if unset."""
        return self._current_execution_id

    def _get_s3_client(self):
        """Get or initialize the S3 client (lazy initialization, cached)."""
        if self._s3_client is None:
            try:
                import boto3
                from botocore.config import Config
            except ImportError as e:
                raise RuntimeError(
                    "boto3 required. Install with: pip install boto3"
                ) from e

            self._s3_client = boto3.client(
                "s3",
                endpoint_url=StorageConfig.S3_ENDPOINT,
                aws_access_key_id=StorageConfig.S3_AUTH_USERNAME,
                aws_secret_access_key=StorageConfig.S3_AUTH_PASSWORD,
                region_name=StorageConfig.S3_REGION,
                config=Config(
                    signature_version="s3v4", s3={"addressing_style": "path"}
                ),
            )
        return self._s3_client

    def _ensure_bucket_exists(self, bucket: str):
        """Ensure the S3 bucket exists, creating it if necessary."""
        s3 = self._get_s3_client()
        try:
            s3.head_bucket(Bucket=bucket)
        except s3.exceptions.NoSuchBucket:
            logger.debug(f"Bucket {bucket} not found, creating it")
            s3.create_bucket(Bucket=bucket)
            logger.info(f"Created bucket: {bucket}")

    def create(self) -> str:
        """
        Creates a new largebinary reference with a unique, execution-scoped S3 URI.

        The object key is namespaced by the current execution id so cleanup can delete
        only this execution's objects. The execution id is injected by the system (set
        via set_current_execution_id() when the worker is initialized); callers never
        pass it.

        Returns:
            S3 URI string (format: s3://bucket/objects/{execution_id}/{uuid})
        """
        self._ensure_bucket_exists(self.DEFAULT_BUCKET)
        execution_id = self.get_current_execution_id()
        if execution_id is None:
            raise RuntimeError(
                "largebinary() requires an execution context, but no execution id "
                "has been set for this worker."
            )
        unique_id = uuid.uuid4()
        object_key = f"objects/{execution_id}/{unique_id}"
        return f"s3://{self.DEFAULT_BUCKET}/{object_key}"
