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

import threading
import uuid
from loguru import logger
from core.storage.storage_config import StorageConfig


class LargeBinaryManager:
    """Manages large binaries in S3 for a worker process.

    A singleton, so the cached S3 client is shared process-wide. create() appends a
    unique suffix to an execution-scoped base URI handed down by the coordinator as
    process config (``StorageConfig.S3_LARGE_BINARIES_BASE_URI``); the worker never
    holds an execution id. This is the Python counterpart of the JVM
    ``LargeBinaryManager``, which uses a thread-local instead because one JVM process
    runs many workers across executions (a Python worker is one process per execution).
    """

    _instance = None
    # Guards singleton creation and S3-client init; reached from the operator and upload
    # threads.
    _lock = threading.Lock()

    def __new__(cls):
        # Double-checked locking: skip the lock once the instance exists.
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._s3_client = None
                    cls._instance = instance
        return cls._instance

    def _get_s3_client(self):
        """Get or initialize the S3 client (lazy initialization, cached)."""
        if self._s3_client is None:
            with self._lock:
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
        """Append a unique suffix to the coordinator-provided base URI.

        Pure string construction (no S3 round-trip); the bucket is created on demand at
        upload time. Returns e.g. ``s3://bucket/objects/{execution_id}/{uuid}``.
        """
        base_uri = StorageConfig.S3_LARGE_BINARIES_BASE_URI
        if not base_uri:
            raise RuntimeError(
                "largebinary() requires a large-binaries base URI, but none is "
                "configured (StorageConfig.S3_LARGE_BINARIES_BASE_URI is unset)."
            )
        return f"{base_uri}{uuid.uuid4()}"
