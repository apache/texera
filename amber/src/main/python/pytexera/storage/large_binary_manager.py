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

import time
import uuid
from loguru import logger
from core.storage.storage_config import StorageConfig

# Module-level state
_s3_client = None
DEFAULT_BUCKET = "texera-large-binaries"


def _get_s3_client():
    """Get or initialize S3 client (lazy initialization, cached)."""
    global _s3_client
    if _s3_client is None:
        try:
            import boto3
            from botocore.config import Config
        except ImportError as e:
            raise RuntimeError("boto3 required. Install with: pip install boto3") from e

        _s3_client = boto3.client(
            "s3",
            endpoint_url=StorageConfig.S3_ENDPOINT,
            aws_access_key_id=StorageConfig.S3_AUTH_USERNAME,
            aws_secret_access_key=StorageConfig.S3_AUTH_PASSWORD,
            region_name=StorageConfig.S3_REGION,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
    return _s3_client


def _ensure_bucket_exists(bucket: str):
    """Ensure S3 bucket exists, creating it if necessary."""
    s3 = _get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket)
    except s3.exceptions.NoSuchBucket:
        logger.debug(f"Bucket {bucket} not found, creating it")
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Created bucket: {bucket}")


def create() -> str:
    """
    Creates a new largebinary reference with a unique S3 URI.

    Returns:
        S3 URI string (format: s3://bucket/key)
    """
    _ensure_bucket_exists(DEFAULT_BUCKET)
    timestamp_ms = int(time.time() * 1000)
    unique_id = uuid.uuid4()
    object_key = f"objects/{timestamp_ms}/{unique_id}"
    return f"s3://{DEFAULT_BUCKET}/{object_key}"
