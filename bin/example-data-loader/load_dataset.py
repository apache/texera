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
Dataset management module for Texera API.
Handles dataset creation, file upload with multipart support.
"""

import requests
import os
import math
from typing import Optional, List, Dict, Any
from urllib.parse import quote
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


class TexeraDatasetLoader:
    """Handle dataset operations with Texera server."""

    # S3 multipart upload constraints
    MINIMUM_PART_SIZE = 5 * 1024 * 1024  # 5 MiB
    MAXIMUM_NUM_PARTS = 10000
    DEFAULT_PART_SIZE = 16 * 1024 * 1024  # 16 MB (matching server default)
    DEFAULT_CONCURRENCY = 10  # Number of concurrent part uploads

    def __init__(self, base_url: str, auth_headers: dict):
        """
        Initialize dataset loader.

        Args:
            base_url: Base URL of Texera server
            auth_headers: Authorization headers from TexeraAuth
        """
        self.base_url = base_url.rstrip('/')
        self.api_url = f"{self.base_url}/api"
        self.auth_headers = auth_headers

    def create_dataset(
        self,
        name: str,
        description: str = "",
        is_public: bool = False,
        is_downloadable: bool = True
    ) -> Dict[str, Any]:
        """
        Create a new dataset.

        Args:
            name: Dataset name
            description: Dataset description
            is_public: Whether dataset is public
            is_downloadable: Whether dataset can be downloaded

        Returns:
            Created dataset information

        Raises:
            Exception: If dataset creation fails
        """
        url = f"{self.api_url}/dataset/create"
        payload = {
            "datasetName": name,
            "datasetDescription": description,
            "isDatasetPublic": is_public,
            "isDatasetDownloadable": is_downloadable
        }

        logger.info(f"Creating dataset: {name}")

        try:
            response = requests.post(
                url,
                json=payload,
                headers=self.auth_headers,
                timeout=30
            )
            response.raise_for_status()

            dataset_info = response.json()
            logger.info(f"Dataset created successfully: {dataset_info}")
            return dataset_info

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create dataset: {e}")
            raise Exception(f"Failed to create dataset: {e}")

    def list_datasets(self) -> List[Dict[str, Any]]:
        """
        List all datasets accessible by the user.

        Returns:
            List of dataset information
        """
        url = f"{self.api_url}/dataset/list"

        try:
            response = requests.get(
                url,
                headers=self.auth_headers,
                timeout=30
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list datasets: {e}")
            raise Exception(f"Failed to list datasets: {e}")

    def _calculate_part_size(self, file_size: int) -> int:
        """
        Calculate optimal part size for multipart upload.

        Args:
            file_size: Total file size in bytes

        Returns:
            Part size in bytes
        """
        part_size = self.DEFAULT_PART_SIZE

        # Calculate number of parts needed
        num_parts = math.ceil(file_size / part_size)

        # If exceeds S3 limit, increase part size
        if num_parts > self.MAXIMUM_NUM_PARTS:
            part_size = max(
                self.MINIMUM_PART_SIZE,
                math.ceil(file_size / (self.MAXIMUM_NUM_PARTS - 1))
            )

        logger.info(f"Using part size: {part_size / (1024*1024):.2f} MB")
        return part_size

    def _initiate_multipart_upload(
        self,
        owner_email: str,
        dataset_name: str,
        file_path: str,
        num_parts: int
    ) -> Dict[str, Any]:
        """
        Initiate multipart upload and get presigned URLs.

        Args:
            owner_email: Owner email address
            dataset_name: Dataset name
            file_path: File path within dataset
            num_parts: Number of parts to upload

        Returns:
            Dict with uploadId, presignedUrls, and physicalAddress
        """
        url = f"{self.api_url}/dataset/multipart-upload"
        params = {
            "type": "init",
            "ownerEmail": owner_email,
            "datasetName": dataset_name,
            "filePath": quote(file_path, safe=''),
            "numParts": num_parts
        }

        logger.info(f"Initiating multipart upload for {file_path} ({num_parts} parts)")

        try:
            response = requests.post(
                url,
                params=params,
                headers=self.auth_headers,
                timeout=60
            )
            response.raise_for_status()

            result = response.json()
            logger.info(f"Multipart upload initiated: {result.get('uploadId')}")
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to initiate multipart upload: {e}")
            raise Exception(f"Failed to initiate multipart upload: {e}")

    def _upload_part(
        self,
        presigned_url: str,
        part_data: bytes,
        part_number: int
    ) -> Dict[str, Any]:
        """
        Upload a single part to S3 using presigned URL.

        Args:
            presigned_url: Presigned URL for this part
            part_data: Binary data for this part
            part_number: Part number (1-indexed)

        Returns:
            Dict with PartNumber and ETag
        """
        logger.debug(f"Uploading part {part_number} ({len(part_data)} bytes)")

        try:
            response = requests.put(
                presigned_url,
                data=part_data,
                headers={"Content-Length": str(len(part_data))},
                timeout=300  # 5 minutes per part
            )
            response.raise_for_status()

            # Extract ETag from response headers
            etag = response.headers.get("ETag", "").strip('"')

            if not etag:
                raise Exception(f"No ETag received for part {part_number}")

            logger.debug(f"Part {part_number} uploaded successfully (ETag: {etag})")

            return {
                "PartNumber": part_number,
                "ETag": etag
            }

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to upload part {part_number}: {e}")
            raise Exception(f"Failed to upload part {part_number}: {e}")

    def _complete_multipart_upload(
        self,
        owner_email: str,
        dataset_name: str,
        file_path: str,
        upload_id: str,
        physical_address: str,
        parts: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Complete multipart upload after all parts are uploaded.

        Args:
            owner_email: Owner email address
            dataset_name: Dataset name
            file_path: File path within dataset
            upload_id: Upload ID from initiation
            physical_address: Physical S3 address
            parts: List of part information with PartNumber and ETag

        Returns:
            Completion response
        """
        url = f"{self.api_url}/dataset/multipart-upload"
        params = {
            "type": "finish",
            "ownerEmail": owner_email,
            "datasetName": dataset_name,
            "filePath": quote(file_path, safe=''),
            "uploadId": upload_id
        }

        # Sort parts by part number
        sorted_parts = sorted(parts, key=lambda x: x["PartNumber"])

        payload = {
            "parts": sorted_parts,
            "physicalAddress": physical_address
        }

        logger.info(f"Completing multipart upload for {file_path}")

        try:
            response = requests.post(
                url,
                params=params,
                json=payload,
                headers=self.auth_headers,
                timeout=120
            )
            response.raise_for_status()

            result = response.json()
            logger.info(f"Multipart upload completed successfully")
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to complete multipart upload: {e}")
            raise Exception(f"Failed to complete multipart upload: {e}")

    def _abort_multipart_upload(
        self,
        owner_email: str,
        dataset_name: str,
        file_path: str,
        upload_id: str,
        physical_address: str
    ):
        """
        Abort multipart upload on error.

        Args:
            owner_email: Owner email address
            dataset_name: Dataset name
            file_path: File path within dataset
            upload_id: Upload ID from initiation
            physical_address: Physical S3 address
        """
        url = f"{self.api_url}/dataset/multipart-upload"
        params = {
            "type": "abort",
            "ownerEmail": owner_email,
            "datasetName": dataset_name,
            "filePath": quote(file_path, safe=''),
            "uploadId": upload_id
        }

        payload = {
            "physicalAddress": physical_address
        }

        logger.warning(f"Aborting multipart upload for {file_path}")

        try:
            requests.post(
                url,
                params=params,
                json=payload,
                headers=self.auth_headers,
                timeout=60
            )
            logger.info("Multipart upload aborted")

        except Exception as e:
            logger.error(f"Failed to abort multipart upload: {e}")

    def upload_file_multipart(
        self,
        owner_email: str,
        dataset_name: str,
        local_file_path: str,
        remote_file_path: Optional[str] = None,
        concurrency: int = DEFAULT_CONCURRENCY
    ) -> Dict[str, Any]:
        """
        Upload a file to dataset using multipart upload.

        Args:
            owner_email: Owner email address
            dataset_name: Dataset name
            local_file_path: Path to local file
            remote_file_path: Path in dataset (defaults to filename)
            concurrency: Number of concurrent part uploads

        Returns:
            Upload completion response

        Raises:
            Exception: If upload fails
        """
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"File not found: {local_file_path}")

        if remote_file_path is None:
            remote_file_path = os.path.basename(local_file_path)

        file_size = os.path.getsize(local_file_path)
        logger.info(f"Uploading {local_file_path} ({file_size} bytes) to {dataset_name}/{remote_file_path}")

        # Calculate part size and number of parts
        part_size = self._calculate_part_size(file_size)
        num_parts = math.ceil(file_size / part_size)

        # Initiate multipart upload
        init_response = self._initiate_multipart_upload(
            owner_email,
            dataset_name,
            remote_file_path,
            num_parts
        )

        upload_id = init_response["uploadId"]
        presigned_urls = init_response["presignedUrls"]
        physical_address = init_response["physicalAddress"]

        try:
            # Read file and upload parts in parallel
            uploaded_parts = []

            with open(local_file_path, 'rb') as f:
                with ThreadPoolExecutor(max_workers=concurrency) as executor:
                    futures = []

                    for part_num in range(1, num_parts + 1):
                        # Read part data
                        offset = (part_num - 1) * part_size
                        f.seek(offset)
                        part_data = f.read(part_size)

                        # Submit upload task
                        presigned_url = presigned_urls[part_num - 1]
                        future = executor.submit(
                            self._upload_part,
                            presigned_url,
                            part_data,
                            part_num
                        )
                        futures.append(future)

                    # Wait for all uploads to complete
                    for future in as_completed(futures):
                        part_info = future.result()
                        uploaded_parts.append(part_info)
                        logger.info(f"Progress: {len(uploaded_parts)}/{num_parts} parts uploaded")

            # Complete multipart upload
            result = self._complete_multipart_upload(
                owner_email,
                dataset_name,
                remote_file_path,
                upload_id,
                physical_address,
                uploaded_parts
            )

            logger.info(f"File uploaded successfully: {remote_file_path}")
            return result

        except Exception as e:
            # Abort upload on error
            logger.error(f"Upload failed, aborting: {e}")
            self._abort_multipart_upload(
                owner_email,
                dataset_name,
                remote_file_path,
                upload_id,
                physical_address
            )
            raise

    def get_dataset_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get dataset information by name.

        Args:
            name: Dataset name

        Returns:
            Dataset information or None if not found
        """
        datasets = self.list_datasets()
        for ds in datasets:
            if ds.get("dataset", {}).get("name") == name:
                return ds
        return None
