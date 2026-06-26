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

import io
import os
import requests
import urllib.parse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class DatasetFileDocument:
    # (connect, read) timeout and retry settings for the file-service GETs below.
    # Read timeout bounds inactivity between bytes, not total download time.
    _CONNECT_TIMEOUT_SECONDS = 5
    _READ_TIMEOUT_SECONDS = 10
    _REQUEST_TIMEOUT = (_CONNECT_TIMEOUT_SECONDS, _READ_TIMEOUT_SECONDS)
    _MAX_RETRIES = 3
    _RETRY_BACKOFF_FACTOR = 0.5
    _RETRY_STATUS_FORCELIST = (500, 502, 503, 504)

    @classmethod
    def _retry_session(cls) -> requests.Session:
        """Returns a Session that retries GETs on connection errors and 5xx."""
        retry = Retry(
            total=cls._MAX_RETRIES,
            connect=cls._MAX_RETRIES,
            read=cls._MAX_RETRIES,
            backoff_factor=cls._RETRY_BACKOFF_FACTOR,
            status_forcelist=cls._RETRY_STATUS_FORCELIST,
            allowed_methods=frozenset({"GET"}),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def __init__(self, file_path: str):
        """
        Parses the file path into dataset metadata.

        :param file_path:
           Expected format - "/ownerEmail/datasetName/versionName/fileRelativePath"
           Example: "/bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv"
        """
        parts = file_path.strip("/").split("/")
        if len(parts) < 4:
            raise ValueError(
                "Invalid file path format. "
                "Expected: /ownerEmail/datasetName/versionName/fileRelativePath"
            )

        self.owner_email = parts[0]
        self.dataset_name = parts[1]
        self.version_name = parts[2]
        self.file_relative_path = "/".join(parts[3:])

        self.jwt_token = os.getenv("USER_JWT_TOKEN")
        self.presign_endpoint = os.getenv("FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT")

        if not self.jwt_token:
            raise ValueError(
                "JWT token is required but not set in environment variables."
            )
        if not self.presign_endpoint:
            self.presign_endpoint = "http://localhost:9092/api/dataset/presign-download"

    def get_presigned_url(self) -> str:
        """
        Requests a presigned URL from the API.

        :return: The presigned URL as a string.
        :raises: RuntimeError if the request fails.
        """
        headers = {"Authorization": f"Bearer {self.jwt_token}"}
        encoded_file_path = urllib.parse.quote(
            f"/{self.owner_email}"
            f"/{self.dataset_name}"
            f"/{self.version_name}"
            f"/{self.file_relative_path}"
        )

        params = {"filePath": encoded_file_path}

        try:
            with self._retry_session() as session:
                response = session.get(
                    self.presign_endpoint,
                    headers=headers,
                    params=params,
                    timeout=self._REQUEST_TIMEOUT,
                )
        except requests.exceptions.RequestException as e:
            raise RuntimeError(
                f"Failed to get presigned URL: request failed: {e}"
            ) from e

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to get presigned URL: {response.status_code} {response.text}"
            )

        try:
            payload = response.json()
        except ValueError as e:
            raise RuntimeError(
                f"Failed to get presigned URL: invalid JSON response: {response.text}"
            ) from e

        presigned_url = payload.get("presignedUrl")
        if not isinstance(presigned_url, str) or not presigned_url:
            raise RuntimeError(
                f"Failed to get presigned URL: 'presignedUrl' missing from "
                f"response: {response.text}"
            )

        return presigned_url

    def read_file(self) -> io.BytesIO:
        """
        Reads the file content from the presigned URL.

        :return: A file-like object.
        :raises: RuntimeError if the retrieval fails.
        """
        presigned_url = self.get_presigned_url()
        try:
            with self._retry_session() as session:
                response = session.get(presigned_url, timeout=self._REQUEST_TIMEOUT)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(
                f"Failed to retrieve file content: request failed: {e}"
            ) from e

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to retrieve file content: "
                f"{response.status_code} {response.text}"
            )

        return io.BytesIO(response.content)
