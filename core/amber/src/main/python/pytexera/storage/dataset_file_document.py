import os
import io
import requests
import urllib.parse


class DatasetFileDocument:
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

        response = requests.get(self.presign_endpoint, headers=headers, params=params)

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to get presigned URL: "
                f"{response.status_code} {response.text}"
            )

        return response.json().get("presignedUrl")

    def read_file(self) -> io.BytesIO:
        """
        Reads the file content from the presigned URL.

        :return: A file-like object.
        :raises: RuntimeError if the retrieval fails.
        """
        presigned_url = self.get_presigned_url()
        response = requests.get(presigned_url)

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to retrieve file content: "
                f"{response.status_code} {response.text}"
            )

        return io.BytesIO(response.content)
