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

import pytest
from unittest.mock import patch, MagicMock

from pytexera.storage.dataset_file_document import DatasetFileDocument


DEFAULT_ENDPOINT = "http://localhost:9092/api/dataset/presign-download"
CUSTOM_ENDPOINT = "https://example.test/api/presign"


@pytest.fixture
def auth_env(monkeypatch):
    """Provide a JWT and pinned presign endpoint for the duration of one test."""
    monkeypatch.setenv("USER_JWT_TOKEN", "test-jwt-token")
    monkeypatch.setenv("FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT", CUSTOM_ENDPOINT)


def make_response(status_code: int, body=None, content: bytes = b""):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = body or {}
    response.text = "" if body is None else str(body)
    response.content = content
    return response


class TestDatasetFileDocumentInit:
    def test_parses_minimal_four_part_path(self, auth_env):
        doc = DatasetFileDocument("/bob@x.com/ds/v1/file.csv")
        assert doc.owner_email == "bob@x.com"
        assert doc.dataset_name == "ds"
        assert doc.version_name == "v1"
        assert doc.file_relative_path == "file.csv"

    def test_joins_nested_relative_path_back_with_slashes(self, auth_env):
        doc = DatasetFileDocument("/bob@x.com/ds/v1/a/b/c/file.csv")
        assert doc.file_relative_path == "a/b/c/file.csv"

    def test_strips_leading_and_trailing_slashes_before_parsing(self, auth_env):
        doc = DatasetFileDocument("///bob@x.com/ds/v1/file.csv///")
        assert doc.owner_email == "bob@x.com"
        assert doc.file_relative_path == "file.csv"

    def test_rejects_path_with_fewer_than_four_segments(self, auth_env):
        with pytest.raises(ValueError, match="Invalid file path format"):
            DatasetFileDocument("/bob@x.com/ds/v1")

    def test_requires_jwt_token_in_environment(self, monkeypatch):
        monkeypatch.delenv("USER_JWT_TOKEN", raising=False)
        monkeypatch.setenv("FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT", CUSTOM_ENDPOINT)
        with pytest.raises(ValueError, match="JWT token is required"):
            DatasetFileDocument("/bob@x.com/ds/v1/file.csv")

    def test_treats_empty_jwt_as_missing(self, monkeypatch):
        # An empty string is falsy and should be rejected just like an unset var.
        monkeypatch.setenv("USER_JWT_TOKEN", "")
        with pytest.raises(ValueError, match="JWT token is required"):
            DatasetFileDocument("/bob@x.com/ds/v1/file.csv")

    def test_falls_back_to_default_endpoint_when_env_missing(self, monkeypatch):
        monkeypatch.setenv("USER_JWT_TOKEN", "tok")
        monkeypatch.delenv("FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT", raising=False)
        doc = DatasetFileDocument("/bob@x.com/ds/v1/file.csv")
        assert doc.presign_endpoint == DEFAULT_ENDPOINT

    def test_uses_explicit_endpoint_from_environment(self, auth_env):
        doc = DatasetFileDocument("/bob@x.com/ds/v1/file.csv")
        assert doc.presign_endpoint == CUSTOM_ENDPOINT


class TestGetPresignedUrl:
    def _make_doc(self, monkeypatch, path="/bob@x.com/ds/v1/file.csv"):
        monkeypatch.setenv("USER_JWT_TOKEN", "test-jwt-token")
        monkeypatch.setenv("FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT", CUSTOM_ENDPOINT)
        return DatasetFileDocument(path)

    def test_returns_presigned_url_field_from_json_body(self, monkeypatch):
        doc = self._make_doc(monkeypatch)
        with patch("pytexera.storage.dataset_file_document.requests.get") as mock_get:
            mock_get.return_value = make_response(
                200, body={"presignedUrl": "https://signed.test/x"}
            )
            assert doc.get_presigned_url() == "https://signed.test/x"

    def test_sends_bearer_authorization_header_with_jwt(self, monkeypatch):
        doc = self._make_doc(monkeypatch)
        with patch("pytexera.storage.dataset_file_document.requests.get") as mock_get:
            mock_get.return_value = make_response(200, body={"presignedUrl": "u"})
            doc.get_presigned_url()
            _, kwargs = mock_get.call_args
            assert kwargs["headers"] == {"Authorization": "Bearer test-jwt-token"}

    def test_url_encodes_filepath_query_parameter(self, monkeypatch):
        # urllib.parse.quote keeps "/" as safe by default, but encodes "@"
        # and " " — pin both pieces so the contract is explicit.
        doc = self._make_doc(monkeypatch, path="/bob@x.com/ds/v1/data file.csv")
        with patch("pytexera.storage.dataset_file_document.requests.get") as mock_get:
            mock_get.return_value = make_response(200, body={"presignedUrl": "u"})
            doc.get_presigned_url()
            _, kwargs = mock_get.call_args
            file_path = kwargs["params"]["filePath"]
            assert "data%20file.csv" in file_path
            assert "bob%40x.com" in file_path
            assert file_path.startswith("/")

    def test_calls_configured_endpoint(self, monkeypatch):
        doc = self._make_doc(monkeypatch)
        with patch("pytexera.storage.dataset_file_document.requests.get") as mock_get:
            mock_get.return_value = make_response(200, body={"presignedUrl": "u"})
            doc.get_presigned_url()
            args, _ = mock_get.call_args
            assert args[0] == CUSTOM_ENDPOINT

    def test_raises_runtime_error_with_status_and_body_on_failure(self, monkeypatch):
        doc = self._make_doc(monkeypatch)
        with patch("pytexera.storage.dataset_file_document.requests.get") as mock_get:
            mock_get.return_value = make_response(403, body="forbidden")
            with pytest.raises(RuntimeError, match=r"403.*forbidden"):
                doc.get_presigned_url()

    def test_returns_none_when_response_body_lacks_presigned_url_key(self, monkeypatch):
        # Pins current behavior: a 200 with no "presignedUrl" key yields None
        # rather than raising. read_file() will then call requests.get(None).
        doc = self._make_doc(monkeypatch)
        with patch("pytexera.storage.dataset_file_document.requests.get") as mock_get:
            mock_get.return_value = make_response(200, body={"other": "value"})
            assert doc.get_presigned_url() is None


class TestReadFile:
    def _make_doc(self, monkeypatch):
        monkeypatch.setenv("USER_JWT_TOKEN", "test-jwt-token")
        monkeypatch.setenv("FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT", CUSTOM_ENDPOINT)
        return DatasetFileDocument("/bob@x.com/ds/v1/file.csv")

    def test_returns_bytesio_with_downloaded_content(self, monkeypatch):
        doc = self._make_doc(monkeypatch)
        with patch("pytexera.storage.dataset_file_document.requests.get") as mock_get:
            mock_get.side_effect = [
                make_response(200, body={"presignedUrl": "https://signed.test/x"}),
                make_response(200, content=b"hello-bytes"),
            ]
            buf = doc.read_file()
            assert isinstance(buf, io.BytesIO)
            assert buf.read() == b"hello-bytes"

    def test_propagates_presigned_url_failure(self, monkeypatch):
        doc = self._make_doc(monkeypatch)
        with patch("pytexera.storage.dataset_file_document.requests.get") as mock_get:
            mock_get.return_value = make_response(500, body="upstream down")
            with pytest.raises(RuntimeError, match=r"500.*upstream down"):
                doc.read_file()

    def test_raises_runtime_error_when_download_fails(self, monkeypatch):
        doc = self._make_doc(monkeypatch)
        with patch("pytexera.storage.dataset_file_document.requests.get") as mock_get:
            mock_get.side_effect = [
                make_response(200, body={"presignedUrl": "https://signed.test/x"}),
                make_response(404, body="missing"),
            ]
            with pytest.raises(RuntimeError, match=r"404.*missing"):
                doc.read_file()

    def test_downloads_from_presigned_url_returned_by_first_call(self, monkeypatch):
        doc = self._make_doc(monkeypatch)
        with patch("pytexera.storage.dataset_file_document.requests.get") as mock_get:
            mock_get.side_effect = [
                make_response(200, body={"presignedUrl": "https://signed.test/x"}),
                make_response(200, content=b""),
            ]
            doc.read_file()
            second_call_args, _ = mock_get.call_args_list[1]
            assert second_call_args[0] == "https://signed.test/x"
