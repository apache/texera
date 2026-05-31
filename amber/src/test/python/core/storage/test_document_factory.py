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

from unittest.mock import MagicMock, patch

import pytest

from core.models import Schema
from core.storage.document_factory import DocumentFactory
from core.storage.storage_config import StorageConfig
from core.storage.vfs_uri_factory import VFSResourceType


# Avoid initializing the real config (only initializable once per process).
StorageConfig.ICEBERG_TABLE_RESULT_NAMESPACE = "test-result-ns"
StorageConfig.ICEBERG_TABLE_STATE_NAMESPACE = "test-state-ns"

VFS_URI = "vfs:///wid/0/eid/0/opid/test/main/0/0/result"


@pytest.fixture
def schema():
    return Schema(raw_schema={"x": "INTEGER"})


def _decode_returning(resource_type):
    """Helper: build a VFSURIFactory.decode_uri side_effect."""
    return lambda _uri: (None, None, None, resource_type)


@patch("core.storage.document_factory.IcebergDocument")
@patch("core.storage.document_factory.amber_schema_to_iceberg_schema")
@patch("core.storage.document_factory.create_table")
@patch("core.storage.document_factory.IcebergCatalogInstance")
@patch("core.storage.document_factory.VFSURIFactory")
class TestCreateDocumentNamespaceRouting:
    def test_state_resource_type_uses_state_namespace(
        self, mock_vfs, _icb, mock_create_table, _amber_schema, _doc, schema
    ):
        mock_vfs.VFS_FILE_URI_SCHEME = "vfs"
        mock_vfs.decode_uri.side_effect = _decode_returning(VFSResourceType.STATE)

        DocumentFactory.create_document(VFS_URI, schema)

        args, _ = mock_create_table.call_args
        assert args[1] == StorageConfig.ICEBERG_TABLE_STATE_NAMESPACE

    def test_result_resource_type_uses_result_namespace(
        self, mock_vfs, _icb, mock_create_table, _amber_schema, _doc, schema
    ):
        mock_vfs.VFS_FILE_URI_SCHEME = "vfs"
        mock_vfs.decode_uri.side_effect = _decode_returning(VFSResourceType.RESULT)

        DocumentFactory.create_document(VFS_URI, schema)

        args, _ = mock_create_table.call_args
        assert args[1] == StorageConfig.ICEBERG_TABLE_RESULT_NAMESPACE

    def test_unsupported_resource_type_raises_value_error(
        self, mock_vfs, _icb, _create_table, _amber_schema, _doc, schema
    ):
        mock_vfs.VFS_FILE_URI_SCHEME = "vfs"
        # CONSOLE_MESSAGES has no namespace mapping in the Python factory.
        mock_vfs.decode_uri.side_effect = _decode_returning(
            VFSResourceType.CONSOLE_MESSAGES
        )

        with pytest.raises(ValueError, match="not supported"):
            DocumentFactory.create_document(VFS_URI, schema)


def test_create_document_rejects_non_vfs_scheme(schema):
    with pytest.raises(NotImplementedError, match="Unsupported URI scheme"):
        DocumentFactory.create_document("file:///tmp/x", schema)


@patch("core.storage.document_factory.IcebergDocument")
@patch("core.storage.document_factory.Schema")
@patch("core.storage.document_factory.load_table_metadata")
@patch("core.storage.document_factory.IcebergCatalogInstance")
@patch("core.storage.document_factory.VFSURIFactory")
class TestOpenDocumentNamespaceRouting:
    @staticmethod
    def _stub_table():
        table = MagicMock()
        table.schema.return_value.as_arrow.return_value = MagicMock()
        return table

    def test_state_resource_type_uses_state_namespace(
        self, mock_vfs, _icb, mock_load, _schema_cls, _doc
    ):
        mock_vfs.VFS_FILE_URI_SCHEME = "vfs"
        mock_vfs.decode_uri.side_effect = _decode_returning(VFSResourceType.STATE)
        mock_load.return_value = self._stub_table()

        DocumentFactory.open_document(VFS_URI)

        args, _ = mock_load.call_args
        assert args[1] == StorageConfig.ICEBERG_TABLE_STATE_NAMESPACE

    def test_unsupported_resource_type_raises_value_error(
        self, mock_vfs, _icb, _load, _schema_cls, _doc
    ):
        mock_vfs.VFS_FILE_URI_SCHEME = "vfs"
        mock_vfs.decode_uri.side_effect = _decode_returning(
            VFSResourceType.CONSOLE_MESSAGES
        )

        with pytest.raises(ValueError, match="not supported"):
            DocumentFactory.open_document(VFS_URI)

    def test_missing_table_raises_value_error(
        self, mock_vfs, _icb, mock_load, _schema_cls, _doc
    ):
        mock_vfs.VFS_FILE_URI_SCHEME = "vfs"
        mock_vfs.decode_uri.side_effect = _decode_returning(VFSResourceType.STATE)
        mock_load.return_value = None

        with pytest.raises(ValueError, match="No storage is found"):
            DocumentFactory.open_document(VFS_URI)


@patch("core.storage.document_factory.IcebergCatalogInstance")
@patch("core.storage.document_factory.VFSURIFactory")
class TestDocumentExists:
    def test_returns_true_when_table_exists(self, mock_vfs, mock_icb):
        mock_vfs.VFS_FILE_URI_SCHEME = "vfs"
        mock_vfs.decode_uri.side_effect = _decode_returning(VFSResourceType.RESULT)
        catalog = MagicMock()
        catalog.table_exists.return_value = True
        mock_icb.get_instance.return_value = catalog

        assert DocumentFactory.document_exists(VFS_URI) is True
        identifier = catalog.table_exists.call_args.args[0]
        assert identifier.startswith(f"{StorageConfig.ICEBERG_TABLE_RESULT_NAMESPACE}.")

    def test_returns_false_when_table_missing(self, mock_vfs, mock_icb):
        mock_vfs.VFS_FILE_URI_SCHEME = "vfs"
        mock_vfs.decode_uri.side_effect = _decode_returning(VFSResourceType.RESULT)
        catalog = MagicMock()
        catalog.table_exists.return_value = False
        mock_icb.get_instance.return_value = catalog

        assert DocumentFactory.document_exists(VFS_URI) is False

    def test_unsupported_resource_type_raises_value_error(self, mock_vfs, _icb):
        mock_vfs.VFS_FILE_URI_SCHEME = "vfs"
        mock_vfs.decode_uri.side_effect = _decode_returning(
            VFSResourceType.CONSOLE_MESSAGES
        )

        with pytest.raises(ValueError, match="not supported"):
            DocumentFactory.document_exists(VFS_URI)


def test_document_exists_rejects_non_vfs_scheme():
    with pytest.raises(NotImplementedError, match="Unsupported URI scheme"):
        DocumentFactory.document_exists("file:///tmp/x")
