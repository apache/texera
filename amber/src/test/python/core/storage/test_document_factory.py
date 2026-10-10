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
from urllib.parse import urlparse

import pytest

from core.models import Schema
from core.storage.document_factory import DocumentFactory
from core.storage.storage_config import StorageConfig
from core.storage.vfs_uri_factory import VFSResourceType, VFSUriComponents


# Avoid initializing the real config (only initializable once per process).
StorageConfig.ICEBERG_TABLE_RESULT_NAMESPACE = "test-result-ns"
StorageConfig.ICEBERG_TABLE_STATE_NAMESPACE = "test-state-ns"

VFS_URI = "vfs:///wid/0/eid/0/opid/test/main/0/0/result"
# The storage key VFS_URI sanitizes down to; the routing tests below assert
# that it is what actually reaches the iceberg layer.
VFS_URI_STORAGE_KEY = "wid_0_eid_0_opid_test_main_0_0_result"


@pytest.fixture
def schema():
    return Schema(raw_schema={"x": "INTEGER"})


def _decode_returning(resource_type):
    """Helper: build a VFSURIFactory.decode_uri side_effect."""
    return lambda _uri: VFSUriComponents(None, None, None, resource_type)


def test_sanitize_uri_path_unquotes_strips_the_warehouse_and_flattens():
    # Each of the four steps in sanitize_uri_path is load-bearing and none of
    # them is observable through create/open/exists, which only ever assert
    # the namespace -- so pin the storage key directly. The leading slash is
    # stripped, an optional "wh/<warehouse>/" prefix is dropped so the key is
    # warehouse-independent, percent escapes are decoded (urlparse, unlike
    # java.net.URI.getPath, does not do it), and the remaining separators
    # become underscores.
    parsed = urlparse("vfs:///wh/w1/wid/0/my%20op/result/")

    assert DocumentFactory.sanitize_uri_path(parsed) == "wid_0_my op_result_"


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

        args, kwargs = mock_create_table.call_args
        assert args[1] == StorageConfig.ICEBERG_TABLE_RESULT_NAMESPACE
        # A table left over from an earlier execution of the same operator
        # must be overwritten, not silently reused with stale rows.
        assert kwargs["override_if_exists"] is True

    def test_document_is_built_for_the_resolved_namespace_and_storage_key(
        self, mock_vfs, _icb, _create_table, _amber_schema, mock_doc, schema
    ):
        mock_vfs.VFS_FILE_URI_SCHEME = "vfs"
        mock_vfs.decode_uri.side_effect = _decode_returning(VFSResourceType.RESULT)

        document = DocumentFactory.create_document(VFS_URI, schema)

        # IcebergDocument[Tuple](namespace, storage_key, ...): the two leading
        # positional arguments are both plain strings and transposing them
        # would go unnoticed by every namespace-only assertion above.
        constructor = mock_doc.__getitem__.return_value
        assert constructor.call_args.args[:2] == (
            StorageConfig.ICEBERG_TABLE_RESULT_NAMESPACE,
            VFS_URI_STORAGE_KEY,
        )
        assert document is constructor.return_value

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
    # Match the per-site suffix, not the shared "Unsupported URI scheme"
    # prefix: all three entry points raise NotImplementedError with that same
    # leading text, so a bare prefix match cannot tell this raise site from
    # the other two and would still pass if this branch delegated to one of
    # them instead of raising on its own.
    with pytest.raises(NotImplementedError, match="for creating the document"):
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

    def test_returns_the_document_and_the_schema_of_the_loaded_table(
        self, mock_vfs, _icb, mock_load, mock_schema_cls, mock_doc
    ):
        mock_vfs.VFS_FILE_URI_SCHEME = "vfs"
        mock_vfs.decode_uri.side_effect = _decode_returning(VFSResourceType.RESULT)
        mock_load.return_value = self._stub_table()

        document, amber_schema = DocumentFactory.open_document(VFS_URI)

        # Bind the pair: with the result discarded, open_document could
        # return (None, None) -- or transpose the namespace and storage key
        # it hands the document -- with nothing in this file noticing.
        assert mock_doc.call_args.args[:2] == (
            StorageConfig.ICEBERG_TABLE_RESULT_NAMESPACE,
            VFS_URI_STORAGE_KEY,
        )
        assert document is mock_doc.return_value
        assert amber_schema is mock_schema_cls.return_value

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


def test_open_document_rejects_non_vfs_scheme():
    # See test_create_document_rejects_non_vfs_scheme: the suffix is what
    # identifies this raise site.
    with pytest.raises(NotImplementedError, match="for opening the document"):
        DocumentFactory.open_document("file:///tmp/x")


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
    # See test_create_document_rejects_non_vfs_scheme: the suffix is what
    # identifies this raise site.
    with pytest.raises(NotImplementedError, match="for checking document existence"):
        DocumentFactory.document_exists("file:///tmp/x")
