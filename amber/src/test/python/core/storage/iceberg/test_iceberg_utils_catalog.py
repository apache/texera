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

from unittest.mock import patch

from core.storage.iceberg import iceberg_utils
from core.storage.iceberg.iceberg_utils import create_postgres_catalog


class TestCreatePostgresCatalog:
    """
    Regression tests for `create_postgres_catalog`.

    The Scala side (`IcebergUtil.createPostgresCatalog`) initializes the JDBC
    catalog with a plain filesystem warehouse path (no URI scheme). PyIceberg
    persists the `warehouse` property into table metadata, so if the Python
    side registers the catalog with a `file://`-prefixed value, Iceberg tables
    written from Python UDFs become unreadable from the Scala/Java engine
    (and vice versa). These tests pin the Python side to the same plain-path
    convention used on the Scala side.

    Windows drive-letter warehouses (e.g. ``C:\\...``) are the one documented
    exception -- pyiceberg cannot parse them as-is -- and are covered by
    ``TestCreatePostgresCatalogWindowsLocal`` below.
    """

    def test_warehouse_is_passed_without_file_scheme(self):
        """`warehouse` must be forwarded as-is, without a `file://` prefix."""
        warehouse_path = "/tmp/texera/iceberg-warehouse"

        with patch.object(iceberg_utils, "SqlCatalog") as mock_sql_catalog:
            create_postgres_catalog(
                catalog_name="texera_iceberg",
                warehouse_path=warehouse_path,
                uri_without_scheme="localhost:5432/texera_iceberg_catalog",
                username="texera",
                password="password",
            )

        assert mock_sql_catalog.call_count == 1
        _, kwargs = mock_sql_catalog.call_args
        assert kwargs["warehouse"] == warehouse_path
        assert not kwargs["warehouse"].startswith("file://")

    def test_windows_style_warehouse_is_passed_verbatim(self):
        """
        The Scala side strips the Windows drive colon (e.g. `C:/x` -> `C/x`)
        before registering the catalog so PyArrow can parse the path. The
        Python side should forward whatever it receives verbatim, so the two
        runtimes agree on the warehouse string stored in Iceberg metadata.
        """
        warehouse_path = "C/Users/texera/iceberg-warehouse"

        with patch.object(iceberg_utils, "SqlCatalog") as mock_sql_catalog:
            create_postgres_catalog(
                catalog_name="texera_iceberg",
                warehouse_path=warehouse_path,
                uri_without_scheme="localhost:5432/texera_iceberg_catalog",
                username="texera",
                password="password",
            )

        _, kwargs = mock_sql_catalog.call_args
        assert kwargs["warehouse"] == warehouse_path
        assert "file://" not in kwargs["warehouse"]

    def test_postgres_uri_is_built_with_pg8000_scheme(self):
        """The JDBC URI should be prefixed with `postgresql+pg8000://` and
        include credentials; nothing about that should bleed into `warehouse`.
        """
        warehouse_path = "/var/lib/texera/warehouse"

        with patch.object(iceberg_utils, "SqlCatalog") as mock_sql_catalog:
            create_postgres_catalog(
                catalog_name="texera_iceberg",
                warehouse_path=warehouse_path,
                uri_without_scheme="db.internal:5432/texera_iceberg_catalog",
                username="texera",
                password="s3cret",
            )

        args, kwargs = mock_sql_catalog.call_args
        assert args == ("texera_iceberg",)
        assert kwargs["uri"] == (
            "postgresql+pg8000://texera:s3cret@db.internal:5432/texera_iceberg_catalog"
        )
        # And warehouse is still the plain path.
        assert kwargs["warehouse"] == warehouse_path


# Reference the production constant (the fsspec FileIO selected for Windows-local
# warehouses) rather than duplicating its literal, so the tests cannot drift from it.
_FSSPEC_FILE_IO = iceberg_utils._FSSPEC_FILE_IO


class TestCreatePostgresCatalogWindowsLocal:
    """
    Windows-local warehouse handling in `create_postgres_catalog`.

    This is the Python-worker counterpart to the Scala/JVM fix in #6488
    (`WinutilsFreeLocalFileSystem`, issue #6487). The Python UDF worker writes
    its output-port Iceberg storage through its own pyiceberg path, which #6488
    does not touch. On a Windows dev machine with a local-filesystem warehouse
    (`postgres` catalog type), a bare drive path fails two ways:

      1. `C:\\...` is mis-parsed by pyiceberg as URI scheme ``c`` -> "Unrecognized
         filesystem type in URI: c" when a table is created.
      2. Even a `file:///C:/...` URI is rejected by pyiceberg's default pyarrow
         FileIO ("/C:/..." -> WinError 123).

    The fix normalizes a drive path to a `file:///` URI and selects
    `FsspecFileIO`, whose `LocalFileSystem` handles Windows drive paths. It is
    gated to warehouses that actually carry a Windows drive letter, so POSIX
    paths, colon-stripped paths, and remote object stores (`s3://...`) keep the
    default pyarrow FileIO / plain-path convention (see the invariant pinned by
    `TestCreatePostgresCatalog`).
    """

    def _make(self, warehouse_path):
        with patch.object(iceberg_utils, "SqlCatalog") as mock_sql_catalog:
            create_postgres_catalog(
                catalog_name="texera_iceberg",
                warehouse_path=warehouse_path,
                uri_without_scheme="localhost:5432/texera_iceberg_catalog",
                username="texera",
                password="password",
            )
        assert mock_sql_catalog.call_count == 1
        _, kwargs = mock_sql_catalog.call_args
        return kwargs

    def test_backslash_drive_path_is_normalized_and_uses_fsspec(self):
        """
        A bare Windows drive path (as produced by Java `Path.toString`, using
        backslashes) is normalized to a `file:///` URI and `FsspecFileIO` is
        selected.
        """
        kwargs = self._make("C:\\Users\\texera\\amber\\workflow-results")

        assert kwargs["warehouse"] == "file:///C:/Users/texera/amber/workflow-results"
        assert kwargs["py-io-impl"] == _FSSPEC_FILE_IO

    def test_forward_slash_drive_path_is_normalized_and_uses_fsspec(self):
        """A forward-slash drive path is normalized the same way."""
        kwargs = self._make("C:/Users/texera/warehouse")

        assert kwargs["warehouse"] == "file:///C:/Users/texera/warehouse"
        assert kwargs["py-io-impl"] == _FSSPEC_FILE_IO

    def test_drive_path_with_space_is_not_percent_encoded(self):
        """
        A warehouse path containing a space (very common on Windows -- e.g.
        ``C:\\Users\\John Doe\\...`` or ``C:\\Program Files\\...``) must be
        normalized with a RAW space, never ``%20``. The fsspec LocalFileSystem
        does not URL-decode, so a percent-encoded path would send writes to a
        literally ``%20``-named directory that the Scala engine never looks in.
        """
        kwargs = self._make("C:\\Users\\John Doe\\amber\\workflow-results")

        assert kwargs["warehouse"] == "file:///C:/Users/John Doe/amber/workflow-results"
        assert "%20" not in kwargs["warehouse"]
        assert kwargs["py-io-impl"] == _FSSPEC_FILE_IO

    def test_normalization_is_os_independent(self):
        """
        The drive-path normalization must be deterministic regardless of the
        host OS (this test also runs on Linux CI), so it relies on
        `PureWindowsPath` rather than the host's `pathlib.Path`.
        """
        kwargs = self._make("D:\\data\\iceberg")

        assert kwargs["warehouse"] == "file:///D:/data/iceberg"
        assert kwargs["py-io-impl"] == _FSSPEC_FILE_IO

    def test_existing_file_uri_with_drive_uses_fsspec_unchanged(self):
        """
        A warehouse already expressed as a `file:///C:/...` URI still needs
        `FsspecFileIO` (pyarrow FileIO rejects it), but must not be re-encoded.
        """
        kwargs = self._make("file:///C:/Users/texera/warehouse")

        assert kwargs["warehouse"] == "file:///C:/Users/texera/warehouse"
        assert kwargs["py-io-impl"] == _FSSPEC_FILE_IO

    def test_uppercase_file_scheme_with_drive_is_detected(self):
        """
        `file` URI schemes are case-insensitive, so `FILE:///C:/...` must be
        detected as Windows-local (and select `FsspecFileIO`) too.
        """
        kwargs = self._make("FILE:///C:/Users/texera/warehouse")

        assert kwargs["warehouse"] == "FILE:///C:/Users/texera/warehouse"
        assert kwargs["py-io-impl"] == _FSSPEC_FILE_IO

    def test_posix_absolute_path_is_untouched(self):
        """
        A POSIX warehouse (Linux/macOS/CI) must keep the plain-path convention
        and the default FileIO -- forcing `file://` here would reintroduce the
        cross-runtime metadata mismatch fixed in #4409.
        """
        kwargs = self._make("/tmp/texera/iceberg-warehouse")

        assert kwargs["warehouse"] == "/tmp/texera/iceberg-warehouse"
        assert "py-io-impl" not in kwargs

    def test_colon_stripped_path_is_untouched(self):
        """
        A colon-stripped path (the form the Scala side registers on Windows,
        `C/...`) is a plain relative path pyiceberg accepts, so it is forwarded
        verbatim with the default FileIO.
        """
        kwargs = self._make("C/Users/texera/iceberg-warehouse")

        assert kwargs["warehouse"] == "C/Users/texera/iceberg-warehouse"
        assert "py-io-impl" not in kwargs

    def test_remote_object_store_is_untouched(self):
        """
        A remote object-store warehouse (`s3://...`) must not be forced onto
        `FsspecFileIO`; pyiceberg selects the appropriate FileIO by scheme.
        """
        kwargs = self._make("s3://texera-bucket/iceberg-warehouse")

        assert kwargs["warehouse"] == "s3://texera-bucket/iceberg-warehouse"
        assert "py-io-impl" not in kwargs

    def test_posix_file_uri_without_drive_is_untouched(self):
        """
        The `file://` detection branch must require a drive letter: a POSIX
        `file:///tmp/...` URI (no drive) is handled fine by the default FileIO
        and must not be forced onto `FsspecFileIO`.
        """
        kwargs = self._make("file:///tmp/texera/iceberg-warehouse")

        assert kwargs["warehouse"] == "file:///tmp/texera/iceberg-warehouse"
        assert "py-io-impl" not in kwargs

    def test_empty_warehouse_is_untouched(self):
        """An empty warehouse is treated as non-Windows-local (default FileIO)."""
        kwargs = self._make("")

        assert kwargs["warehouse"] == ""
        assert "py-io-impl" not in kwargs
