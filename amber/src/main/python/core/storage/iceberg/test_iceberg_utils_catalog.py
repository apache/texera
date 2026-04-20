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
