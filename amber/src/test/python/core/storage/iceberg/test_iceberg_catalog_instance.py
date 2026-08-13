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

import pytest

from core.storage.iceberg import iceberg_catalog_instance
from core.storage.iceberg.iceberg_catalog_instance import IcebergCatalogInstance
from core.storage.storage_config import StorageConfig


@pytest.fixture(autouse=True)
def _isolated_catalog_cache():
    """Save/clear/restore the class-level cache so these tests neither see nor
    leak cached catalogs (mirrors test_state_materialization_e2e.py)."""
    original = dict(IcebergCatalogInstance._catalogs)
    IcebergCatalogInstance._catalogs.clear()
    yield
    IcebergCatalogInstance._catalogs.clear()
    IcebergCatalogInstance._catalogs.update(original)


def _rest_config():
    """StorageConfig patched to the `rest` catalog type. No live Lakekeeper is
    involved anywhere in this file: `create_rest_catalog` is patched out, which
    is exactly the boundary the cache under test sits on."""
    return (
        patch.object(StorageConfig, "ICEBERG_CATALOG_TYPE", "rest"),
        patch.object(
            StorageConfig, "ICEBERG_REST_CATALOG_WAREHOUSE_NAME", "default-wh"
        ),
        patch.object(
            StorageConfig, "ICEBERG_REST_CATALOG_URI", "http://localhost:8181"
        ),
    )


class TestRestCatalogCachedPerWarehouse:
    """
    Under the `rest` catalog type the cache is keyed by warehouse name -- the
    core of the per-warehouse catalog cache (#6870 Phase 0): one process can
    hold several REST catalogs, one per warehouse it touches, while repeated
    lookups for the same warehouse share one client.
    """

    def test_caches_one_catalog_per_warehouse(self):
        type_p, default_p, uri_p = _rest_config()
        with (
            type_p,
            default_p,
            uri_p,
            patch.object(
                iceberg_catalog_instance,
                "create_rest_catalog",
                side_effect=lambda *args: object(),
            ) as mock_create,
        ):
            first = IcebergCatalogInstance.get_instance("wh-a")
            again = IcebergCatalogInstance.get_instance("wh-a")
            other = IcebergCatalogInstance.get_instance("wh-b")

        assert first is again, "same warehouse must reuse the cached catalog"
        assert first is not other, "distinct warehouses must get distinct catalogs"
        assert mock_create.call_count == 2
        # The warehouse name is what create_rest_catalog is keyed/called with.
        assert [call.args[1] for call in mock_create.call_args_list] == ["wh-a", "wh-b"]

    def test_none_falls_back_to_the_configured_default_warehouse(self):
        type_p, default_p, uri_p = _rest_config()
        with (
            type_p,
            default_p,
            uri_p,
            patch.object(
                iceberg_catalog_instance,
                "create_rest_catalog",
                side_effect=lambda *args: object(),
            ) as mock_create,
        ):
            from_none = IcebergCatalogInstance.get_instance(None)
            from_name = IcebergCatalogInstance.get_instance("default-wh")

        assert from_none is from_name, "None must share the default warehouse's entry"
        assert mock_create.call_count == 1
        assert mock_create.call_args.args[1] == "default-wh"

    def test_replace_instance_keys_by_warehouse(self):
        sentinel = object()
        type_p, default_p, uri_p = _rest_config()
        with (
            type_p,
            default_p,
            uri_p,
            patch.object(
                iceberg_catalog_instance,
                "create_rest_catalog",
                side_effect=AssertionError(
                    "cache was pre-seeded; nothing should be created"
                ),
            ),
        ):
            IcebergCatalogInstance.replace_instance(sentinel, "wh-a")
            assert IcebergCatalogInstance.get_instance("wh-a") is sentinel


class TestWarehouseAgnosticCatalogTypes:
    """The postgres catalog has no warehouse concept: every caller shares the
    one entry under the constant key, mirroring the Scala `SharedCatalogKey`."""

    def test_postgres_catalog_is_created_once_and_shared(self):
        with (
            patch.object(StorageConfig, "ICEBERG_CATALOG_TYPE", "postgres"),
            patch.object(
                iceberg_catalog_instance,
                "create_postgres_catalog",
                side_effect=lambda *args: object(),
            ) as mock_create,
        ):
            first = IcebergCatalogInstance.get_instance()
            again = IcebergCatalogInstance.get_instance("ignored-warehouse")

        assert first is again, "postgres ignores the warehouse and shares one entry"
        assert mock_create.call_count == 1

    def test_replace_instance_uses_the_shared_postgres_key(self):
        sentinel = object()
        with patch.object(StorageConfig, "ICEBERG_CATALOG_TYPE", "postgres"):
            IcebergCatalogInstance.replace_instance(sentinel)
            assert IcebergCatalogInstance.get_instance() is sentinel
            assert IcebergCatalogInstance._catalogs == {
                IcebergCatalogInstance._POSTGRES_KEY: sentinel
            }

    def test_unsupported_catalog_type_raises(self):
        with patch.object(StorageConfig, "ICEBERG_CATALOG_TYPE", "bogus"):
            with pytest.raises(ValueError, match="Unsupported catalog type: bogus"):
                IcebergCatalogInstance.get_instance()
