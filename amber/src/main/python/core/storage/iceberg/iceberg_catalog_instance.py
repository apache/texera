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

from pyiceberg.catalog import Catalog
from typing import Optional

from core.storage.iceberg.iceberg_utils import (
    create_postgres_catalog,
    create_rest_catalog,
)
from core.storage.storage_config import StorageConfig


class IcebergCatalogInstance:
    """
    Manages Iceberg catalog instances, cached per warehouse.
    - REST catalogs are keyed by warehouse so one process can read/write tables in
      many warehouses (Design 2, warehouse-per-execution).
    - The postgres catalog has no warehouse concept and shares a single entry.
    - Catalogs are lazily created on first access; entries can be replaced for
      testing or reconfiguration.
    """

    _catalogs: dict = {}
    _POSTGRES_KEY = "__postgres__"

    @classmethod
    def get_instance(cls, warehouse: Optional[str] = None) -> Catalog:
        """
        Retrieves the Iceberg catalog for the given warehouse, creating and caching
        it on first use. For REST catalogs, `warehouse` selects which warehouse's
        catalog to use (defaults to the configured warehouse when None). For the
        postgres catalog, `warehouse` is ignored.
        :param warehouse: the warehouse name (REST only); None uses the default.
        :return: the Iceberg catalog instance.
        """
        catalog_type = StorageConfig.ICEBERG_CATALOG_TYPE
        if catalog_type == "postgres":
            if cls._POSTGRES_KEY not in cls._catalogs:
                cls._catalogs[cls._POSTGRES_KEY] = create_postgres_catalog(
                    "texera_iceberg",
                    StorageConfig.ICEBERG_FILE_STORAGE_DIRECTORY_PATH,
                    StorageConfig.ICEBERG_POSTGRES_CATALOG_URI_WITHOUT_SCHEME,
                    StorageConfig.ICEBERG_POSTGRES_CATALOG_USERNAME,
                    StorageConfig.ICEBERG_POSTGRES_CATALOG_PASSWORD,
                )
            return cls._catalogs[cls._POSTGRES_KEY]
        elif catalog_type == "rest":
            key = warehouse or StorageConfig.ICEBERG_REST_CATALOG_WAREHOUSE_NAME
            if key not in cls._catalogs:
                cls._catalogs[key] = create_rest_catalog(
                    "texera_iceberg",
                    key,
                    StorageConfig.ICEBERG_REST_CATALOG_URI,
                )
            return cls._catalogs[key]
        else:
            raise ValueError(f"Unsupported catalog type: {catalog_type}")

    @classmethod
    def replace_instance(cls, catalog: Catalog, warehouse: Optional[str] = None):
        """
        Replaces the cached catalog for a warehouse (testing or reconfiguration).
        :param catalog: the new Iceberg catalog instance.
        :param warehouse: the warehouse to replace (REST only); None uses default.
        """
        if StorageConfig.ICEBERG_CATALOG_TYPE == "postgres":
            key = cls._POSTGRES_KEY
        else:
            key = warehouse or StorageConfig.ICEBERG_REST_CATALOG_WAREHOUSE_NAME
        cls._catalogs[key] = catalog
