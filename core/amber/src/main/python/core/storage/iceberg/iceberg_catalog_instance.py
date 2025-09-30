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

from core.storage.iceberg.iceberg_utils import create_postgres_catalog
from core.storage.storage_config import StorageConfig


class IcebergCatalogInstance:
    """
    IcebergCatalogInstance is a singleton that manages the Iceberg catalog instance.
    Currently only postgres SQL catalog is supported.
    - Provides a single shared catalog for all Iceberg table-related operations.
    - Lazily initializes the catalog on first access.
    - Supports replacing the catalog instance for testing or reconfiguration.
    """

    _instance: Optional[Catalog] = None

    @classmethod
    def get_instance(cls):
        """
        Retrieves the singleton Iceberg catalog instance.
        - If the catalog is not initialized, it is lazily created using the configured
        properties.
        :return: the Iceberg catalog instance.
        """
        if cls._instance is None:
            cls._instance = create_postgres_catalog(
                "texera_iceberg",
                StorageConfig.ICEBERG_FILE_STORAGE_DIRECTORY_PATH,
                StorageConfig.ICEBERG_POSTGRES_CATALOG_URI_WITHOUT_SCHEME,
                StorageConfig.ICEBERG_POSTGRES_CATALOG_USERNAME,
                StorageConfig.ICEBERG_POSTGRES_CATALOG_PASSWORD,
            )
        return cls._instance

    @classmethod
    def replace_instance(cls, catalog: Catalog):
        """
        Replaces the existing Iceberg catalog instance.
        - This method is useful for testing or dynamically updating the catalog.
        :param catalog: the new Iceberg catalog instance to replace the current one.
        """
        cls._instance = catalog
