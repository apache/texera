from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from typing import Optional

# replace with actual path
warehouse_path = "../../../../../core/amber/user-resources/workflow-results"


class IcebergCatalogInstance:
    """
    IcebergCatalogInstance is a singleton that manages the Iceberg catalog instance.
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
            cls._instance = SqlCatalog(
                "texera_iceberg",
                **{
                    "uri": "postgresql+psycopg2://testdb:testuser@localhost/"
                    "testpassword",
                    "warehouse": f"file://{warehouse_path}",
                    "init_catalog_tables": "true",
                },
            )
            # cls._instance = RestCatalog(
            #     "texera_iceberg",
            #     **{
            #         "uri": "postgresql+psycopg2://iceberg_py_test:test@localhost/test_
            #         iceberg_catalog_jan16",
            #         "warehouse": f"file://{warehouse_path}",
            #         "init_catalog_tables": "true"
            #     }
            # )
        return cls._instance

    @classmethod
    def replace_instance(cls, catalog: Catalog):
        """
        Replaces the existing Iceberg catalog instance.
        - This method is useful for testing or dynamically updating the catalog.
        :param catalog: the new Iceberg catalog instance to replace the current one.
        """
        cls._instance = catalog
