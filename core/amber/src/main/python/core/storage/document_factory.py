from urllib.parse import urlparse

from typing import Optional

from core.models import Schema, Tuple
from core.storage.iceberg.iceberg_catalog_instance import IcebergCatalogInstance
from core.storage.iceberg.iceberg_document import IcebergDocument
from core.storage.iceberg.iceberg_utils import (
    create_table,
    amber_tuples_to_arrow_table,
    arrow_table_to_amber_tuples,
    load_table_metadata,
)
from core.storage.model.virtual_document import VirtualDocument
from core.storage.storage_config import StorageConfig
from core.storage.vfs_uri_factory import VFSURIFactory, VFSResourceType


class DocumentFactory:
    """
    Factory class to create and open documents.
    Currently only iceberg documents are supported.
    """

    ICEBERG = "iceberg"

    @staticmethod
    def sanitize_uri_path(uri):
        return uri.path.lstrip("/").replace("/", "_")

    @staticmethod
    def create_document(uri: str, schema: Schema) -> VirtualDocument:
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme == VFSURIFactory.VFS_FILE_URI_SCHEME:
            _, _, _, _, resource_type = VFSURIFactory.decode_uri(uri)

            if resource_type in {
                VFSResourceType.RESULT,
                VFSResourceType.MATERIALIZED_RESULT,
            }:
                storage_key = DocumentFactory.sanitize_uri_path(parsed_uri)

                iceberg_schema = Schema.as_arrow_schema(schema)

                create_table(
                    IcebergCatalogInstance.get_instance(),
                    StorageConfig.ICEBERG_TABLE_RESULT_NAMESPACE,
                    storage_key,
                    iceberg_schema,
                    override_if_exists=True,
                )

                return IcebergDocument[Tuple](
                    StorageConfig.ICEBERG_TABLE_RESULT_NAMESPACE,
                    storage_key,
                    iceberg_schema,
                    amber_tuples_to_arrow_table,
                    arrow_table_to_amber_tuples,
                )
            else:
                raise ValueError(f"Resource type {resource_type} is not supported")
        else:
            raise NotImplementedError(
                f"Unsupported URI scheme: {parsed_uri.scheme} for creating the document"
            )

    @staticmethod
    def open_document(uri: str) -> (VirtualDocument, Optional[Schema]):
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme == "vfs":
            _, _, _, _, resource_type = VFSURIFactory.decode_uri(uri)

            if resource_type in {
                VFSResourceType.RESULT,
                VFSResourceType.MATERIALIZED_RESULT,
            }:
                storage_key = DocumentFactory.sanitize_uri_path(parsed_uri)

                table = load_table_metadata(
                    IcebergCatalogInstance.get_instance(),
                    StorageConfig.ICEBERG_TABLE_RESULT_NAMESPACE,
                    storage_key,
                )

                if table is None:
                    raise ValueError("No storage is found for the given URI")

                amber_schema = Schema(table.schema().as_arrow())

                document = IcebergDocument(
                    StorageConfig.ICEBERG_TABLE_RESULT_NAMESPACE,
                    storage_key,
                    table.schema(),
                    amber_tuples_to_arrow_table,
                    arrow_table_to_amber_tuples,
                )
                return document, amber_schema
            else:
                raise ValueError(f"Resource type {resource_type} is not supported")
        else:
            raise NotImplementedError(
                f"Unsupported URI scheme: {parsed_uri.scheme} for opening the document"
            )
