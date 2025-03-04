import pyarrow as pa
import pyiceberg.table
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from typing import Optional, Iterable

import core
from core.models import ArrowTableTupleProvider, Tuple


def create_postgres_catalog(
    catalog_name: str,
    warehouse_path: str,
    uri_without_scheme: str,
    username: str,
    password: str,
) -> SqlCatalog:
    """
    Creates a Postgres SQL catalog instance by connecting to the database named
    "texera_iceberg_catalog".
    - The only requirement of the database is that it already exists. Once pyiceberg
    can connect to the database, it will handle the initializations.
    :param catalog_name: the name of the catalog.
    :param warehouse_path: the root path for the warehouse where the tables are stored.
    :param uri_without_scheme: the uri of the postgres database but without
            the scheme prefix since java and python use different schemes.
    :param username: the username of the postgres database.
    :param password: the password of the postgres database.
    :return: a SQLCatalog instance.
    """
    return SqlCatalog(
        catalog_name,
        **{
            "uri": f"postgresql+pg8000://{username}:{password}@{uri_without_scheme}",
            "warehouse": f"file://{warehouse_path}",
        },
    )


def create_table(
    catalog: Catalog,
    table_namespace: str,
    table_name: str,
    table_schema: Schema,
    override_if_exists: bool = False,
) -> Table:
    """
    Creates a new Iceberg table with the specified schema and properties.
    - Drops the existing table if `override_if_exists` is true and the table already
    exists.
    - Creates an unpartitioned table with custom commit retry properties.

    :param catalog: The Iceberg catalog to manage the table.
    :param table_namespace: The namespace of the table.
    :param table_name: The name of the table.
    :param table_schema: The schema of the table.
    :param override_if_exists: Whether to drop and recreate the table if it exists.
    :return: The created Iceberg table.
    """

    identifier = f"{table_namespace}.{table_name}"

    catalog.create_namespace_if_not_exists(table_namespace)

    if catalog.table_exists(identifier) and override_if_exists:
        catalog.drop_table(identifier)

    table = catalog.create_table(
        identifier=identifier,
        schema=table_schema,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
    )

    return table


def load_table_metadata(
    catalog: Catalog, table_namespace: str, table_name: str
) -> Optional[Table]:
    """
    Loads metadata for an existing Iceberg table.
    - Returns the table if it exists and is successfully loaded.
    - Returns None if the table does not exist or cannot be loaded.

    :param catalog: The Iceberg catalog to load the table from.
    :param table_namespace: The namespace of the table.
    :param table_name: The name of the table.
    :return: The table if found, or None if not found.
    """
    identifier = f"{table_namespace}.{table_name}"
    try:
        return catalog.load_table(identifier)
    except Exception:
        return None


def read_data_file_as_arrow_table(
    planfile: pyiceberg.table.FileScanTask, iceberg_table: pyiceberg.table.Table
) -> pa.Table:
    """Reads a data file as a pyarrow table and returns an iterator over its records."""
    arrow_table: pa.Table = ArrowScan(
        iceberg_table.metadata,
        iceberg_table.io,
        iceberg_table.schema(),
        AlwaysTrue(),
        True,
    ).to_table([planfile])
    return arrow_table


def amber_tuples_to_arrow_table(
    iceberg_schema: Schema, tuple_list: Iterable[Tuple]
) -> pa.Table:
    """
    Converts a list of amber tuples to a pyarrow table for serialization.
    """
    return pa.Table.from_pydict(
        {
            name: [t[name] for t in tuple_list]
            for name in iceberg_schema.as_arrow().names
        },
        schema=iceberg_schema.as_arrow(),
    )


def arrow_table_to_amber_tuples(
    iceberg_schema: Schema, arrow_table: pa.Table
) -> Iterable[Tuple]:
    """
    Converts an arrow table to a list of amber tuples for deserialization.
    """
    tuple_provider = ArrowTableTupleProvider(arrow_table)
    return (
        Tuple(
            {name: field_accessor for name in arrow_table.column_names},
            schema=core.models.Schema(iceberg_schema.as_arrow()),
        )
        for field_accessor in tuple_provider
    )
