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
from pyiceberg import types as iceberg_types

import core
import core.models
from core.models import ArrowTableTupleProvider, Tuple
from core.models.schema.attribute_type import AttributeType, TO_ARROW_MAPPING

# Suffix used to encode LARGE_BINARY fields in Iceberg (must match Scala IcebergUtil)
LARGE_BINARY_FIELD_SUFFIX = "__texera_large_binary_ptr"

# Type mappings
_ICEBERG_TO_AMBER_TYPE_MAPPING = {
    "string": "STRING",
    "int": "INT",
    "integer": "INT",
    "long": "LONG",
    "double": "DOUBLE",
    "float": "DOUBLE",
    "boolean": "BOOL",
    "timestamp": "TIMESTAMP",
    "binary": "BINARY",
}

_AMBER_TO_ICEBERG_TYPE_MAPPING = {
    AttributeType.STRING: iceberg_types.StringType(),
    AttributeType.INT: iceberg_types.IntegerType(),
    AttributeType.LONG: iceberg_types.LongType(),
    AttributeType.DOUBLE: iceberg_types.DoubleType(),
    AttributeType.BOOL: iceberg_types.BooleanType(),
    AttributeType.TIMESTAMP: iceberg_types.TimestampType(),
    AttributeType.BINARY: iceberg_types.BinaryType(),
    AttributeType.LARGE_BINARY: iceberg_types.StringType(),
}


def encode_large_binary_field_name(field_name: str, attr_type) -> str:
    """Encodes LARGE_BINARY field names with suffix for Iceberg storage."""
    if attr_type == AttributeType.LARGE_BINARY:
        return f"{field_name}{LARGE_BINARY_FIELD_SUFFIX}"
    return field_name


def decode_large_binary_field_name(field_name: str) -> str:
    """Decodes field names by removing LARGE_BINARY suffix if present."""
    if field_name.endswith(LARGE_BINARY_FIELD_SUFFIX):
        return field_name[: -len(LARGE_BINARY_FIELD_SUFFIX)]
    return field_name


def iceberg_schema_to_amber_schema(iceberg_schema: Schema):
    """
    Converts PyIceberg Schema to Amber Schema.
    Decodes LARGE_BINARY field names and adds Arrow metadata.
    """
    arrow_fields = []
    for field in iceberg_schema.fields:
        decoded_name = decode_large_binary_field_name(field.name)
        is_large_binary = field.name != decoded_name

        if is_large_binary:
            attr_type = AttributeType.LARGE_BINARY
        else:
            iceberg_type_str = str(field.field_type).lower()
            attr_type_name = _ICEBERG_TO_AMBER_TYPE_MAPPING.get(
                iceberg_type_str, "STRING"
            )
            attr_type = getattr(AttributeType, attr_type_name)

        arrow_fields.append(
            pa.field(
                decoded_name,
                TO_ARROW_MAPPING[attr_type],
                metadata={b"texera_type": b"LARGE_BINARY"} if is_large_binary else None,
            )
        )

    return core.models.Schema(pa.schema(arrow_fields))


def amber_schema_to_iceberg_schema(amber_schema) -> Schema:
    """
    Converts Amber Schema to PyIceberg Schema.
    Encodes LARGE_BINARY field names with suffix.
    """
    fields = [
        iceberg_types.NestedField(
            field_id=idx,
            name=encode_large_binary_field_name(field_name, attr_type),
            field_type=_AMBER_TO_ICEBERG_TYPE_MAPPING[attr_type],
            required=False,
        )
        for idx, (field_name, attr_type) in enumerate(
            amber_schema._name_type_mapping.items(), start=1
        )
    ]

    return Schema(*fields)


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
    Handles LARGE_BINARY field name encoding and serialization.
    """
    from core.models.type.large_binary import largebinary

    tuple_list = list(tuple_list)  # Convert to list to allow multiple iterations
    data_dict = {}
    for encoded_name in iceberg_schema.as_arrow().names:
        decoded_name = decode_large_binary_field_name(encoded_name)
        data_dict[encoded_name] = [
            (
                t[decoded_name].uri
                if isinstance(t[decoded_name], largebinary)
                else t[decoded_name]
            )
            for t in tuple_list
        ]

    return pa.Table.from_pydict(data_dict, schema=iceberg_schema.as_arrow())


def arrow_table_to_amber_tuples(
    iceberg_schema: Schema, arrow_table: pa.Table
) -> Iterable[Tuple]:
    """
    Converts an arrow table read from Iceberg to Amber tuples.
    Properly handles LARGE_BINARY field name decoding and type detection.
    """
    amber_schema = iceberg_schema_to_amber_schema(iceberg_schema)
    arrow_table_with_metadata = pa.Table.from_arrays(
        [arrow_table.column(name) for name in arrow_table.column_names],
        schema=amber_schema.as_arrow_schema(),
    )

    tuple_provider = ArrowTableTupleProvider(arrow_table_with_metadata)
    return (
        Tuple(
            {
                decode_large_binary_field_name(name): field_accessor
                for name in arrow_table.column_names
            },
            schema=amber_schema,
        )
        for field_accessor in tuple_provider
    )
