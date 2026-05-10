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

import uuid

import pytest
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType

from core.storage.iceberg.iceberg_utils import create_rest_catalog

pytestmark = pytest.mark.integration


@pytest.fixture
def rest_catalog():
    return create_rest_catalog(
        catalog_name="rest_integration_test",
        warehouse_name="texera",
        rest_uri="http://localhost:8181/catalog/",
        s3_endpoint="http://localhost:9000",
        s3_region="us-west-2",
        s3_username="texera_minio",
        s3_password="password",
    )


def test_rest_catalog_round_trip(rest_catalog):
    """Round-trip table metadata via the REST catalog (Lakekeeper)."""
    namespace = "rest_integration_test_ns"
    table_name = f"rest_test_{uuid.uuid4().hex}"
    identifier = f"{namespace}.{table_name}"

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )

    rest_catalog.create_namespace_if_not_exists(namespace)
    if rest_catalog.table_exists(identifier):
        rest_catalog.drop_table(identifier)

    # create — exercises REST createTable.
    rest_catalog.create_table(identifier=identifier, schema=schema)
    assert rest_catalog.table_exists(identifier)

    # load — exercises REST loadTable (metadata fetch).
    loaded = rest_catalog.load_table(identifier)
    assert len(loaded.schema().fields) == 2

    # drop — exercises REST dropTable.
    rest_catalog.drop_table(identifier)
    assert not rest_catalog.table_exists(identifier)
    with pytest.raises(NoSuchTableError):
        rest_catalog.load_table(identifier)
