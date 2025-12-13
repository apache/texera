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

"""
Utilities for converting between AttributeTypes and Arrow field types,
handling LARGE_BINARY metadata preservation.
"""

import pyarrow as pa

from core.models.schema.attribute_type import (
    AttributeType,
    FROM_ARROW_MAPPING,
    TO_ARROW_MAPPING,
)

# Metadata key used to mark LARGE_BINARY fields in Arrow schemas
TEXERA_TYPE_METADATA_KEY = b"texera_type"
LARGE_BINARY_METADATA_VALUE = b"LARGE_BINARY"


def detect_attribute_type_from_arrow_field(field: pa.Field) -> AttributeType:
    """
    Detects the AttributeType from an Arrow field, checking metadata for LARGE_BINARY.

    :param field: PyArrow field that may contain metadata
    :return: The detected AttributeType
    """
    # Check metadata for LARGE_BINARY type
    # (can be stored by either Scala ArrowUtils or Python)
    is_large_binary = (
        field.metadata
        and field.metadata.get(TEXERA_TYPE_METADATA_KEY) == LARGE_BINARY_METADATA_VALUE
    )

    if is_large_binary:
        return AttributeType.LARGE_BINARY
    else:
        return FROM_ARROW_MAPPING[field.type.id]


def create_arrow_field_with_metadata(
    attr_name: str, attr_type: AttributeType
) -> pa.Field:
    """
    Creates a PyArrow field with appropriate metadata for the given AttributeType.

    :param attr_name: Name of the attribute
    :param attr_type: The AttributeType
    :return: PyArrow field with metadata if needed
    """
    metadata = (
        {TEXERA_TYPE_METADATA_KEY: LARGE_BINARY_METADATA_VALUE}
        if attr_type == AttributeType.LARGE_BINARY
        else None
    )

    return pa.field(attr_name, TO_ARROW_MAPPING[attr_type], metadata=metadata)
