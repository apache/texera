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

import datetime
import pyarrow as pa
from bidict import bidict
from enum import Enum
from pyarrow import lib
from core.models.type.large_binary import largebinary


class AttributeType(Enum):
    """
    Types supported by PyTexera & PyAmber.

    The definitions are mapped and following the AttributeType.java
    (src/main/scala/org/apache/texera/workflow/common/tuple/schema/AttributeType.java)
    """

    STRING = 1
    INT = 2
    LONG = 3
    BOOL = 4
    DOUBLE = 5
    TIMESTAMP = 6
    BINARY = 7
    LARGE_BINARY = 8


RAW_TYPE_MAPPING = bidict(
    {
        "STRING": AttributeType.STRING,
        "INTEGER": AttributeType.INT,
        "LONG": AttributeType.LONG,
        "DOUBLE": AttributeType.DOUBLE,
        "BOOLEAN": AttributeType.BOOL,
        "TIMESTAMP": AttributeType.TIMESTAMP,
        "BINARY": AttributeType.BINARY,
        "LARGE_BINARY": AttributeType.LARGE_BINARY,
    }
)

TO_ARROW_MAPPING = {
    AttributeType.INT: pa.int32(),
    AttributeType.LONG: pa.int64(),
    AttributeType.STRING: pa.string(),
    AttributeType.DOUBLE: pa.float64(),
    AttributeType.BOOL: pa.bool_(),
    AttributeType.BINARY: pa.binary(),
    AttributeType.TIMESTAMP: pa.timestamp("us"),
    AttributeType.LARGE_BINARY: pa.string(),  # Serialized as URI string
}

FROM_ARROW_MAPPING = {
    lib.Type_INT32: AttributeType.INT,
    lib.Type_INT64: AttributeType.LONG,
    lib.Type_STRING: AttributeType.STRING,
    lib.Type_LARGE_STRING: AttributeType.STRING,
    lib.Type_DOUBLE: AttributeType.DOUBLE,
    lib.Type_BOOL: AttributeType.BOOL,
    lib.Type_BINARY: AttributeType.BINARY,
    lib.Type_LARGE_BINARY: AttributeType.BINARY,
    lib.Type_TIMESTAMP: AttributeType.TIMESTAMP,
}


# Only single-directional mapping.
TO_PYOBJECT_MAPPING = {
    AttributeType.STRING: str,
    AttributeType.INT: int,
    AttributeType.LONG: int,  # Python3 unifies long into int.
    AttributeType.DOUBLE: float,
    AttributeType.BOOL: bool,
    AttributeType.BINARY: bytes,
    AttributeType.TIMESTAMP: datetime.datetime,
    AttributeType.LARGE_BINARY: largebinary,
}

FROM_PYOBJECT_MAPPING = {
    str: AttributeType.STRING,
    int: AttributeType.INT,
    float: AttributeType.DOUBLE,
    bool: AttributeType.BOOL,
    bytes: AttributeType.BINARY,
    datetime.datetime: AttributeType.TIMESTAMP,
    largebinary: AttributeType.LARGE_BINARY,
}
