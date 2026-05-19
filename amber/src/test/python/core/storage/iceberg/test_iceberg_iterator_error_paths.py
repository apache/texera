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

from unittest.mock import Mock, patch

import pytest

from core.storage.iceberg import iceberg_document
from core.storage.iceberg.iceberg_document import IcebergIterator


def test_seek_to_usable_file_preserves_original_error():
    failing_table = Mock()
    failing_table.refresh.side_effect = RuntimeError(
        "Catalog auth failure: token expired"
    )

    with patch.object(
        iceberg_document, "load_table_metadata", return_value=failing_table
    ):
        it = IcebergIterator(0, None, None, "ns", "tbl", None, None)
        with pytest.raises(RuntimeError, match="Catalog auth failure"):
            next(it)
