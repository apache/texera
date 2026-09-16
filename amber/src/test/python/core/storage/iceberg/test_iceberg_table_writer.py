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

from unittest.mock import Mock, patch, sentinel

import pyarrow as pa
import pytest
from pyiceberg.exceptions import CommitFailedException

from core.storage.iceberg.iceberg_table_writer import IcebergTableWriter
from core.storage.storage_config import StorageConfig

SCHEMA = pa.schema([("value", pa.int64())])


def make_writer(buffer_size=3, serde=None, table=None):
    """Build a writer backed by a mocked catalog/table and a serde stub.

    The serde captures a *copy* of the buffer at call time because the writer
    passes its live buffer list to serde and clears it in place afterwards.
    """
    catalog = Mock()
    table = table if table is not None else Mock()
    catalog.load_table.return_value = table

    captured = []
    if serde is None:

        def serde(schema, items):
            captured.append((schema, list(items)))
            return sentinel.arrow_table

    with patch.object(StorageConfig, "ICEBERG_TABLE_COMMIT_BATCH_SIZE", buffer_size):
        writer = IcebergTableWriter(
            writer_identifier="writer-1",
            catalog=catalog,
            table_namespace="ns",
            table_name="tbl",
            table_schema=SCHEMA,
            serde=serde,
        )
    return writer, catalog, table, captured


def test_constructor_loads_table_and_reads_buffer_size_from_config():
    writer, catalog, table, _ = make_writer(buffer_size=7)

    catalog.load_table.assert_called_once_with("ns.tbl")
    assert writer.table is table
    assert writer.buffer_size == 7
    assert writer.buffer == []


def test_put_one_below_threshold_does_not_flush():
    writer, _, table, captured = make_writer(buffer_size=3)

    writer.put_one({"value": 1})
    writer.put_one({"value": 2})

    assert writer.buffer == [{"value": 1}, {"value": 2}]
    assert captured == []
    table.append.assert_not_called()
    table.refresh.assert_not_called()


def test_put_one_reaching_threshold_flushes_and_clears_buffer():
    writer, _, table, captured = make_writer(buffer_size=3)

    items = [{"value": 1}, {"value": 2}, {"value": 3}]
    for item in items:
        writer.put_one(item)

    assert captured == [(SCHEMA, items)]
    table.refresh.assert_called_once()
    table.append.assert_called_once_with(sentinel.arrow_table)
    assert writer.buffer == []


def test_buffer_refills_after_flush_without_early_flush():
    writer, _, table, captured = make_writer(buffer_size=2)

    writer.put_one({"value": 1})
    writer.put_one({"value": 2})
    writer.put_one({"value": 3})

    # Only the first two items were flushed; the third starts a fresh buffer.
    assert captured == [(SCHEMA, [{"value": 1}, {"value": 2}])]
    assert table.append.call_count == 1
    assert writer.buffer == [{"value": 3}]


def test_close_flushes_remaining_items():
    writer, _, table, captured = make_writer(buffer_size=10)

    writer.put_one({"value": 1})
    writer.close()

    assert captured == [(SCHEMA, [{"value": 1}])]
    table.append.assert_called_once_with(sentinel.arrow_table)
    assert writer.buffer == []


def test_close_with_empty_buffer_does_nothing():
    writer, _, table, captured = make_writer(buffer_size=10)

    writer.close()

    assert captured == []
    table.append.assert_not_called()


def test_open_clears_dirty_buffer():
    writer, _, _, _ = make_writer(buffer_size=10)

    writer.put_one({"value": 1})
    writer.open()

    assert writer.buffer == []


def test_remove_one_removes_buffered_item():
    writer, _, _, _ = make_writer(buffer_size=10)

    writer.put_one({"value": 1})
    writer.put_one({"value": 2})
    writer.remove_one({"value": 1})

    assert writer.buffer == [{"value": 2}]


def test_remove_one_raises_for_already_flushed_item():
    writer, _, _, _ = make_writer(buffer_size=1)

    # buffer_size=1 flushes immediately, so the item is no longer buffered.
    writer.put_one({"value": 1})

    with pytest.raises(ValueError):
        writer.remove_one({"value": 1})


def test_flush_buffer_with_empty_buffer_is_a_no_op():
    writer, _, table, captured = make_writer(buffer_size=10)

    writer._flush_buffer()

    assert captured == []
    table.append.assert_not_called()
    table.refresh.assert_not_called()


def test_flush_retries_on_commit_conflict_then_succeeds():
    writer, _, table, _ = make_writer(buffer_size=1)
    table.append.side_effect = [
        CommitFailedException("concurrent commit"),
        CommitFailedException("concurrent commit"),
        None,
    ]

    # Neutralize tenacity's backoff sleeps so the test runs instantly.
    with patch("tenacity.nap.time.sleep"):
        writer.put_one({"value": 1})

    assert table.append.call_count == 3
    # refresh runs once per attempt, before each append.
    assert table.refresh.call_count == 3
    assert writer.buffer == []


def test_serde_failure_propagates_without_retry_and_keeps_buffer():
    calls = []

    def failing_serde(schema, items):
        calls.append(list(items))
        raise RuntimeError("serialization failed")

    writer, _, table, _ = make_writer(buffer_size=1, serde=failing_serde)

    with pytest.raises(RuntimeError):
        writer.put_one({"value": 1})

    # serde runs outside the retry loop, so it is called exactly once and the
    # table is never touched; the buffered item survives for a later retry.
    assert calls == [[{"value": 1}]]
    table.refresh.assert_not_called()
    table.append.assert_not_called()
    assert writer.buffer == [{"value": 1}]


def test_retry_is_not_limited_to_commit_conflicts():
    writer, _, table, _ = make_writer(buffer_size=1)
    table.append.side_effect = ValueError("not a commit conflict")

    with patch("tenacity.nap.time.sleep"):
        with pytest.raises(ValueError):
            writer.put_one({"value": 1})

    # The retry decorator sets no exception filter, so even a non-conflict
    # error is retried for all 10 attempts before being reraised.
    assert table.append.call_count == 10
    assert writer.buffer == [{"value": 1}]


def test_flush_reraises_after_ten_failed_attempts_and_keeps_buffer():
    writer, _, table, _ = make_writer(buffer_size=1)
    table.append.side_effect = CommitFailedException("concurrent commit")

    with patch("tenacity.nap.time.sleep") as mock_sleep:
        with pytest.raises(CommitFailedException):
            writer.put_one({"value": 1})

    assert table.append.call_count == 10
    assert table.refresh.call_count == 10
    # 9 sleeps happen between the 10 attempts, all mocked out.
    assert mock_sleep.call_count == 9
    # The failed flush must not drop the buffered item.
    assert writer.buffer == [{"value": 1}]
