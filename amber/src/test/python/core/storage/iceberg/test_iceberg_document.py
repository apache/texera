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
import pytest
import random
import tempfile
import uuid
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from unittest.mock import MagicMock, Mock, patch
from urllib.parse import urlparse

from pyiceberg import types as iceberg_types
from pyiceberg.schema import Schema as IcebergSchema

from core.models import Schema, Tuple
from core.models.state import State
from core.storage.document_factory import DocumentFactory
from core.storage.iceberg import iceberg_document
from core.storage.iceberg.iceberg_document import IcebergDocument
from core.storage.iceberg.iceberg_utils import (
    amber_tuples_to_arrow_table,
    arrow_table_to_amber_tuples,
)
from core.storage.storage_config import StorageConfig
from core.storage.vfs_uri_factory import VFSURIFactory
from proto.org.apache.texera.amber.core import (
    WorkflowIdentity,
    ExecutionIdentity,
    OperatorIdentity,
    PortIdentity,
    GlobalPortIdentity,
    PhysicalOpIdentity,
)

# Hardcoded storage config only for test purposes. The iceberg warehouse
# directory must be a writable absolute path; using `tempfile.mkdtemp()`
# avoids depending on pytest's cwd (an earlier `"../../../../../../amber/
# user-resources/..."` value silently relied on CWD = amber/src/main/python
# and broke when the cwd moved up to amber/).
StorageConfig.initialize(
    catalog_type="postgres",
    postgres_uri_without_scheme="localhost:5432/texera_iceberg_catalog",
    postgres_username="texera",
    postgres_password="password",
    rest_catalog_uri="http://localhost:8181/catalog/",
    rest_catalog_warehouse_name="texera",
    table_result_namespace="operator-port-result",
    table_state_namespace="operator-port-state",
    directory_path=tempfile.mkdtemp(prefix="texera-iceberg-warehouse-"),
    commit_batch_size=4096,
    s3_endpoint="http://localhost:9000",
    s3_region="us-east-1",
    s3_auth_username="minioadmin",
    s3_auth_password="minioadmin",
    s3_large_binaries_base_uri="s3://texera-large-binaries/objects/0/",
)


class TestIcebergDocument:
    @pytest.fixture
    def amber_schema(self):
        """Sample Amber schema"""
        return Schema(
            raw_schema={
                "col-string": "STRING",
                "col-int": "INTEGER",
                "col-bool": "BOOLEAN",
                "col-long": "LONG",
                "col-double": "DOUBLE",
                "col-timestamp": "TIMESTAMP",
                "col-binary": "BINARY",
            }
        )

    @pytest.fixture
    def iceberg_document(self, amber_schema):
        """
        Creates an iceberg document of operator port results using the sample schema
        with a random operator id
        """
        operator_uuid = str(uuid.uuid4()).replace("-", "")
        uri = VFSURIFactory.result_uri(
            VFSURIFactory.create_port_base_uri(
                WorkflowIdentity(id=0),
                ExecutionIdentity(id=0),
                GlobalPortIdentity(
                    op_id=PhysicalOpIdentity(
                        logical_op_id=OperatorIdentity(
                            id=f"test-table-{operator_uuid}"
                        ),
                        layer_name="main",
                    ),
                    port_id=PortIdentity(id=0),
                    input=False,
                ),
            )
        )
        DocumentFactory.create_document(uri, amber_schema)
        document, _ = DocumentFactory.open_document(uri)
        return document

    @pytest.fixture
    def sample_items(self, amber_schema) -> [Tuple]:
        """
        Generates a list of sample tuples
        """
        base_tuples = [
            Tuple(
                {
                    "col-string": "Hello World",
                    "col-int": 42,
                    "col-bool": True,
                    "col-long": 1123213213213,
                    "col-double": 214214.9969346,
                    "col-timestamp": datetime.datetime.now(),
                    "col-binary": b"hello",
                },
                schema=amber_schema,
            ),
            Tuple(
                {
                    "col-string": "",
                    "col-int": -1,
                    "col-bool": False,
                    "col-long": -98765432109876,
                    "col-double": -0.001,
                    "col-timestamp": datetime.datetime.fromtimestamp(100000000),
                    "col-binary": bytearray([255, 0, 0, 64]),
                },
                schema=amber_schema,
            ),
            Tuple(
                {
                    "col-string": "Special Characters: \n\t\r",
                    "col-int": 2147483647,
                    "col-bool": True,
                    "col-long": 9223372036854775807,
                    "col-double": 1.7976931348623157e308,
                    "col-timestamp": datetime.datetime.fromtimestamp(1234567890),
                    "col-binary": bytearray([1, 2, 3, 4, 5]),
                },
                schema=amber_schema,
            ),
        ]

        # Function to generate random binary data
        def generate_random_binary(size):
            return bytearray(random.getrandbits(8) for _ in range(size))

        # Generate additional tuples
        additional_tuples = [
            Tuple(
                {
                    "col-string": None if i % 7 == 0 else f"Generated String {i}",
                    "col-int": None if i % 5 == 0 else i,
                    "col-bool": None if i % 6 == 0 else i % 2 == 0,
                    "col-long": None if i % 4 == 0 else i * 1000000,
                    "col-double": None if i % 3 == 0 else i * 0.12345,
                    "col-timestamp": (
                        None
                        if i % 8 == 0
                        else datetime.datetime.fromtimestamp(
                            datetime.datetime.now().timestamp() + i
                        )
                    ),
                    "col-binary": None if i % 9 == 0 else generate_random_binary(10),
                },
                schema=amber_schema,
            )
            for i in range(1, 20001)
        ]

        return base_tuples + additional_tuples

    def test_basic_read_and_write(self, iceberg_document, sample_items):
        """
        Create an iceberg document, write sample items, and read it back.
        """
        writer = iceberg_document.writer(str(uuid.uuid4()))
        writer.open()
        for item in sample_items:
            writer.put_one(item)
        writer.close()
        retrieved_items = list(iceberg_document.get())
        assert sample_items == retrieved_items

    def test_clear_document(self, iceberg_document, sample_items):
        """
        Create an iceberg document, write sample items, and clear the document.
        """
        writer = iceberg_document.writer(str(uuid.uuid4()))
        writer.open()
        for item in sample_items:
            writer.put_one(item)
        writer.close()
        assert len(list(iceberg_document.get())) > 0

        iceberg_document.clear()
        assert len(list(iceberg_document.get())) == 0

    def test_handle_empty_read(self, iceberg_document):
        """
        The iceberg document should handle empty reads gracefully
        """
        retrieved_items = list(iceberg_document.get())
        assert retrieved_items == []

    def test_concurrent_writes_followed_by_read(self, iceberg_document, sample_items):
        """
        Tests multiple concurrent writers writing to the same iceberg document
        """
        all_items = sample_items
        num_writers = 10
        # Calculate the batch size and the remainder
        batch_size = len(all_items) // num_writers
        remainder = len(all_items) % num_writers
        # Create writer's batches
        item_batches = [
            all_items[
                i * batch_size + min(i, remainder) : i * batch_size
                + min(i, remainder)
                + batch_size
                + (1 if i < remainder else 0)
            ]
            for i in range(num_writers)
        ]

        assert len(item_batches) == num_writers, (
            f"Expected {num_writers} batches but got {len(item_batches)}"
        )

        # Perform concurrent writes
        def write_batch(batch):
            writer = iceberg_document.writer(str(uuid.uuid4()))
            writer.open()
            for item in batch:
                writer.put_one(item)
            writer.close()

        with ThreadPoolExecutor(max_workers=num_writers) as executor:
            futures = [executor.submit(write_batch, batch) for batch in item_batches]
            for future in as_completed(futures):
                future.result()  # Wait for each future to complete

        # Read all items back
        retrieved_items = list(iceberg_document.get())
        # Verify that the retrieved items match the original items
        assert set(retrieved_items) == set(all_items), (
            "All items should be read correctly after concurrent writes."
        )

    def test_read_using_range(self, iceberg_document, sample_items):
        """
        The iceberg document should read all items using rages correctly.
        """
        writer = iceberg_document.writer(str(uuid.uuid4()))
        writer.open()
        for item in sample_items:
            writer.put_one(item)
        writer.close()
        # Read all items using ranges
        batch_size = 1500
        # Generate ranges
        ranges = [
            range(i, min(i + batch_size, len(sample_items)))
            for i in range(0, len(sample_items), batch_size)
        ]

        # Retrieve items using ranges
        retrieved_items = [
            item for r in ranges for item in iceberg_document.get_range(r.start, r.stop)
        ]

        assert len(retrieved_items) == len(sample_items), (
            "The number of retrieved items does not match the number of all items."
        )

        # Verify that the retrieved items match the original items
        assert set(retrieved_items) == set(sample_items), (
            "All items should be retrieved correctly using ranges."
        )

    def test_get_after(self, iceberg_document, sample_items):
        """
        The iceberg document should retrieve items correctly using get_after
        """
        writer = iceberg_document.writer(str(uuid.uuid4()))
        writer.open()
        for item in sample_items:
            writer.put_one(item)
        writer.close()
        # Test get_after for various offsets
        offsets = [0, len(sample_items) // 2, len(sample_items) - 1]
        for offset in offsets:
            if offset < len(sample_items):
                expected_items = sample_items[offset:]
            else:
                expected_items = []

            retrieved_items = list(iceberg_document.get_after(offset))
            assert retrieved_items == expected_items, (
                f"get_after({offset}) did not return the expected items. "
                f"Expected: {expected_items}, Got: {retrieved_items}"
            )

        # Test get_after for an offset beyond the range
        invalid_offset = len(sample_items)
        retrieved_items = list(iceberg_document.get_after(invalid_offset))
        assert not retrieved_items, (
            f"get_after({invalid_offset}) should return "
            f"an empty list, but got: {retrieved_items}"
        )

    def test_get_counts(self, iceberg_document, sample_items):
        """
        The iceberg document should correctly return the count of items.
        """
        writer = iceberg_document.writer(str(uuid.uuid4()))
        writer.open()
        for item in sample_items:
            writer.put_one(item)
        writer.close()

        assert iceberg_document.get_count() == len(sample_items), (
            "get_count should return the same number as the length of sample_items"
        )

    def test_state_materialization_round_trip(self):
        operator_uuid = str(uuid.uuid4()).replace("-", "")
        base_uri = VFSURIFactory.create_port_base_uri(
            WorkflowIdentity(id=0),
            ExecutionIdentity(id=0),
            GlobalPortIdentity(
                op_id=PhysicalOpIdentity(
                    logical_op_id=OperatorIdentity(id=f"test-state-{operator_uuid}"),
                    layer_name="main",
                ),
                port_id=PortIdentity(id=0),
                input=False,
            ),
        )
        state_uri = VFSURIFactory.state_uri(base_uri)
        DocumentFactory.create_document(state_uri, State.SCHEMA)
        document, _ = DocumentFactory.open_document(state_uri)

        state = State(
            {
                "loop_counter": 3,
                "name": "outer-loop",
                "payload": b"\x00\x01state-bytes",
                "nested": {"enabled": True, "values": [1, 2, 3]},
            }
        )

        writer = document.writer(str(uuid.uuid4()))
        writer.open()
        writer.put_one(state.to_tuple())
        writer.close()

        stored_rows = list(document.get())
        assert len(stored_rows) == 1
        assert State.from_tuple(stored_rows[0]) == state

    def test_multiple_states_materialize_as_rows_in_one_table(self):
        operator_uuid = str(uuid.uuid4()).replace("-", "")
        base_uri = VFSURIFactory.create_port_base_uri(
            WorkflowIdentity(id=0),
            ExecutionIdentity(id=0),
            GlobalPortIdentity(
                op_id=PhysicalOpIdentity(
                    logical_op_id=OperatorIdentity(
                        id=f"test-multiple-states-{operator_uuid}"
                    ),
                    layer_name="main",
                ),
                port_id=PortIdentity(id=0),
                input=False,
            ),
        )
        state_uri = VFSURIFactory.state_uri(base_uri)
        DocumentFactory.create_document(state_uri, State.SCHEMA)
        document, _ = DocumentFactory.open_document(state_uri)

        states = [
            State({"loop_counter": 0, "i": 1, "payload": b"first"}),
            State(
                {
                    "loop_counter": 1,
                    "i": 2,
                    "payload": b"second",
                    "nested": {"values": [3, 4]},
                }
            ),
        ]

        writer = document.writer(str(uuid.uuid4()))
        writer.open()
        for state in states:
            writer.put_one(state.to_tuple())
        writer.close()

        stored_rows = list(document.get())
        assert len(stored_rows) == len(states)
        actual_states = sorted(
            [State.from_tuple(row) for row in stored_rows],
            key=lambda state: state["loop_counter"],
        )
        assert actual_states == states


class TestIcebergDocumentWithMockCatalog:
    """
    The catalog-facing paths of IcebergDocument that need no catalog service:
    the table-location lookup, the "table is absent" arms of clear() and
    get_count(), the locks clear() and the read path take, what each read entry
    point hands the iterator, and both sides of the iterator's seek guard. The
    catalog is a mock, so unlike TestIcebergDocument above these run on any host
    without a live Iceberg catalog, and they mutate no cached catalog state.
    """

    @pytest.fixture
    def iceberg_schema(self):
        return IcebergSchema(
            iceberg_types.NestedField(
                field_id=1,
                name="col-int",
                field_type=iceberg_types.IntegerType(),
                required=False,
            )
        )

    @pytest.fixture
    def document(self, iceberg_schema):
        """
        An IcebergDocument for `ns.tbl` whose catalog is a mock. `get_instance`
        is patched only for the duration of the construction, so the real
        per-warehouse catalog cache is never touched.
        """
        catalog = Mock()
        with patch.object(
            iceberg_document.IcebergCatalogInstance,
            "get_instance",
            return_value=catalog,
        ):
            return IcebergDocument(
                "ns",
                "tbl",
                iceberg_schema,
                amber_tuples_to_arrow_table,
                arrow_table_to_amber_tuples,
            )

    def test_get_uri_returns_the_parsed_table_location(self, document):
        # The location is unique per run and the expectation is derived from it,
        # so a get_uri that returned a fixed URI instead of parsing the loaded
        # table's own location could not accidentally match.
        location = f"file:///warehouse/{uuid.uuid4().hex}/ns.db/tbl"
        table = Mock()
        table.location.return_value = location

        with patch.object(
            iceberg_document, "load_table_metadata", return_value=table
        ) as load_table_metadata:
            uri = document.get_uri()

        expected = urlparse(location)
        assert (uri.scheme, uri.path) == (expected.scheme, expected.path)
        table.location.assert_called_once_with()
        assert load_table_metadata.call_args.args == (document.catalog, "ns", "tbl")

    def test_get_uri_rejects_a_table_that_does_not_exist(self, document):
        with patch.object(iceberg_document, "load_table_metadata", return_value=None):
            with pytest.raises(Exception, match=r"table ns\.tbl doesn't exist\."):
                document.get_uri()

    def test_clear_drops_a_table_that_exists(self, document):
        document.catalog.table_exists.return_value = True

        document.clear()

        document.catalog.drop_table.assert_called_once_with("ns.tbl")

    def test_clear_leaves_an_absent_table_alone(self, document):
        document.catalog.table_exists.return_value = False

        document.clear()

        document.catalog.table_exists.assert_called_once_with("ns.tbl")
        document.catalog.drop_table.assert_not_called()

    def test_get_count_is_zero_when_the_table_does_not_exist(self, document):
        with patch.object(
            iceberg_document, "load_table_metadata", return_value=None
        ) as load_table_metadata:
            assert document.get_count() == 0

        assert load_table_metadata.call_args.args == (document.catalog, "ns", "tbl")

    def test_a_negative_offset_is_rejected_rather_than_read_as_zero(self, document):
        """
        IcebergIterator guards its file seek against having already skipped past
        `from_index`. The only way the guard can fire is a negative offset, since
        the skip counter is still 0 when the seek generator first runs.

        Neither the guard's message ("seek operation should not be called", which
        describes a re-entrant seek rather than a bad argument) nor its exception
        type is pinned: rejecting a negative offset as a ValueError would be the
        better behaviour, so accepting either type here keeps that fix open while
        still requiring that the offset is rejected rather than read as zero.
        """
        iterator = document.get_after(-1)

        with pytest.raises((RuntimeError, ValueError)):
            next(iterator)

    @pytest.mark.parametrize("offset", [0, 5])
    def test_a_non_negative_offset_does_not_trip_the_seek_guard(self, document, offset):
        """
        The other side of the seek guard's boundary: a legal offset must reach
        the table lookup and then end the iteration cleanly, not raise. Without
        this, the guard's comparison is unconstrained on hosts that cannot run
        TestIcebergDocument above.
        """
        with patch.object(
            iceberg_document, "load_table_metadata", return_value=None
        ) as load_table_metadata:
            with pytest.raises(StopIteration):
                next(document.get_after(offset))

        assert load_table_metadata.call_args.args == (document.catalog, "ns", "tbl")

    @pytest.mark.parametrize(
        "read, from_index, until_index, total",
        [
            (lambda document: document.get(), 0, None, float("inf")),
            (lambda document: document.get_range(3, 7), 3, 7, 4),
            (lambda document: document.get_after(4), 4, None, float("inf")),
        ],
    )
    def test_the_read_entry_points_delegate_to_the_iterator(
        self, document, read, from_index, until_index, total
    ):
        """
        Each read entry point hands a specific [from, until) range -- and the
        document's own catalog, table identity, schema and deserde -- to the
        iterator. The seek generator's body does not run at construction, so
        this needs no catalog.

        `total` is spelled out per case rather than recomputed from the range,
        so the expectation does not restate the production formula.
        """
        iterator = read(document)

        assert (iterator.from_index, iterator.until_index) == (from_index, until_index)
        assert iterator.total_records_to_return == total
        assert (iterator.table_namespace, iterator.table_name) == ("ns", "tbl")
        assert iterator.catalog is document.catalog
        assert iterator.table_schema is document.table_schema
        assert iterator.deserde is document.deserde

    def test_the_read_path_takes_the_shared_read_lock(self, document):
        """
        The counterpart of test_clear_takes_the_write_lock: reads must take the
        shared read lock, so that concurrent reads are not serialised behind
        each other. MagicMock (not Mock) is required: the lock is used as a
        context manager.
        """
        document.lock = MagicMock()

        document.get()

        document.lock.gen_rlock.assert_called_once_with()
        document.lock.gen_wlock.assert_not_called()

    def test_clear_takes_the_write_lock(self, document):
        """
        clear() drops the table, so it must hold the write lock rather than the
        shared read lock the readers take. MagicMock (not Mock) is required: the
        lock is used as a context manager.
        """
        document.catalog.table_exists.return_value = True
        document.lock = MagicMock()

        document.clear()

        document.lock.gen_wlock.assert_called_once_with()
        document.lock.gen_rlock.assert_not_called()
        document.catalog.drop_table.assert_called_once_with("ns.tbl")
