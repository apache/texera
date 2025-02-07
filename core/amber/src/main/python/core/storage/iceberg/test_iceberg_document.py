import datetime
import random
import uuid
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

import pytest

from core.models import Schema, Tuple
from core.storage.document_factory import DocumentFactory
from core.storage.storage_config import StorageConfig
from core.storage.vfs_uri_factory import VFSURIFactory
from proto.edu.uci.ics.amber.core import (
    WorkflowIdentity,
    ExecutionIdentity,
    OperatorIdentity,
    PortIdentity,
)

# Hardcoded storage config only for test purposes.
StorageConfig.initialize(
    postgres_uri_without_scheme="localhost:5432/texera_iceberg_catalog",
    postgres_username="texera_iceberg_admin",
    postgres_password="password",
    table_namespace="operator-port-result",
    directory_path="../../../../../../core/amber/user-resources/workflow-results",
    commit_batch_size=4096,
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
        uri = VFSURIFactory.create_result_uri(
            WorkflowIdentity(id=0),
            ExecutionIdentity(id=0),
            OperatorIdentity(id=f"test_table_{operator_uuid}"),
            PortIdentity(id=0),
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
                i * batch_size
                + min(i, remainder) : i * batch_size
                + min(i, remainder)
                + batch_size
                + (1 if i < remainder else 0)
            ]
            for i in range(num_writers)
        ]

        assert (
            len(item_batches) == num_writers
        ), f"Expected {num_writers} batches but got {len(item_batches)}"

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
        assert set(retrieved_items) == set(
            all_items
        ), "All items should be read correctly after concurrent writes."

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

        assert len(retrieved_items) == len(
            sample_items
        ), "The number of retrieved items does not match the number of all items."

        # Verify that the retrieved items match the original items
        assert set(retrieved_items) == set(
            sample_items
        ), "All items should be retrieved correctly using ranges."

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

        assert iceberg_document.get_count() == len(
            sample_items
        ), "get_count should return the same number as the length of sample_items"
