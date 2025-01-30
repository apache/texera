from itertools import islice
from threading import RLock
from typing import Iterator, Optional, Callable, Iterable
from typing import TypeVar
from urllib.parse import ParseResult, urlparse

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table, FileScanTask
from readerwriterlock import rwlock

from core.storage.iceberg.iceberg_catalog_instance import IcebergCatalogInstance
from core.storage.iceberg.iceberg_table_writer import IcebergTableWriter
from core.storage.iceberg.iceberg_utils import (
    load_table_metadata,
    read_data_file_as_arrow_table,
)
from core.storage.model.virtual_document import VirtualDocument

# Define a type variable
T = TypeVar("T")


class IcebergDocument(VirtualDocument[T]):
    """
    IcebergDocument is used to read and write a set of T as an Iceberg table.
    It provides iterator-based read methods and supports multiple writers to write to
    the same table.

    - On construction, the table will be created if it does not exist.
    - If the table exists, it will be overridden.

    :param table_namespace: Namespace of the table.
    :param table_name: Name of the table.
    :param table_schema: Schema of the table.
    :param serde: A function to convert a T iterable into a pyarrow Table. Note the
    conversion is not based on a single T item (unlike Texera's Java IcebergDocument.)
    :param deserde: A function to convert a pyarrow Table back into a T iterable.
    """

    def __init__(
        self,
        table_namespace: str,
        table_name: str,
        table_schema: Schema,
        serde: Callable[[Schema, Iterable[T]], pa.Table],
        deserde: Callable[[Schema, pa.Table], Iterable[T]],
    ):
        self.table_namespace = table_namespace
        self.table_name = table_name
        self.table_schema = table_schema
        self.serde = serde
        self.deserde = deserde

        self.lock = rwlock.RWLockFair()
        self.catalog = IcebergCatalogInstance.get_instance()

    def get_uri(self) -> ParseResult:
        """Returns the URI of the table location."""
        table = load_table_metadata(self.catalog, self.table_namespace, self.table_name)
        if not table:
            raise Exception(
                f"table {self.table_namespace}.{self.table_name} doesn't exist."
            )
        return urlparse(table.location())

    def clear(self):
        """Deletes the table and clears its contents."""
        with self.lock.gen_wlock():
            table_identifier = f"{self.table_namespace}.{self.table_name}"
            if self.catalog.table_exists(table_identifier):
                self.catalog.drop_table(table_identifier)

    def get(self) -> Iterator[T]:
        """Get an iterator for reading all records from the table."""
        return self._get_using_file_sequence_order(0, None)

    def get_range(self, from_index: int, until_index: int) -> Iterator[T]:
        """Get records within a specified range [from, until)."""
        return self._get_using_file_sequence_order(from_index, until_index)

    def get_after(self, offset: int) -> Iterator[T]:
        """Get records starting after a specified offset."""
        return self._get_using_file_sequence_order(offset, None)

    def get_count(self) -> int:
        """Get the total count of records in the table."""
        table = load_table_metadata(self.catalog, self.table_namespace, self.table_name)
        if not table:
            return 0
        return sum(f.file.record_count for f in table.scan().plan_files())

    def writer(self, writer_identifier: str):
        """
        Creates a BufferedItemWriter for writing data to the table.
        :param writer_identifier: The writer's ID. It should be unique within the same
        table, as each writer will use it as the prefix of the files they append
        :return: An IcebergTableWriter
        """
        return IcebergTableWriter[T](
            writer_identifier=writer_identifier,
            catalog=self.catalog,
            table_namespace=self.table_namespace,
            table_name=self.table_name,
            table_schema=self.table_schema,
            serde=self.serde,
        )

    def _get_using_file_sequence_order(
        self, from_index: int, until_index: Optional[int]
    ) -> Iterator[T]:
        """Utility to get records within a specified range."""
        with self.lock.gen_rlock():
            return IcebergIterator[T](
                from_index,
                until_index,
                self.catalog,
                self.table_namespace,
                self.table_name,
                self.table_schema,
                self.deserde,
            )


class IcebergIterator(Iterator[T]):
    """
    A custom iterator class to read items from an iceberg table based on an index range.
    """

    def __init__(
        self,
        from_index: int,
        until_index: int,
        catalog: Catalog,
        table_namespace: str,
        table_name: str,
        table_schema: Schema,
        deserde: Callable[[Schema, pa.Table], Iterable[T]],
    ):
        self.from_index = from_index
        self.until_index = until_index
        self.catalog = catalog
        self.table_namespace = table_namespace
        self.table_name = table_name
        self.table_schema = table_schema
        self.deserde = deserde
        self.lock = RLock()
        # Counter for how many records have been skipped
        self.num_of_skipped_records = 0
        # Counter for how many records have been returned
        self.num_of_returned_records = 0
        # Total number of records to return, used for termination condition
        self.total_records_to_return = (
            self.until_index - self.from_index if until_index else float("inf")
        )
        # Load the table instance, initially the table instance may not exist
        self.table = self._load_table_metadata()
        # Iterator for usable file scan tasks
        self.usable_file_iterator = self._seek_to_usable_file()
        # Current record iterator for the active file
        self.current_record_iterator = iter([])

    def _load_table_metadata(self) -> Optional[Table]:
        """Util function to load the table's metadata."""
        return load_table_metadata(self.catalog, self.table_namespace, self.table_name)

    def _seek_to_usable_file(self) -> Iterator[FileScanTask]:
        """Find usable file scan tasks starting from the specified record index."""
        with self.lock:
            if self.num_of_skipped_records > self.from_index:
                raise RuntimeError("seek operation should not be called")

            # Load the table for the first time
            if not self.table:
                self.table = self._load_table_metadata()

            # If the table still does not exist after loading, end iterator.
            if self.table:
                try:
                    self.table.refresh()
                    current_snapshot = self.table.current_snapshot()
                    if current_snapshot is None:
                        return iter([])
                    sorted_file_scan_tasks = self._extract_sorted_file_scan_tasks(
                        current_snapshot
                    )
                    # Skip records in files before the `from_index`
                    for task in sorted_file_scan_tasks:
                        record_count = task.file.record_count
                        if (
                            self.num_of_skipped_records + record_count
                            <= self.from_index
                        ):
                            self.num_of_skipped_records += record_count
                            continue
                        yield task
                except Exception:
                    print("Could not read iceberg table:\n")
                    raise Exception
            else:
                return iter([])

    def _extract_sorted_file_scan_tasks(self, current_snapshot):
        """
        As self.table.inspect.entries() does not work with java files, this method
        implements the logic to find file_sequence_number for each data file ourselves
        :param current_snapshot: The current snapshot of the table.
        :return: The file scan tasks of the file sorted by file_sequence_number
        """
        file_sequence_map = {}
        for manifest in current_snapshot.manifests(self.table.io):
            for entry in manifest.fetch_manifest_entry(io=self.table.io):
                file_sequence_map[entry.data_file.file_path] = entry.sequence_number
        # Retrieve and sort the file scan tasks by file sequence number
        file_scan_tasks = list(self.table.scan().plan_files())
        # Sort files by their sequence number. Files without a sequence
        # number will be read last.
        sorted_file_scan_tasks = sorted(
            file_scan_tasks,
            key=lambda t: file_sequence_map.get(t.file.file_path, float("inf")),
        )
        return sorted_file_scan_tasks

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        if self.num_of_returned_records >= self.total_records_to_return:
            raise StopIteration("No more records available")

        while True:
            try:
                record = next(self.current_record_iterator)
                self.num_of_returned_records += 1
                return record
            except StopIteration:
                # current_record_iterator is exhausted, need to go to the next file
                try:
                    next_file = next(self.usable_file_iterator)
                    arrow_table = read_data_file_as_arrow_table(next_file, self.table)
                    self.current_record_iterator = self.deserde(
                        self.table_schema, arrow_table
                    )
                    # Skip records within the file if necessary
                    records_to_skip_in_file = (
                        self.from_index - self.num_of_skipped_records
                    )
                    if records_to_skip_in_file > 0:
                        self.current_record_iterator = self._skip_records(
                            self.current_record_iterator, records_to_skip_in_file
                        )
                        self.num_of_skipped_records += records_to_skip_in_file
                except StopIteration:
                    # no more files left in this table
                    raise StopIteration("No more records available")

    @staticmethod
    def _skip_records(iterator, count):
        return islice(iterator, count, None)
