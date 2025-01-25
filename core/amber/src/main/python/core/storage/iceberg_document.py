from threading import RLock
from typing import Iterator, Optional, Callable, Iterable
from typing import TypeVar
from urllib.parse import ParseResult, urlparse

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table, FileScanTask
from readerwriterlock import rwlock

from core.storage.iceberg_catalog_instance import IcebergCatalogInstance
from core.storage.iceberg_table_writer import IcebergTableWriter
from core.storage.iceberg_utils import load_table_metadata, read_data_file_as_arrow_table
from core.storage.model.virtual_document import VirtualDocument

# Define a type variable
T = TypeVar('T')


class IcebergDocument(VirtualDocument[T]):
    """
    IcebergDocument is used to read and write a set of T as an Iceberg table.
    It provides iterator-based read methods and supports multiple writers to write to the same table.

    - On construction, the table will be created if it does not exist.
    - If the table exists, it will be overridden.

    :param table_namespace: Namespace of the table.
    :param table_name: Name of the table.
    :param table_schema: Schema of the table.
    """

    def __init__(
            self,
            table_namespace: str,
            table_name: str,
            table_schema: Schema,
            serde: Callable[[Schema, Iterable[T]], pa.Table],
            deserde: Callable[[Schema, pa.Table], Iterable[T]],
            catalog: Optional[Catalog] = None
    ):
        self.table_namespace = table_namespace
        self.table_name = table_name
        self.table_schema = table_schema
        self.serde = serde
        self.deserde = deserde

        self.lock = rwlock.RWLockFair()
        self.catalog = catalog or self._load_catalog()

        # Create or override the table during initialization
        load_table_metadata(
            self.catalog,
            self.table_namespace,
            self.table_name
        )

    def _load_catalog(self) -> Catalog:
        """Load the Iceberg catalog."""
        # Implement catalog loading logic here, e.g., load from configuration
        return IcebergCatalogInstance.get_instance()

    def get_uri(self) -> ParseResult:
        """Returns the URI of the table location."""
        table = load_table_metadata(self.catalog, self.table_namespace, self.table_name)
        if not table:
            raise Exception(f"table {self.table_namespace}.{self.table_name} doesn't exist.")
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

    def get_range(self, from_index: int, until: int) -> Iterator[T]:
        """Get records within a specified range [from, until)."""
        return self._get_using_file_sequence_order(from_index, until)

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
        """Creates a BufferedItemWriter for writing data to the table."""
        return IcebergTableWriter(
            writer_identifier=writer_identifier,
            catalog=self.catalog,
            table_namespace=self.table_namespace,
            table_name=self.table_name,
            table_schema=self.table_schema,
            serde=self.serde
        )

    def _get_using_file_sequence_order(
            self, from_index: int, until: Optional[int]
    ) -> Iterator[T]:
        """Utility to get records within a specified range."""
        with self.lock.gen_rlock():
            iterator = IcebergIterator(
                from_index,
                until,
                self.catalog,
                self.table_namespace,
                self.table_name,
                self.table_schema,
                self.deserde
            )
            return iterator


class IcebergIterator(Iterator[T]):
    def __init__(self, from_index, until, catalog, table_namespace, table_name, table_schema, deserde):
        self.from_index = from_index
        self.until = until
        self.catalog = catalog
        self.table_namespace = table_namespace
        self.table_name = table_name
        self.table_schema = table_schema
        self.deserde = deserde
        self.lock = RLock()
        self.num_of_skipped_records = 0
        self.num_of_returned_records = 0
        self.total_records_to_return = self.until - self.from_index if until else float("inf")
        self.current_record_iterator = iter([])
        self.table = self._load_table_metadata()
        self.usable_file_iterator = self._seek_to_usable_file()

    def _load_table_metadata(self) -> Optional[Table]:
        """Load table metadata."""
        return load_table_metadata(self.catalog, self.table_namespace, self.table_name)

    def _seek_to_usable_file(self) -> Iterator[FileScanTask]:
        """Find usable file scan tasks starting from the specified record index."""
        with self.lock:
            if self.num_of_skipped_records > self.from_index:
                raise RuntimeError("seek operation should not be called")

            # Refresh table snapshots
            if not self.table:
                self.table = self._load_table_metadata()

            if self.table:
                try:
                    self.table.refresh()
                    # Retrieve the entries from the table
                    entries = self.table.inspect.metadata_log_entries()

                    # Convert to a Pandas DataFrame for easy manipulation
                    entries_df = entries.to_pandas()

                    # Sort by file_sequence_number
                    file_sequence_map = {
                        row["file"]: row["latest_sequence_number"]
                        for _, row in entries_df.iterrows()
                    }

                    # Retrieve and sort the file scan tasks by file sequence number
                    file_scan_tasks = list(self.table.scan().plan_files())
                    sorted_file_scan_tasks = sorted(
                        file_scan_tasks,
                        key=lambda task: file_sequence_map.get(task.file.file_path, float('inf'))  # Use float('inf') for missing files
                    )
                    # Skip records in files before the `from_index`
                    for task in sorted_file_scan_tasks:
                        record_count = task.file.record_count
                        if self.num_of_skipped_records + record_count <= self.from_index:
                            self.num_of_skipped_records += record_count
                            continue
                        yield task
                except Exception:
                    print("Could not read iceberg table:\n", Exception)
                    return iter([])
            else:
                return iter([])

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
                    self.current_record_iterator = self.deserde(self.table_schema, arrow_table)
                    # Skip records within the file if necessary
                    records_to_skip_in_file = self.from_index - self.num_of_skipped_records
                    if records_to_skip_in_file > 0:
                        self.current_record_iterator = self._skip_records(
                            self.current_record_iterator,
                            records_to_skip_in_file
                        )
                        self.num_of_skipped_records += records_to_skip_in_file
                except StopIteration:
                    # no more files left in this table
                    raise StopIteration("No more records available")

    @staticmethod
    def _skip_records(iterator, count):
        return iter(list(iterator)[count:])
