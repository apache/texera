from pathlib import Path

from tenacity import retry, wait_exponential, stop_after_attempt
from typing import List, TypeVar

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from core.storage.model.buffered_item_writer import BufferedItemWriter

# Define a type variable for the data type T
T = TypeVar('T')


class IcebergTableWriter(BufferedItemWriter[T]):
    """
    IcebergTableWriter writes data to the given Iceberg table in an append-only way.
    - Each time the buffer is flushed, a new data file is created with a unique name.
    - The `writer_identifier` is used to prefix the created files.
    - Iceberg data files are immutable once created. So each flush will create a distinct file.

    **Thread Safety**: This writer is NOT thread-safe, so only one thread should call this writer.

    :param writer_identifier: A unique identifier used to prefix the created files.
    :param catalog: The Iceberg catalog to manage table metadata.
    :param table_namespace: The namespace of the Iceberg table.
    :param table_name: The name of the Iceberg table.
    :param table_schema: The schema of the Iceberg table.
    :param buffer_size: The maximum size of the buffer before flushing.
    """

    def __init__(
            self,
            writer_identifier: str,
            catalog: Catalog,
            table_namespace: str,
            table_name: str,
            table_schema: pa.Schema,
            buffer_size: int = 4096  # Default buffer size TODO: move to config
    ):
        self.writer_identifier = writer_identifier
        self.catalog = catalog
        self.table_namespace = table_namespace
        self.table_name = table_name
        self.table_schema = table_schema
        self.buffer_size = buffer_size

        # Internal state
        self.buffer: List[T] = []
        self.filename_idx = 0
        self.record_id = 0

        # Load the Iceberg table
        self.table: Table = self.catalog.load_table(f"{self.table_namespace}.{self.table_name}")

    @property
    def buffer_size(self) -> int:
        return self._buffer_size

    def open(self) -> None:
        """Open the writer and clear the buffer."""
        self.buffer.clear()

    def put_one(self, item: T) -> None:
        """Add a single item to the buffer."""
        self.buffer.append(item)
        if len(self.buffer) >= self.buffer_size:
            self.flush_buffer()

    def remove_one(self, item: T) -> None:
        """Remove a single item from the buffer."""
        self.buffer.remove(item)

    def flush_buffer(self) -> None:
        """Flush the current buffer to a new Iceberg data file."""
        if not self.buffer:
            return
        df = pa.Table.from_pydict(
            {
                name: [t[name] for t in self.buffer]
                for name in self.table_schema.names
            },
            schema=self.table_schema,
        )

        def append_to_table_with_retry(pa_df: pa.Table) -> None:
            """Appends a pyarrow dataframe to the table in the catalog using tenacity exponential backoff."""

            @retry(
                wait=wait_exponential(multiplier=1, min=4, max=32),
                stop=stop_after_attempt(10),
                reraise=True
            )
            def append_with_retry():
                # table = catalog.load_table(table_name)  # <---- If a process appends between this line ...
                self.table.refresh()
                self.table.append(pa_df)  # <----- and this line, then Tenacity will retry.
                self.filename_idx+=1

            append_with_retry()

        append_to_table_with_retry(df)
        self.buffer.clear()

    def close(self) -> None:
        """Close the writer, ensuring any remaining buffered items are flushed."""
        if self.buffer:
            self.flush_buffer()

    @buffer_size.setter
    def buffer_size(self, value):
        self._buffer_size = value
