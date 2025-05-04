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

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from tenacity import retry, stop_after_attempt, wait_random_exponential
from typing import List, TypeVar, Callable, Iterable

from core.storage.model.buffered_item_writer import BufferedItemWriter
from core.storage.storage_config import StorageConfig

# Define a type variable for the data type T
T = TypeVar("T")


class IcebergTableWriter(BufferedItemWriter[T]):
    """
    IcebergTableWriter writes data to the given Iceberg table in an append-only way.
    - Each time the buffer is flushed, a new data file is created using pyarrow
    - Iceberg data files are immutable once created. So each flush will create a
    distinct file.

    **Thread Safety**: This writer is NOT thread-safe, so only one thread should call
    this writer.

    :param writer_identifier: A unique identifier used to prefix the created files.
    :param catalog: The Iceberg catalog to manage table metadata.
    :param table_namespace: The namespace of the Iceberg table.
    :param table_name: The name of the Iceberg table.
    :param table_schema: The schema of the Iceberg table.
    """

    def __init__(
        self,
        writer_identifier: str,
        catalog: Catalog,
        table_namespace: str,
        table_name: str,
        table_schema: pa.Schema,
        serde: Callable[[Schema, Iterable[T]], pa.Table],
    ):
        self.writer_identifier = writer_identifier
        self.catalog = catalog
        self.table_namespace = table_namespace
        self.table_name = table_name
        self.table_schema = table_schema
        self.serde = serde
        self.buffer_size = StorageConfig.ICEBERG_TABLE_COMMIT_BATCH_SIZE

        # Internal state
        self.buffer: List[T] = []

        # Load the Iceberg table
        self.table: Table = self.catalog.load_table(
            f"{self.table_namespace}.{self.table_name}"
        )

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
            self._flush_buffer()

    def remove_one(self, item: T) -> None:
        """Remove a single item from the buffer."""
        self.buffer.remove(item)

    def _flush_buffer(self) -> None:
        """
        Flush the current buffer to a new Iceberg data file. The buffer is first
        converted to a pyarrow table, and then appended to the iceberg table as a
        parquet file. Note in the case of concurrent writers, as iceberg uses
        optimistic concurrency control, we use a random exponential backoff mechanism
        when commit failure happens because currently pyiceberg does not natively
        support retry.
        """
        if not self.buffer:
            return
        df = self.serde(self.table_schema, self.buffer)

        def append_to_table_with_retry(pa_df: pa.Table) -> None:
            @retry(
                wait=wait_random_exponential(0.001, 10),
                stop=stop_after_attempt(10),
                reraise=True,
            )
            def append_with_retry():
                self.table.refresh()
                self.table.append(pa_df)

            append_with_retry()

        append_to_table_with_retry(df)
        self.buffer.clear()

    def close(self) -> None:
        """Close the writer, ensuring any remaining buffered items are flushed."""
        if self.buffer:
            self._flush_buffer()

    @buffer_size.setter
    def buffer_size(self, value):
        self._buffer_size = value
