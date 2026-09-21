/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.core.storage.result.iceberg

import org.apache.texera.common.config.StorageConfig
import org.apache.texera.amber.core.storage.IcebergCatalogInstance
import org.apache.texera.amber.core.storage.model.{ArrowVectorizedSink, BufferedItemWriter}
import org.apache.texera.amber.core.tuple.{AttributeTypeUtils, LargeBinary}
import org.apache.texera.amber.util.{ArrowUtils, IcebergUtil}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.data.Record
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.io.{DataWriter, OutputFile}
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.types.Types.StructType
import org.apache.iceberg.{Schema, Table}
import org.apache.parquet.schema.MessageType

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.time.ZoneId
import scala.collection.mutable.ArrayBuffer

/**
  * IcebergTableWriter writes data to the given Iceberg table in an append-only way.
  * - Each time the buffer is flushed, a new data file is created with a unique name.
  * - The `writerIdentifier` is used to prefix the created files.
  * - Iceberg data files are immutable once created. So each flush will create a distinct file.
  *
  * **Thread Safety**: This writer is **NOT thread-safe**, so only one thread should call this writer.
  *
  * @param writerIdentifier a unique identifier used to prefix the created files.
  * @param warehouse the warehouse whose catalog manages the table metadata; `None` uses the
  *                  configured default.
  * @param tableNamespace the namespace of the Iceberg table.
  * @param tableName the name of the Iceberg table.
  * @param tableSchema the schema of the Iceberg table.
  * @param serde a function to serialize `T` into an Iceberg `Record`.
  * @tparam T the type of the data items written to the table.
  */
private[storage] class IcebergTableWriter[T](
    val writerIdentifier: String,
    val warehouse: Option[String],
    val tableNamespace: String,
    val tableName: String,
    val tableSchema: Schema,
    val serde: (org.apache.iceberg.Schema, T) => Record
) extends BufferedItemWriter[T]
    with ArrowVectorizedSink {

  // Resolved per use (#7290): the catalog cache is bounded and closes evicted entries,
  // so the writer must not pin one across its lifetime.
  private def catalog: Catalog = IcebergCatalogInstance.getInstance(warehouse)

  // Buffer to hold items before flushing to the table
  private val buffer = new ArrayBuffer[T]()
  // Incremental filename index, incremented each time a new buffer is flushed
  private var filenameIdx = 0
  // Incremental record ID, incremented for each record
  private var recordId = 0

  override val bufferSize: Int = StorageConfig.icebergTableCommitBatchSize

  /**
    * Open the writer and clear the buffer.
    */
  override def open(): Unit = {
    buffer.clear()
  }

  /**
    * Add a single item to the buffer.
    * - If the buffer size exceeds the configured limit, the buffer is flushed.
    * @param item the item to add to the buffer.
    */
  override def putOne(item: T): Unit = {
    buffer.append(item)
    if (buffer.size >= bufferSize) {
      flushBuffer()
    }
  }

  /**
    * Remove a single item from the buffer.
    * @param item the item to remove from the buffer.
    */
  override def removeOne(item: T): Unit = {
    buffer -= item
  }

  /**
    * Flush the current buffer to a new Iceberg data file.
    * - Creates a new data file using the writer identifier and an incremental filename index.
    * - Writes all buffered items to the new file and commits it to the Iceberg table.
    */
  private def flushBuffer(): Unit = {
    if (buffer.nonEmpty) {
      // Resolve the table per flush (#7290): an eagerly-held Table would pin REST
      // operations backed by a catalog the bounded cache may close, and resolving
      // here also keeps this warehouse's cache entry live for the whole execution.
      val table: Table = IcebergUtil
        .loadTableMetadata(catalog, tableNamespace, tableName)
        .get
      writeRecordsToNewFile(table, buffer.iterator.map(item => serde(tableSchema, item)))
      buffer.clear()
    }
  }

  // Write a batch of Iceberg Records to a new, uniquely named data file and
  // commit it. Iceberg's DataWriter produces the file metrics and DataFile, so
  // the callers (row buffer flush and the Arrow batch path) get a valid table.
  // The table is resolved per call (#7290), so callers pass in the live one.
  private def writeRecordsToNewFile(table: Table, records: Iterator[Record]): Unit = {
    val location = table.location().stripSuffix("/")
    val filepathString = s"$location/${writerIdentifier}_$filenameIdx"
    filenameIdx += 1
    val outputFile: OutputFile = table.io().newOutputFile(filepathString)
    val dataWriter: DataWriter[Record] = Parquet
      .writeData(outputFile)
      .forTable(table)
      .createWriterFunc((schema: Schema, messageType: MessageType) =>
        GenericParquetWriter.create(schema, messageType)
      )
      .overwrite()
      .build()
    try records.foreach(dataWriter.write)
    finally dataWriter.close()
    table.newAppend().appendFile(dataWriter.toDataFile).commit()
  }

  // Vectorized sink: read the Arrow batch and write it via a reused record view
  // that pulls values straight from the Arrow columns, materializing no rows.
  @transient private var columnarAllocator: RootAllocator = _
  override def writeArrowBatch(arrowIpcBytes: Array[Byte]): Unit = {
    if (columnarAllocator == null) columnarAllocator = new RootAllocator()
    val reader = new ArrowStreamReader(new ByteArrayInputStream(arrowIpcBytes), columnarAllocator)
    try {
      val root = reader.getVectorSchemaRoot
      val struct = tableSchema.asStruct()
      while (reader.loadNextBatch()) {
        val n = root.getRowCount
        if (n > 0) {
          // Resolve the table per batch (#7290), same as the row flush path.
          val table: Table = IcebergUtil
            .loadTableMetadata(catalog, tableNamespace, tableName)
            .get
          val view = new IcebergTableWriter.ArrowRecordView(root, struct)
          writeRecordsToNewFile(table, (0 until n).iterator.map { i => view.setRow(i); view })
        }
      }
    } finally reader.close()
  }

  /**
    * Close the writer, ensuring any remaining buffered items are flushed.
    */
  override def close(): Unit = {
    if (buffer.nonEmpty) {
      flushBuffer()
    }
    if (columnarAllocator != null) { columnarAllocator.close(); columnarAllocator = null }
  }
}

object IcebergTableWriter {

  // A single mutable Iceberg Record backed by an Arrow batch and a row index.
  // Only the positional reads the Parquet write path uses (get/get-typed/size)
  // are implemented; mutation and name lookups are unsupported. Each column's
  // value is decoded from Arrow to the Texera type, then mapped to the Iceberg
  // Java type exactly as IcebergUtil.toGenericRecord does.
  private class ArrowRecordView(root: VectorSchemaRoot, structType: StructType) extends Record {
    private val vectors = root.getFieldVectors
    private val texeraTypes = ArrowUtils.toTexeraSchema(root.getSchema).getAttributes.map(_.getType)
    private var rowIndex = 0
    def setRow(i: Int): Unit = rowIndex = i

    private def icebergValue(pos: Int): AnyRef = {
      val raw = vectors.get(pos).getObject(rowIndex)
      val texeraValue =
        try AttributeTypeUtils.parseField(raw, texeraTypes(pos))
        catch { case _: Exception => null }
      texeraValue match {
        case null                        => null
        case ts: java.sql.Timestamp      => ts.toInstant.atZone(ZoneId.systemDefault()).toLocalDateTime
        case bytes: Array[Byte]          => ByteBuffer.wrap(bytes)
        case largeBinaryPtr: LargeBinary => largeBinaryPtr.getUri
        case other                       => other.asInstanceOf[AnyRef]
      }
    }

    override def size(): Int = vectors.size
    override def get(pos: Int): AnyRef = icebergValue(pos)
    override def get[U](pos: Int, javaClass: Class[U]): U = javaClass.cast(icebergValue(pos))
    override def struct(): StructType = structType

    private def unsupported =
      throw new UnsupportedOperationException("ArrowRecordView is read-only")
    override def set[U](pos: Int, value: U): Unit = unsupported
    override def getField(name: String): AnyRef = unsupported
    override def setField(name: String, value: AnyRef): Unit = unsupported
    override def copy(): Record = unsupported
    override def copy(overwriteValues: java.util.Map[String, AnyRef]): Record = unsupported
  }
}
