package edu.uci.ics.amber.core.storage.result.iceberg

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.storage.model.BufferedItemWriter
import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.{Schema, Table}
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.data.Record
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.io.{DataWriter, OutputFile}
import org.apache.iceberg.parquet.Parquet

import java.nio.file.Paths
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
  * @param catalog the Iceberg catalog to manage table metadata.
  * @param tableNamespace the namespace of the Iceberg table.
  * @param tableName the name of the Iceberg table.
  * @param tableSchema the schema of the Iceberg table.
  * @param serde a function to serialize `T` into an Iceberg `Record`.
  * @tparam T the type of the data items written to the table.
  */
class IcebergTableWriter[T](
    val writerIdentifier: String,
    val catalog: Catalog,
    val tableNamespace: String,
    val tableName: String,
    val tableSchema: Schema,
    val serde: (org.apache.iceberg.Schema, T) => Record
) extends BufferedItemWriter[T] {

  // Buffer to hold items before flushing to the table
  private val buffer = new ArrayBuffer[T]()
  // Incremental filename index, incremented each time a new buffer is flushed
  private var filenameIdx = 0
  // Incremental record ID, incremented for each record
  private var recordId = 0

  override val bufferSize: Int = StorageConfig.icebergTableCommitBatchSize

  // Load the Iceberg table
  private val table: Table =
    IcebergUtil
      .loadTableMetadata(catalog, tableNamespace, tableName)
      .get

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
      // Create a unique file path using the writer's identifier and the filename index
      val filepath = Paths.get(table.location()).resolve(s"${writerIdentifier}_${filenameIdx}")
      // Increment the filename index by 1
      filenameIdx += 1
      val outputFile: OutputFile = table.io().newOutputFile(filepath.toString)
      // Create a Parquet data writer to write a new file
      val dataWriter: DataWriter[Record] = Parquet
        .writeData(outputFile)
        .forTable(table)
        .createWriterFunc(GenericParquetWriter.buildWriter)
        .overwrite()
        .build()
      // Write each buffered item to the data file
      try {
        buffer.foreach { item =>
          dataWriter.write(serde(tableSchema, item))
        }
      } finally {
        dataWriter.close()
      }
      // Commit the new file to the table
      val dataFile = dataWriter.toDataFile
      table.newAppend().appendFile(dataFile).commit()
      // Clear the item buffer
      buffer.clear()
    }
  }

  /**
    * Close the writer, ensuring any remaining buffered items are flushed.
    */
  override def close(): Unit = {
    if (buffer.nonEmpty) {
      flushBuffer()
    }
  }
}
