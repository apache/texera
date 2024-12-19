package edu.uci.ics.amber.core.storage.result.iceberg

import edu.uci.ics.amber.core.storage.model.BufferedItemWriter
import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.{Schema, Table}
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.data.Record
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.io.{DataWriter, OutputFile}
import org.apache.iceberg.parquet.Parquet

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ArrayBuffer

class IcebergTableWriter[T](
    val catalog: Catalog,
    val tableNamespace: String,
    val tableName: String,
    val tableSchema: Schema,
    val serde: T => Record
) extends BufferedItemWriter[T] {

  private val lock = new ReentrantLock()
  private val buffer = new ArrayBuffer[T]()
  override val bufferSize: Int = 1024

  // Load the Iceberg table
  private val table: Table =
    IcebergUtil.loadOrCreateTable(catalog, tableNamespace, tableName, tableSchema)

  override def open(): Unit =
    withLock {
      buffer.clear()
    }

  override def putOne(item: T): Unit =
    withLock {
      buffer.append(item)
      if (buffer.size >= bufferSize) {
        flushBuffer()
      }
    }

  override def removeOne(item: T): Unit =
    withLock {
      buffer -= item
    }

  private def flushBuffer(): Unit =
    withLock {
      if (buffer.nonEmpty) {
        try {
          // Create a unique file path using UUID
          val filepath = s"${table.location()}/${UUID.randomUUID().toString}"
          val outputFile: OutputFile = table.io().newOutputFile(filepath)

          // Create a Parquet data writer
          val dataWriter: DataWriter[Record] = Parquet
            .writeData(outputFile)
            .forTable(table)
            .createWriterFunc(GenericParquetWriter.buildWriter)
            .overwrite()
            .build()

          try {
            buffer.foreach { item =>
              val record = serde(item)
              dataWriter.write(record)
            }
          } finally {
            dataWriter.close()
          }

          // Commit the new file to the table
          val dataFile = dataWriter.toDataFile
          table.newAppend().appendFile(dataFile).commit()

          println(s"Flushed ${buffer.size} records to ${filepath}")

          buffer.clear()
        } catch {
          case e: Exception =>
            println(s"Error during flush: ${e.getMessage}")
            e.printStackTrace()
        }
      }
    }

  override def close(): Unit =
    withLock {
      if (buffer.nonEmpty) {
        flushBuffer()
      }
    }

  // Utility function to wrap code block with write lock
  private def withLock[M](block: => M): M = {
    lock.lock()
    try block
    finally lock.unlock()
  }
}
