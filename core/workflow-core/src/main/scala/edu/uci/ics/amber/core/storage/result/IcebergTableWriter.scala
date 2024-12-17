package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.storage.model.BufferedItemWriter
import org.apache.iceberg.{CatalogUtil, DataFile, Table}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.io.{DataWriter, OutputFile}
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.data.GenericRecord

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class IcebergTableWriter[T](
                             val tableName: String,
                             val tableSchema: org.apache.iceberg.Schema,
                             val convertToGenericRecord: T => GenericRecord
                           ) extends BufferedItemWriter[T] {

  private val lock = new ReentrantLock()
  private val buffer = new ArrayBuffer[T]()
  override val bufferSize: Int = 1024

  // Load the Iceberg table
  private val table: Table = loadOrCreateTable()

  private def loadOrCreateTable(): Table = {
    val catalog = CatalogUtil.loadCatalog(
      classOf[org.apache.iceberg.jdbc.JdbcCatalog].getName,
      "iceberg",
      Map(
        "uri" -> StorageConfig.jdbcUrl,
        "warehouse" -> StorageConfig.fileStorageDirectoryUri.toString,
        "jdbc.user" -> StorageConfig.jdbcUsername,
        "jdbc.password" -> StorageConfig.jdbcPassword
      ).asJava,
      null
    )

    val identifier = TableIdentifier.of(Namespace.of("default"), tableName)
    if (!catalog.tableExists(identifier)) {
      catalog.createTable(identifier, tableSchema)
    } else {
      catalog.loadTable(identifier)
    }
  }

  override def open(): Unit = withLock {
    buffer.clear()
  }

  override def putOne(item: T): Unit = withLock {
    buffer.append(item)
    if (buffer.size >= bufferSize) {
      flushBuffer()
    }
  }

  override def removeOne(item: T): Unit = withLock {
    buffer -= item
  }

  private def flushBuffer(): Unit = withLock {
    if (buffer.nonEmpty) {
      try {
        // Create a unique file path using UUID
        val filepath = s"${table.location()}/${UUID.randomUUID().toString}"
        val outputFile: OutputFile = table.io().newOutputFile(filepath)

        // Create a Parquet data writer
        val dataWriter: DataWriter[GenericRecord] = Parquet.writeData(outputFile)
          .schema(table.schema())
          .createWriterFunc(org.apache.iceberg.data.parquet.GenericParquetWriter.buildWriter)
          .overwrite()
          .build()

        try {
          buffer.foreach { item =>
            val record = convertToGenericRecord(item)
            dataWriter.write(record)
          }
        } finally {
          dataWriter.close()
        }

        // Commit the new file to the table
        val dataFile: DataFile = dataWriter.toDataFile
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

  override def close(): Unit = withLock {
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