package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}
import org.apache.iceberg.{CatalogUtil, DataFile, Table}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.io.{DataTask, OutputFileFactory}
import org.apache.iceberg.parquet.Parquet

import java.net.URI
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Using

class IcebergFileDocument[T](
                              val tableName: String,
                              val tableLocation: String,
                              val serializer: T => Array[Byte],
                              val deserializer: Array[Byte] => T
                            ) extends VirtualDocument[T]
  with BufferedItemWriter[T] {

  private val lock = new ReentrantReadWriteLock()
  private val buffer = new ArrayBuffer[T]()
  override val bufferSize: Int = 1024

  // Iceberg table reference
  private val table: Table = loadOrCreateTable()

  // Utility function to wrap code block with read lock
  private def withReadLock[M](block: => M): M = {
    lock.readLock().lock()
    try block
    finally lock.readLock().unlock()
  }

  // Utility function to wrap code block with write lock
  private def withWriteLock[M](block: => M): M = {
    lock.writeLock().lock()
    try block
    finally lock.writeLock().unlock()
  }

  private def loadOrCreateTable(): Table = {
    val catalog = CatalogUtil.loadCatalog("rest", "iceberg", Map("uri" -> tableLocation))
    val identifier = TableIdentifier.of(Namespace.of("default"), tableName)
    if (!catalog.tableExists(identifier)) {
      catalog.createTable(identifier, org.apache.iceberg.Schema.empty())
    } else {
      catalog.loadTable(identifier)
    }
  }

  override def getURI: URI = new URI(tableLocation)

  override def open(): Unit = withWriteLock {
    buffer.clear()
  }

  override def putOne(item: T): Unit = withWriteLock {
    buffer.append(item)
    if (buffer.size >= bufferSize) {
      flushBuffer()
    }
  }

  override def removeOne(item: T): Unit = withWriteLock {
    buffer -= item
  }

  private def flushBuffer(): Unit = withWriteLock {
    if (buffer.nonEmpty) {
      val outputFileFactory = OutputFileFactory.builderFor(table, 0, 0).build()
      val outputFile = outputFileFactory.newOutputFile()
      Using(Parquet.writeData(outputFile).createWriterFunc(serializer).build()) { writer =>
        buffer.foreach(writer.write)
      }
      table.refresh() // Refresh table to commit new data
      buffer.clear()
    }
  }

  override def close(): Unit = withWriteLock {
    if (buffer.nonEmpty) {
      flushBuffer()
    }
  }

  override def clear(): Unit = withWriteLock {
    table.newDelete().deleteFromRowFilter(null).commit()
  }

  override def get(): Iterator[T] = withReadLock {
    table.currentSnapshot() match {
      case null => Iterator.empty
      case snapshot =>
        table.newScan().planFiles().iterator().flatMap { task =>
          val file = task.file()
          val reader = Parquet.read(file.path().toString).createReaderFunc(deserializer).build()
          reader.iterator().asScala
        }
    }
  }
}