package edu.uci.ics.amber.core.storage.result.iceberg

import edu.uci.ics.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}
import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.data.{IcebergGenerics, Record}
import org.apache.iceberg.io.CloseableIterable

import java.net.URI
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.jdk.CollectionConverters._

class IcebergDocument[T >: Null <: AnyRef](
    val catalog: Catalog,
    val tableNamespace: String,
    val tableName: String,
    val tableSchema: org.apache.iceberg.Schema,
    val serde: T => Record,
    val deserde: (org.apache.iceberg.Schema, Record) => T
) extends VirtualDocument[T] {

  private val lock = new ReentrantReadWriteLock()

  /**
    * Returns the URI of the table location.
    */
  override def getURI: URI = {
    val table = IcebergUtil.loadOrCreateTable(catalog, tableNamespace, tableName, tableSchema)
    URI.create(table.location())
  }

  /**
    * Deletes the table.
    */
  override def clear(): Unit =
    withWriteLock {
      val identifier = TableIdentifier.of(tableNamespace, tableName)
      if (catalog.tableExists(identifier)) {
        catalog.dropTable(identifier)
      }
    }

  /**
    * Returns an iterator that iterates over all records in the table, including new records
    * from concurrent writers as they commit.
    */
  override def get(): Iterator[T] =
    new Iterator[T] {
      private val table =
        IcebergUtil.loadOrCreateTable(catalog, tableNamespace, tableName, tableSchema)
      private var currentSnapshot = table.currentSnapshot()
      private var recordIterator = loadRecords()

      /**
        * Loads all records from the current snapshot.
        */
      private def loadRecords(): Iterator[T] = {
        if (currentSnapshot != null) {
          try {
            val records: CloseableIterable[Record] = IcebergGenerics.read(table).build()
            records.iterator().asScala.map(record => deserde(tableSchema, record))
          } catch {
            case _: java.io.FileNotFoundException =>
              println("Metadata file not found. Returning an empty iterator.")
              Iterator.empty
            case e: Exception =>
              println(s"Error during record loading: ${e.getMessage}")
              e.printStackTrace()
              Iterator.empty
          }
        } else {
          Iterator.empty
        }
      }

      override def hasNext: Boolean = {
        if (recordIterator.hasNext) {
          true
        } else {
          // Refresh the table and check for new commits
          table.refresh()
          val newSnapshot = table.currentSnapshot()
          if (newSnapshot != currentSnapshot) {
            currentSnapshot = newSnapshot
            recordIterator = loadRecords()
            recordIterator.hasNext
          } else {
            false
          }
        }
      }

      override def next(): T = recordIterator.next()
    }

  /**
    * Returns a BufferedItemWriter for writing data to the table.
    */
  override def writer(): BufferedItemWriter[T] = {
    new IcebergTableWriter[T](catalog, tableNamespace, tableName, tableSchema, serde)
  }

  // Utility function to wrap code block with write lock
  private def withWriteLock[M](block: => M): M = {
    lock.writeLock().lock()
    try block
    finally lock.writeLock().unlock()
  }
}
