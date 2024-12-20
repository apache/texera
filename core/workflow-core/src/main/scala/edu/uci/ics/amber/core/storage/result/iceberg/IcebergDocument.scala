package edu.uci.ics.amber.core.storage.result.iceberg

import edu.uci.ics.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}
import edu.uci.ics.amber.core.storage.util.StorageUtil.{withLock, withReadLock, withWriteLock}
import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.{Snapshot, Table}
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.data.{IcebergGenerics, Record}
import org.apache.iceberg.exceptions.NoSuchTableException
import org.apache.iceberg.io.CloseableIterable

import java.net.URI
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
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
  synchronized {
    IcebergUtil
      .loadTable(catalog, tableNamespace, tableName, tableSchema, createIfNotExist = true)
      .get
  }

  /**
    * Returns the URI of the table location.
    */
  override def getURI: URI = {
    val table = IcebergUtil
      .loadTable(catalog, tableNamespace, tableName, tableSchema, createIfNotExist = false)
      .getOrElse(
        throw new NoSuchTableException(f"table ${tableNamespace}.${tableName} doesn't exist")
      )
    URI.create(table.location())
  }

  /**
    * Deletes the table.
    */
  override def clear(): Unit =
    withWriteLock(lock) {
      val identifier = TableIdentifier.of(tableNamespace, tableName)
      if (catalog.tableExists(identifier)) {
        catalog.dropTable(identifier)
      }
    }

  override def get(): Iterator[T] =
    withReadLock(lock) {
      new Iterator[T] {
        private val iteLock = new ReentrantLock()
        private var table: Option[Table] = loadTable()
        private var lastSnapshotId: Option[Long] = None
        private var recordIterator: Iterator[T] = loadRecords()

        private def loadTable(): Option[Table] = {
          IcebergUtil.loadTable(
            catalog,
            tableNamespace,
            tableName,
            tableSchema,
            createIfNotExist = false
          )
        }

        /**
          * Loads records incrementally using `newIncrementalAppendScan` from the last snapshot ID.
          */
        private def loadRecords(): Iterator[T] =
          withLock(iteLock) {
            table match {
              case Some(t) =>
                val currentSnapshot = Option(t.currentSnapshot())
                val currentSnapshotId = currentSnapshot.map(_.snapshotId())

                val records: CloseableIterable[Record] = (lastSnapshotId, currentSnapshotId) match {
                  case (Some(lastId), Some(currId)) if lastId != currId =>
                    // Perform incremental append scan if snapshot IDs are different
                    IcebergGenerics.read(t).appendsAfter(lastId).build()

                  case (None, Some(_)) =>
                    // First read, perform a full scan
                    IcebergGenerics.read(t).build()

                  case _ =>
                    // No new data; return an empty iterator
                    CloseableIterable.empty()
                }

                // Update the last snapshot ID to the current one
                lastSnapshotId = currentSnapshotId
                records.iterator().asScala.map(record => deserde(tableSchema, record))

              case _ => Iterator.empty
            }
          }

        override def hasNext: Boolean = {
          if (recordIterator.hasNext) {
            true
          } else {
            // Refresh the table and check for new commits
            if (table.isEmpty) {
              table = loadTable()
            }
            table.foreach(_.refresh())
            recordIterator = loadRecords()
            recordIterator.hasNext

          }
        }

        override def next(): T = {
          if (!hasNext) throw new NoSuchElementException("No more records available")
          recordIterator.next()
        }
      }
    }

  /**
    * Returns a BufferedItemWriter for writing data to the table.
    */
  override def writer(): BufferedItemWriter[T] = {
    new IcebergTableWriter[T](catalog, tableNamespace, tableName, tableSchema, serde)
  }
}
