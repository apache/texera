package edu.uci.ics.amber.core.storage.result.iceberg

import edu.uci.ics.amber.core.storage.IcebergCatalog
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

/**
  * IcebergDocument is used to read and write a set of T as an Iceberg table.
  * It provides iterator-based read methods and supports multiple writers to write to the same table.
  *
  * - On construction, the table will be created if it does not exist.
  * - If the table exists, it will be overridden.
  *
  * @param tableNamespace namespace of the table.
  * @param tableName name of the table.
  * @param tableSchema schema of the table.
  * @param serde function to serialize T into an Iceberg Record.
  * @param deserde function to deserialize an Iceberg Record into T.
  * @tparam T type of the data items stored in the Iceberg table.
  */
class IcebergDocument[T >: Null <: AnyRef](
    val tableNamespace: String,
    val tableName: String,
    val tableSchema: org.apache.iceberg.Schema,
    val serde: T => Record,
    val deserde: (org.apache.iceberg.Schema, Record) => T
) extends VirtualDocument[T] {

  private val lock = new ReentrantReadWriteLock()

  @transient lazy val catalog: Catalog = IcebergCatalog.getInstance()

  // During construction, create or override the table
  synchronized {
    IcebergUtil.createTable(
      catalog,
      tableNamespace,
      tableName,
      tableSchema,
      overrideIfExists = true
    )
  }

  /**
    * Returns the URI of the table location.
    * @throws NoSuchTableException if the table does not exist.
    */
  override def getURI: URI = {
    val table = IcebergUtil
      .loadTableMetadata(catalog, tableNamespace, tableName)
      .getOrElse(
        throw new NoSuchTableException(f"table ${tableNamespace}.${tableName} doesn't exist")
      )
    URI.create(table.location())
  }

  /**
    * Deletes the table and clears its contents.
    */
  override def clear(): Unit =
    withWriteLock(lock) {
      val identifier = TableIdentifier.of(tableNamespace, tableName)
      if (catalog.tableExists(identifier)) {
        catalog.dropTable(identifier)
      }
    }

  /**
    * Get an iterator for reading records from the table.
    */
  override def get(): Iterator[T] =
    withReadLock(lock) {
      new Iterator[T] {
        private val iteLock = new ReentrantLock()
        // Load the table instance, initially the table instance may not exists
        private var table: Option[Table] = loadTableMetadata()

        // Last seen snapshot id(logically it's like a version number). While reading, new snapshots may be created
        private var lastSnapshotId: Option[Long] = None

        // Iterator for the records
        private var recordIterator: Iterator[T] = loadRecords()

        // Util function to load the table's metadata
        private def loadTableMetadata(): Option[Table] = {
          IcebergUtil.loadTableMetadata(
            catalog,
            tableNamespace,
            tableName
          )
        }

        // Util function to load new records when current iterator reach to EOF
        private def loadRecords(): Iterator[T] =
          withLock(iteLock) {
            table match {
              case Some(t) =>
                val currentSnapshotId = Option(t.currentSnapshot()).map(_.snapshotId())

                val records: CloseableIterable[Record] = (lastSnapshotId, currentSnapshotId) match {
                  // case1: the read hasn't started yet(because the lastSnapshotId is None)
                  // - create a iterator that will read from the beginning of the table
                  case (None, Some(_)) =>
                    IcebergGenerics.read(t).build()

                  // case2: the read is ongoing and two Ids are not equal
                  case (Some(lastId), Some(currId)) if lastId != currId =>
                    // This means that the new snapshots have been produced since last read, thus
                    //   create a iterator that only reads the new data
                    IcebergGenerics.read(t).appendsAfter(lastId).build()

                  // case3: the read is ongoing and two Ids are equal
                  case (Some(lastId), Some(currId)) if lastId == currId =>
                    // This means that there is no new data during the read, thus no new record
                    CloseableIterable.empty()

                  // default case: Both Ids are None, meaning no data yet.
                  case _ =>
                    CloseableIterable.empty()
                }

                lastSnapshotId = currentSnapshotId
                records.iterator().asScala.map(record => deserde(tableSchema, record))

              case _ => Iterator.empty
            }
          }

        override def hasNext: Boolean = {
          if (recordIterator.hasNext) {
            true
          } else {
            // Refresh table and check for new data
            if (table.isEmpty) {
              table = loadTableMetadata()
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
    * Get records within a specified range [from, until).
    */
  override def getRange(from: Int, until: Int): Iterator[T] = {
    get().slice(from, until)
  }

  /**
    * Get records starting after a specified offset.
    */
  override def getAfter(offset: Int): Iterator[T] = {
    get().drop(offset + 1)
  }

  /**
    * Get the total count of records in the table.
    */
  override def getCount: Long = {
    get().length
  }

  /**
    * Creates a BufferedItemWriter for writing data to the table.
    * @param writerIdentifier The writer's ID. It should be unique within the same table, as each writer will use it as
    *                         the prefix of the files they append
    */
  override def writer(writerIdentifier: String): BufferedItemWriter[T] = {
    new IcebergTableWriter[T](
      writerIdentifier,
      catalog,
      tableNamespace,
      tableName,
      tableSchema,
      serde
    )
  }
}
