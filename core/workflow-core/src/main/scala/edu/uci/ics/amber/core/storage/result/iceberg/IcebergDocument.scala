package edu.uci.ics.amber.core.storage.result.iceberg

import edu.uci.ics.amber.core.storage.IcebergCatalogInstance
import edu.uci.ics.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}
import edu.uci.ics.amber.core.storage.util.StorageUtil.{withLock, withReadLock, withWriteLock}
import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.{FileScanTask, Table}
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.data.Record
import org.apache.iceberg.exceptions.NoSuchTableException

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
    val serde: (org.apache.iceberg.Schema, T) => Record,
    val deserde: (org.apache.iceberg.Schema, Record) => T
) extends VirtualDocument[T] {

  private val lock = new ReentrantReadWriteLock()

  @transient lazy val catalog: Catalog = IcebergCatalogInstance.getInstance()

  // During construction, create or override the table
  IcebergUtil.createTable(
    catalog,
    tableNamespace,
    tableName,
    tableSchema,
    overrideIfExists = true
  )

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
  override def get(): Iterator[T] = getUsingFileSequenceOrder(0, None)

  /**
    * Get records within a specified range [from, until).
    */
  override def getRange(from: Int, until: Int): Iterator[T] = {
    getUsingFileSequenceOrder(from, Some(until))
  }

  /**
    * Get records starting after a specified offset.
    */
  override def getAfter(offset: Int): Iterator[T] = {
    getUsingFileSequenceOrder(offset, None)
  }

  /**
    * Get the total count of records in the table.
    */
  override def getCount: Long = {
    val table = IcebergUtil
      .loadTableMetadata(catalog, tableNamespace, tableName)
      .getOrElse(
        return 0
      )
    table.newScan().planFiles().iterator().asScala.map(f => f.file().recordCount()).sum
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

  /**
    * Util iterator to get T in certain range
    * @param from start from which record inclusively, if 0 means start from the first
    * @param until end at which record exclusively, if None means read to the table's EOF
    */
  private def getUsingFileSequenceOrder(from: Int, until: Option[Int]): Iterator[T] =
    withReadLock(lock) {
      new Iterator[T] {
        private val iteLock = new ReentrantLock()
        // Load the table instance, initially the table instance may not exist
        private var table: Option[Table] = loadTableMetadata()

        // Last seen snapshot id(logically it's like a version number). While reading, new snapshots may be created
        private var lastSnapshotId: Option[Long] = None

        // Counter for how many records have been skipped
        private var numOfSkippedRecords = 0

        // Counter for how many records have been returned
        private var numOfReturnedRecords = 0

        // Total number of records to return
        private val totalRecordsToReturn = until.map(_ - from).getOrElse(Int.MaxValue)

        // Iterator for usable file scan tasks
        private var usableFileIterator: Iterator[FileScanTask] = seekToUsableFile()

        // Current record iterator for the active file
        private var currentRecordIterator: Iterator[Record] = Iterator.empty

        // Util function to load the table's metadata
        private def loadTableMetadata(): Option[Table] = {
          IcebergUtil.loadTableMetadata(
            catalog,
            tableNamespace,
            tableName
          )
        }

        private def seekToUsableFile(): Iterator[FileScanTask] =
          withLock(iteLock) {
            if (numOfSkippedRecords > from) {
              throw new RuntimeException("seek operation should not be called")
            }

            // refresh the table's snapshots
            if (table.isEmpty) {
              table = loadTableMetadata()
            }
            table.foreach(_.refresh())

            // Retrieve and sort the file scan tasks by file sequence number
            val fileScanTasksIterator: Iterator[FileScanTask] = table match {
              case Some(t) =>
                val currentSnapshotId = Option(t.currentSnapshot()).map(_.snapshotId())
                val fileScanTasks = (lastSnapshotId, currentSnapshotId) match {
                  // Read from the start
                  case (None, Some(_)) =>
                    val tasks = t.newScan().planFiles().iterator().asScala
                    lastSnapshotId = currentSnapshotId
                    tasks

                  // Read incrementally from the last snapshot
                  case (Some(lastId), Some(currId)) if lastId != currId =>
                    val tasks = t
                      .newIncrementalAppendScan()
                      .fromSnapshotExclusive(lastId)
                      .toSnapshot(currId)
                      .planFiles()
                      .iterator()
                      .asScala
                    lastSnapshotId = currentSnapshotId
                    tasks

                  // No new data
                  case (Some(lastId), Some(currId)) if lastId == currId =>
                    Iterator.empty

                  // Default: No data yet
                  case _ =>
                    Iterator.empty
                }
                fileScanTasks.toSeq.sortBy(_.file().fileSequenceNumber()).iterator

              case None =>
                Iterator.empty
            }

            // Iterate through sorted FileScanTasks and update numOfSkippedRecords
            val usableTasks = fileScanTasksIterator.dropWhile { task =>
              val recordCount = task.file().recordCount()
              if (numOfSkippedRecords + recordCount <= from) {
                numOfSkippedRecords += recordCount.toInt
                true
              } else {
                false
              }
            }

            usableTasks
          }

        override def hasNext: Boolean = {
          if (numOfReturnedRecords >= totalRecordsToReturn) {
            return false
          }

          if (!usableFileIterator.hasNext) {
            usableFileIterator = seekToUsableFile()
          }

          while (!currentRecordIterator.hasNext && usableFileIterator.hasNext) {
            val nextFile = usableFileIterator.next()
            currentRecordIterator = IcebergUtil.readDataFileAsIterator(
              nextFile.file(),
              tableSchema,
              table.get
            )

            // Skip records within the file if necessary
            val recordsToSkipInFile = from - numOfSkippedRecords
            if (recordsToSkipInFile > 0) {
              currentRecordIterator = currentRecordIterator.drop(recordsToSkipInFile)
              numOfSkippedRecords += recordsToSkipInFile
            }
          }

          currentRecordIterator.hasNext
        }

        override def next(): T = {
          if (!hasNext) throw new NoSuchElementException("No more records available")

          val record = currentRecordIterator.next()
          numOfReturnedRecords += 1
          deserde(tableSchema, record)
        }
      }
    }
}
