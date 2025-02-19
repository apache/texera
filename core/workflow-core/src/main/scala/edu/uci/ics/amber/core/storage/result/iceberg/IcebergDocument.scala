package edu.uci.ics.amber.core.storage.result.iceberg

import edu.uci.ics.amber.core.storage.IcebergCatalogInstance
import edu.uci.ics.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}
import edu.uci.ics.amber.core.storage.util.StorageUtil.{withLock, withReadLock, withWriteLock}
import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.{FileScanTask, Table}
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.data.Record
import org.apache.iceberg.exceptions.NoSuchTableException
import org.apache.iceberg.types.{Conversions, Types}

import java.net.URI
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import scala.jdk.CollectionConverters._
import java.nio.ByteBuffer
import java.time.{Instant, LocalDate, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.collection.mutable

/**
  * IcebergDocument is used to read and write a set of T as an Iceberg table.
  * It provides iterator-based read methods and supports multiple writers to write to the same table.
  *
  * The table must exist when constructing the document
  *
  * @param tableNamespace namespace of the table.
  * @param tableName name of the table.
  * @param tableSchema schema of the table.
  * @param serde function to serialize T into an Iceberg Record.
  * @param deserde function to deserialize an Iceberg Record into T.
  * @tparam T type of the data items stored in the Iceberg table.
  */
private[storage] class IcebergDocument[T >: Null <: AnyRef](
    val tableNamespace: String,
    val tableName: String,
    val tableSchema: org.apache.iceberg.Schema,
    val serde: (org.apache.iceberg.Schema, T) => Record,
    val deserde: (org.apache.iceberg.Schema, Record) => T
) extends VirtualDocument[T] {

  private val lock = new ReentrantReadWriteLock()

  @transient lazy val catalog: Catalog = IcebergCatalogInstance.getInstance()

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

  /**
    * Provides methods for extracting metadata statistics for the results
    *
    * **Statistics Computed:**
    * - **Numeric fields (Int, Long, Double)**: Computes `min` and `max`.
    * - **Date fields (Timestamp)**: Computes `min` and `max` (converted to `LocalDate`).
    * - **All fields**: Computes `not_null_count` (number of non-null values).
    *
    * @return A map where each field name is mapped to a nested map containing its statistics.
    * @throws NoSuchTableException if the table does not exist in the catalog.
    */
  override def getTableStatistics: Map[String, Map[String, Any]] = {
    val table = IcebergUtil
      .loadTableMetadata(catalog, tableNamespace, tableName)
      .getOrElse(
        throw new NoSuchTableException(f"table ${tableNamespace}.${tableName} doesn't exist")
      )

    val schema = table.schema()

    // Extract field names, IDs, and types from the schema
    val fieldTypes =
      schema.columns().asScala.map(col => col.name() -> (col.fieldId(), col.`type`())).toMap
    val fieldStats = mutable.Map[String, mutable.Map[String, Any]]()
    val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE

    // Initialize statistics for each field
    fieldTypes.foreach {
      case (field, (_, fieldType)) =>
        val initialStats = mutable.Map[String, Any](
          "not_null_count" -> 0L
        )
        if (
          fieldType == Types.IntegerType.get() || fieldType == Types.LongType
            .get() || fieldType == Types.DoubleType.get()
        ) {
          initialStats("min") = Double.MaxValue
          initialStats("max") = Double.MinValue
        } else if (
          fieldType == Types.TimestampType.withoutZone() || fieldType == Types.TimestampType
            .withZone()
        ) {
          initialStats("min") = LocalDate.MAX.format(dateFormatter)
          initialStats("max") = LocalDate.MIN.format(dateFormatter)
        }

        fieldStats(field) = initialStats
    }

    // Scan table files and aggregate statistics
    table.newScan().includeColumnStats().planFiles().iterator().asScala.foreach { file =>
      val fileStats = file.file()
      // Extract column-level statistics
      val lowerBounds =
        Option(fileStats.lowerBounds()).getOrElse(Map.empty[Integer, ByteBuffer].asJava)
      val upperBounds =
        Option(fileStats.upperBounds()).getOrElse(Map.empty[Integer, ByteBuffer].asJava)
      val nullCounts =
        Option(fileStats.nullValueCounts()).getOrElse(Map.empty[Integer, java.lang.Long].asJava)
      val nanCounts =
        Option(fileStats.nanValueCounts()).getOrElse(Map.empty[Integer, java.lang.Long].asJava)

      fieldTypes.foreach {
        case (field, (fieldId, fieldType)) =>
          val lowerBound = Option(lowerBounds.get(fieldId))
          val upperBound = Option(upperBounds.get(fieldId))
          val nullCount: Long = Option(nullCounts.get(fieldId)).map(_.toLong).getOrElse(0L)
          val nanCount: Long = Option(nanCounts.get(fieldId)).map(_.toLong).getOrElse(0L)
          val fieldStat = fieldStats(field)

          // Process min/max values for numerical types
          if (
            fieldType == Types.IntegerType.get() || fieldType == Types.LongType
              .get() || fieldType == Types.DoubleType.get()
          ) {
            lowerBound.foreach { buffer =>
              val minValue =
                Conversions.fromByteBuffer(fieldType, buffer).asInstanceOf[Number].doubleValue()
              fieldStat("min") = Math.min(fieldStat("min").asInstanceOf[Double], minValue)
            }

            upperBound.foreach { buffer =>
              val maxValue =
                Conversions.fromByteBuffer(fieldType, buffer).asInstanceOf[Number].doubleValue()
              fieldStat("max") = Math.max(fieldStat("max").asInstanceOf[Double], maxValue)
            }
          }
          // Process min/max values for timestamp types
          else if (
            fieldType == Types.TimestampType.withoutZone() || fieldType == Types.TimestampType
              .withZone()
          ) {
            lowerBound.foreach { buffer =>
              val epochMicros = Conversions
                .fromByteBuffer(Types.TimestampType.withoutZone(), buffer)
                .asInstanceOf[Long]
              val dateValue =
                Instant.ofEpochMilli(epochMicros / 1000).atZone(ZoneOffset.UTC).toLocalDate
              fieldStat("min") =
                if (
                  dateValue
                    .isBefore(LocalDate.parse(fieldStat("min").asInstanceOf[String], dateFormatter))
                )
                  dateValue.format(dateFormatter)
                else
                  fieldStat("min")
            }

            upperBound.foreach { buffer =>
              val epochMicros = Conversions
                .fromByteBuffer(Types.TimestampType.withoutZone(), buffer)
                .asInstanceOf[Long]
              val dateValue =
                Instant.ofEpochMilli(epochMicros / 1000).atZone(ZoneOffset.UTC).toLocalDate
              fieldStat("max") =
                if (
                  dateValue
                    .isAfter(LocalDate.parse(fieldStat("max").asInstanceOf[String], dateFormatter))
                )
                  dateValue.format(dateFormatter)
                else
                  fieldStat("max")
            }
          }
          // Update non-null count
          fieldStat("not_null_count") = fieldStat("not_null_count").asInstanceOf[Long] +
            (fileStats.recordCount().toLong - nullCount - nanCount)
      }
    }
    fieldStats.map {
      case (field, stats) =>
        field -> stats.toMap
    }.toMap
  }
}
