package edu.uci.ics.amber.core.storage.result.iceberg

import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.exceptions.NoSuchTableException

trait OnIceberg {
  def catalog: org.apache.iceberg.catalog.Catalog
  def tableNamespace: String
  def tableName: String

  /**
    * Expire snapshots for the table.
    */
  def expireSnapshots(): Unit = {
    val table = IcebergUtil
      .loadTableMetadata(catalog, tableNamespace, tableName)
      .getOrElse(throw new NoSuchTableException(s"table $tableNamespace.$tableName doesn't exist"))

    // Begin the snapshot expiration process:
    table
      .expireSnapshots() // Initiate snapshot expiration.
      .retainLast(1) // Retain only the most recent snapshot.
      .expireOlderThan(
        System.currentTimeMillis()
      ) // Expire all snapshots older than the current time.
      .cleanExpiredFiles(true) // Remove the files associated with expired snapshots.
      .commit() // Commit the changes to make expiration effective.
  }
}
