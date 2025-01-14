package edu.uci.ics.amber.core.storage

import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.amber.util.PathUtils.corePath

import java.nio.file.Path

object StorageConfig {

  // Load configuration
  private val conf: Config = ConfigFactory.parseResources("storage.conf").resolve()

  // General storage settings
  val resultStorageMode: String = conf.getString("storage.result-storage-mode")

  // MongoDB specifics
  val mongodbUrl: String = conf.getString("storage.mongodb.url")
  val mongodbDatabaseName: String = conf.getString("storage.mongodb.database")
  val mongodbBatchSize: Int = conf.getInt("storage.mongodb.commit-batch-size")

  // JDBC specifics
  val jdbcUrl: String = conf.getString("storage.jdbc.url")
  val jdbcUsername: String = conf.getString("storage.jdbc.username")
  val jdbcPassword: String = conf.getString("storage.jdbc.password")

  // Iceberg specifics
  val icebergTableNamespace: String = conf.getString("storage.iceberg.table.namespace")
  val icebergTableCommitBatchSize: Int = conf.getInt("storage.iceberg.table.commit.batch-size")
  val icebergTableCommitNumRetries: Int =
    conf.getInt("storage.iceberg.table.commit.retry.num-retries")
  val icebergTableCommitMinRetryWaitMs: Int =
    conf.getInt("storage.iceberg.table.commit.retry.min-wait-ms")
  val icebergTableCommitMaxRetryWaitMs: Int =
    conf.getInt("storage.iceberg.table.commit.retry.max-wait-ms")

  // File storage configurations
  val fileStorageDirectoryPath: Path =
    corePath.resolve("amber").resolve("user-resources").resolve("workflow-results")
}
