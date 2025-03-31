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
  val icebergCatalogType: String = conf.getString("storage.iceberg.catalog.type")
  val icebergRESTCatalogUri: String = conf.getString("storage.iceberg.catalog.rest-uri")

  // Iceberg Postgres specifics
  val icebergPostgresCatalogUriWithoutScheme: String =
    conf.getString("storage.iceberg.catalog.postgres.uri-without-scheme")
  val icebergPostgresCatalogUsername: String =
    conf.getString("storage.iceberg.catalog.postgres.username")
  val icebergPostgresCatalogPassword: String =
    conf.getString("storage.iceberg.catalog.postgres.password")

  // Iceberg Table specifics
  val icebergTableResultNamespace: String = conf.getString("storage.iceberg.table.result-namespace")
  val icebergTableConsoleMessagesNamespace: String =
    conf.getString("storage.iceberg.table.console-messages-namespace")
  val icebergTableRuntimeStatisticsNamespace: String =
    conf.getString("storage.iceberg.table.runtime-statistics-namespace")
  val icebergTableCommitBatchSize: Int =
    conf.getInt("storage.iceberg.table.commit.batch-size")
  val icebergTableCommitNumRetries: Int =
    conf.getInt("storage.iceberg.table.commit.retry.num-retries")
  val icebergTableCommitMinRetryWaitMs: Int =
    conf.getInt("storage.iceberg.table.commit.retry.min-wait-ms")
  val icebergTableCommitMaxRetryWaitMs: Int =
    conf.getInt("storage.iceberg.table.commit.retry.max-wait-ms")

  // LakeFS specifics
  val lakefsEndpoint: String = conf.getString("storage.lakefs.endpoint")
  val lakefsApiSecret: String = conf.getString("storage.lakefs.auth.api-secret")
  val lakefsUsername: String = conf.getString("storage.lakefs.auth.username")
  val lakefsPassword: String = conf.getString("storage.lakefs.auth.password")
  val lakefsBlockStorageType: String = conf.getString("storage.lakefs.block-storage.type")
  val lakefsBucketName: String = conf.getString("storage.lakefs.block-storage.bucket-name")

  // S3 specifics
  val s3Endpoint: String = conf.getString("storage.s3.endpoint")
  val s3Region: String = conf.getString("storage.s3.region")
  val s3Username: String = conf.getString("storage.s3.auth.username")
  val s3Password: String = conf.getString("storage.s3.auth.password")

  // File storage configurations
  val fileStorageDirectoryPath: Path =
    corePath.resolve("amber").resolve("user-resources").resolve("workflow-results")
}
