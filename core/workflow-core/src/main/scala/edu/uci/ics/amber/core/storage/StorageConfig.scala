package edu.uci.ics.amber.core.storage

import edu.uci.ics.amber.util.PathUtils.corePath
import org.yaml.snakeyaml.Yaml

import java.nio.file.Path
import java.util.{Map => JMap}
import scala.jdk.CollectionConverters._

object StorageConfig {
  private val conf: Map[String, Any] = {
    val yaml = new Yaml()
    val inputStream = getClass.getClassLoader.getResourceAsStream("storage-config.yaml")
    val javaConf = yaml.load(inputStream).asInstanceOf[JMap[String, Any]].asScala.toMap

    val storageMap = javaConf("storage").asInstanceOf[JMap[String, Any]].asScala.toMap
    val mongodbMap = storageMap("mongodb").asInstanceOf[JMap[String, Any]].asScala.toMap
    val icebergMap = storageMap("iceberg").asInstanceOf[JMap[String, Any]].asScala.toMap
    val icebergTableMap = icebergMap("table").asInstanceOf[JMap[String, Any]].asScala.toMap
    val icebergCommitMap = icebergTableMap("commit").asInstanceOf[JMap[String, Any]].asScala.toMap
    val icebergRetryMap = icebergCommitMap("retry").asInstanceOf[JMap[String, Any]].asScala.toMap
    val jdbcMap = storageMap("jdbc").asInstanceOf[JMap[String, Any]].asScala.toMap

    javaConf.updated(
      "storage",
      storageMap
        .updated("mongodb", mongodbMap)
        .updated(
          "iceberg",
          icebergMap
            .updated(
              "table",
              icebergTableMap.updated(
                "commit",
                icebergCommitMap.updated("retry", icebergRetryMap)
              )
            )
        )
        .updated("jdbc", jdbcMap)
    )
  }

  // Result storage mode
  val resultStorageMode: String =
    conf("storage").asInstanceOf[Map[String, Any]]("result-storage-mode").asInstanceOf[String]

  // MongoDB configurations
  val mongodbUrl: String = conf("storage")
    .asInstanceOf[Map[String, Any]]("mongodb")
    .asInstanceOf[Map[String, Any]]("url")
    .asInstanceOf[String]

  val mongodbDatabaseName: String = conf("storage")
    .asInstanceOf[Map[String, Any]]("mongodb")
    .asInstanceOf[Map[String, Any]]("database")
    .asInstanceOf[String]

  val mongodbBatchSize: Int = conf("storage")
    .asInstanceOf[Map[String, Any]]("mongodb")
    .asInstanceOf[Map[String, Any]]("commit-batch-size")
    .asInstanceOf[Int]

  val icebergTableNamespace: String = conf("storage")
    .asInstanceOf[Map[String, Any]]("iceberg")
    .asInstanceOf[Map[String, Any]]("table")
    .asInstanceOf[Map[String, Any]]("namespace")
    .asInstanceOf[String]

  val icebergTableCommitBatchSize: Int = conf("storage")
    .asInstanceOf[Map[String, Any]]("iceberg")
    .asInstanceOf[Map[String, Any]]("table")
    .asInstanceOf[Map[String, Any]]("commit")
    .asInstanceOf[Map[String, Any]]("batch-size")
    .asInstanceOf[Int]

  val icebergTableCommitNumRetries: Int = conf("storage")
    .asInstanceOf[Map[String, Any]]("iceberg")
    .asInstanceOf[Map[String, Any]]("table")
    .asInstanceOf[Map[String, Any]]("commit")
    .asInstanceOf[Map[String, Any]]("retry")
    .asInstanceOf[Map[String, Any]]("num-retries")
    .asInstanceOf[Int]

  val icebergTableCommitMinRetryWaitMs: Int = conf("storage")
    .asInstanceOf[Map[String, Any]]("iceberg")
    .asInstanceOf[Map[String, Any]]("table")
    .asInstanceOf[Map[String, Any]]("commit")
    .asInstanceOf[Map[String, Any]]("retry")
    .asInstanceOf[Map[String, Any]]("min-wait-ms")
    .asInstanceOf[Int]

  val icebergTableCommitMaxRetryWaitMs: Int = conf("storage")
    .asInstanceOf[Map[String, Any]]("iceberg")
    .asInstanceOf[Map[String, Any]]("table")
    .asInstanceOf[Map[String, Any]]("commit")
    .asInstanceOf[Map[String, Any]]("retry")
    .asInstanceOf[Map[String, Any]]("max-wait-ms")
    .asInstanceOf[Int]

  // JDBC configurations
  val jdbcUrl: String = conf("storage")
    .asInstanceOf[Map[String, Any]]("jdbc")
    .asInstanceOf[Map[String, Any]]("url")
    .asInstanceOf[String]

  val jdbcUsername: String = conf("storage")
    .asInstanceOf[Map[String, Any]]("jdbc")
    .asInstanceOf[Map[String, Any]]("username")
    .asInstanceOf[String]

  val jdbcPassword: String = conf("storage")
    .asInstanceOf[Map[String, Any]]("jdbc")
    .asInstanceOf[Map[String, Any]]("password")
    .asInstanceOf[String]

  // File storage configurations
  val fileStorageDirectoryPath: Path =
    corePath.resolve("amber").resolve("user-resources").resolve("workflow-results")
}
