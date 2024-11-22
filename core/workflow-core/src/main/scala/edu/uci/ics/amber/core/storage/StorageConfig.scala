package edu.uci.ics.amber.core.storage

import org.yaml.snakeyaml.Yaml

import java.util.{Map => JMap}
import scala.jdk.CollectionConverters._

object StorageConfig {
  private val conf: Map[String, Any] = {
    val yaml = new Yaml()
    val inputStream = getClass.getClassLoader.getResourceAsStream("storage-config.yaml")
    val javaConf = yaml.load(inputStream).asInstanceOf[JMap[String, Any]].asScala.toMap

    val storageMap = javaConf("storage").asInstanceOf[JMap[String, Any]].asScala.toMap
    val mongodbMap = storageMap("mongodb").asInstanceOf[JMap[String, Any]].asScala.toMap
    val jdbcMap = storageMap("jdbc").asInstanceOf[JMap[String, Any]].asScala.toMap
    javaConf
      .updated("storage", storageMap.updated("mongodb", mongodbMap).updated("jdbc", jdbcMap))
  }

  val resultStorageMode: String =
    conf("storage").asInstanceOf[Map[String, Any]]("result-storage-mode").asInstanceOf[String]

  // For MongoDB specifics
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

  val jdbcUrl: String = conf("storage")
    .asInstanceOf[Map[String, Any]]("jdbc")
    .asInstanceOf[Map[String, Any]]("url")
    .asInstanceOf[String]

  // For jdbc specifics
  val jdbcUsername: String = conf("storage")
    .asInstanceOf[Map[String, Any]]("jdbc")
    .asInstanceOf[Map[String, Any]]("username")
    .asInstanceOf[String]

  val jdbcPassword: String = conf("storage")
    .asInstanceOf[Map[String, Any]]("jdbc")
    .asInstanceOf[Map[String, Any]]("password")
    .asInstanceOf[String]
}
