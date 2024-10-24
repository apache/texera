package edu.uci.ics.amber

import org.yaml.snakeyaml.Yaml
import java.util.{Map => JMap}
import scala.jdk.CollectionConverters._

object WorkflowCoreConfig {
  private val conf: Map[String, Any] = {
    val yaml = new Yaml()
    val inputStream = getClass.getClassLoader.getResourceAsStream("workflow-core-config.yaml")
    val javaConf = yaml.load(inputStream).asInstanceOf[JMap[String, Any]].asScala.toMap

    val storageMap = javaConf("storage").asInstanceOf[JMap[String, Any]].asScala.toMap
    val mongodbMap = storageMap("mongodb").asInstanceOf[JMap[String, Any]].asScala.toMap

    val userSysMap = javaConf("user-sys").asInstanceOf[JMap[String, Any]].asScala.toMap

    javaConf
      .updated("storage", storageMap.updated("mongodb", mongodbMap))
      .updated("user-sys", userSysMap)
  }

  val userSysEnabled: Boolean =
    conf("user-sys").asInstanceOf[Map[String, Any]]("enable").asInstanceOf[Boolean]

  val storageMode: String =
    conf("storage").asInstanceOf[Map[String, Any]]("mode").asInstanceOf[String]

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
}
