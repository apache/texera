package edu.uci.ics.amber

import org.yaml.snakeyaml.Yaml

import java.io.File
import java.nio.file.Path
import java.util.{Map => JMap}
import scala.jdk.CollectionConverters._

object WorkflowCoreConfig {

  private val configFile: File = new File(
    Path
      .of("workflow-core")
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("workflow-core-config.yaml")
      .toString
  )

  // Load the configuration once during object initialization
  private val conf: Map[String, Any] = {
    val yaml = new Yaml()
    val inputStream = getClass.getClassLoader.getResourceAsStream("workflow-core-config.yaml")
    val javaConf = yaml.load(inputStream).asInstanceOf[JMap[String, Any]].asScala.toMap

    // Ensure the mongodb section is also converted to a Scala Map
    val storageMap = javaConf("storage").asInstanceOf[JMap[String, Any]].asScala.toMap
    val mongodbMap = storageMap("mongodb").asInstanceOf[JMap[String, Any]].asScala.toMap

    // Ensure the user-sys section is also converted to a Scala Map
    val userSysMap = javaConf("user-sys").asInstanceOf[JMap[String, Any]].asScala.toMap

    // Update the configuration map with properly converted sub-maps
    javaConf
      .updated("storage", storageMap.updated("mongodb", mongodbMap))
      .updated("user-sys", userSysMap)
  }

  // Extract values from the configuration file for storage
  val storageMode: String =
    conf("storage").asInstanceOf[Map[String, Any]]("mode").asInstanceOf[String]

  // For user-system
  val userSysEnabled: Boolean =
    conf("user-sys").asInstanceOf[Map[String, Any]]("enable").asInstanceOf[Boolean]

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
