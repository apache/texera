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
    val mongodbMap = javaConf("storage").asInstanceOf[Map[String, Any]]("mongodb").asInstanceOf[JMap[String, Any]].asScala.toMap
    javaConf.updated("storage", javaConf("storage").asInstanceOf[Map[String, Any]].updated("mongodb", mongodbMap)) // Update conf with mongodb as Scala Map
  }

  // Extract values from the configuration file for storage
  val storageMode: String = conf("storage").asInstanceOf[Map[String, String]]("mode")

  // For user-system
  val userSysEnabled: Boolean = conf("user-sys").asInstanceOf[Map[String, Boolean]]("enable")

  // For MongoDB specifics
  val mongodbUrl: String = conf("storage").asInstanceOf[Map[String, Any]]("mongodb").asInstanceOf[Map[String, String]]("url")
  val mongodbDatabaseName: String = conf("storage").asInstanceOf[Map[String, Any]]("mongodb").asInstanceOf[Map[String, String]]("database")
  val mongodbBatchSize: Int = conf("storage").asInstanceOf[Map[String, Any]]("mongodb").asInstanceOf[Map[String, Int]]("commit-batch-size")
}