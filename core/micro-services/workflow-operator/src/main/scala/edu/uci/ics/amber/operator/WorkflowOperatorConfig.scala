package edu.uci.ics.amber.operator

import org.yaml.snakeyaml.Yaml

import java.io.File
import java.nio.file.Path
import java.util.{Map => JMap}
import scala.jdk.CollectionConverters._

object WorkflowOperatorConfig {
  private val configFile: File = new File(
    Path
      .of("workflow-operators")
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("workflow-operators-config.yaml")
      .toString
  )

  // Load the configuration once during object initialization
  private val conf: Map[String, Any] = {
    val yaml = new Yaml()
    val inputStream = getClass.getClassLoader.getResourceAsStream("workflow-operators-config.yaml")
    yaml.load(inputStream).asInstanceOf[JMap[String, Any]].asScala.toMap
  }

  // Extract values from the configuration file
  val numWorkerPerOperator: Int = conf("workflow")
    .asInstanceOf[Map[String, Any]]("operator")
    .asInstanceOf[Map[String, Int]]("num-worker-per-operator")
}
