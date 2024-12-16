package edu.uci.ics.texera.dao

import org.jooq.codegen.GenerationTool
import org.jooq.meta.jaxb.{Configuration, Jdbc}
import org.yaml.snakeyaml.Yaml

import java.io.InputStream
import java.nio.file.{Files, Path}
import java.util.{Map => JMap}
import scala.jdk.CollectionConverters._

object JooqCodeGenerator {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    // Load jOOQ configuration XML
    val jooqXmlPath: Path =
      Path.of("dao").resolve("src").resolve("main").resolve("resources").resolve("jooq-conf.xml")
    val jooqConfig: Configuration = GenerationTool.load(Files.newInputStream(jooqXmlPath))

    // Load YAML configuration
    val yamlConfPath: Path = Path
      .of("workflow-core")
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("storage-config.yaml")
    val yaml = new Yaml
    val inputStream: InputStream = Files.newInputStream(yamlConfPath)

    val conf: Map[String, Any] =
      yaml.load(inputStream).asInstanceOf[JMap[String, Any]].asScala.toMap

    val jdbcConfig = conf("storage")
      .asInstanceOf[JMap[String, Any]]
      .asScala("jdbc")
      .asInstanceOf[JMap[String, Any]]
      .asScala

    // Set JDBC configuration for jOOQ
    val jooqJdbcConfig = new Jdbc
    jooqJdbcConfig.setDriver("com.mysql.cj.jdbc.Driver")
    jooqJdbcConfig.setUrl(jdbcConfig("url").toString)
    jooqJdbcConfig.setUsername(jdbcConfig("username").toString)
    jooqJdbcConfig.setPassword(jdbcConfig("password").toString)

    jooqConfig.setJdbc(jooqJdbcConfig)

    // Generate the code
    GenerationTool.generate(jooqConfig)

    // Close input stream
    inputStream.close()
  }
}
