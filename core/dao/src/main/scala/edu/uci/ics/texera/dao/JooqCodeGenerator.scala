package edu.uci.ics.texera.dao

import org.jooq.codegen.GenerationTool
import org.jooq.meta.jaxb.{Configuration, Jdbc}
import org.yaml.snakeyaml.Yaml

import java.io.InputStream
import java.nio.file.{Files, Path}

object JooqCodeGenerator {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    // Path to jOOQ configuration XML
    val jooqXmlPath: Path = Path.of("dao").resolve("src").resolve("main").resolve("resources").resolve("jooq-conf.xml")
    val jooqConfig: Configuration = GenerationTool.load(Files.newInputStream(jooqXmlPath))
    // Path to the YAML configuration file
    val yamlConfPath: Path = Path.of("workflow-core").resolve("src").resolve("main").resolve("resources").resolve("storage-config.yaml")
    // Load YAML configuration
    val yaml: Yaml = new Yaml
    try {
      val inputStream: InputStream = Files.newInputStream(yamlConfPath)
      try {
        val config: Map[String, AnyRef] = yaml.load(inputStream)
        val jdbcConfigMap: Map[String, AnyRef] = (config.get("storage").asInstanceOf[Map[String, AnyRef]]).get("jdbc").asInstanceOf[Map[String, AnyRef]]
        // Set JDBC configuration for jOOQ
        val jooqJdbcConfig: Jdbc = new Jdbc
        jooqJdbcConfig.setDriver("com.mysql.cj.jdbc.Driver")
        jooqJdbcConfig.setUrl(jdbcConfigMap.get("url").toString)
        jooqJdbcConfig.setUsername(jdbcConfigMap.get("username").toString)
        jooqJdbcConfig.setPassword(jdbcConfigMap.get("password").toString)
        jooqConfig.setJdbc(jooqJdbcConfig)
        // Generate the code
        GenerationTool.generate(jooqConfig)
      } finally {
        if (inputStream != null) inputStream.close()
      }
    }
  }
}
