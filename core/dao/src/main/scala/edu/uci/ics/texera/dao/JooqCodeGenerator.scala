/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.texera.dao

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import org.jooq.codegen.GenerationTool
import org.jooq.meta.jaxb.{Configuration, Jdbc}

import java.nio.file.{Files, Path}
import java.io.File

object JooqCodeGenerator {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    // Load jOOQ configuration XML
    val jooqXmlPath: Path =
      Path.of("dao").resolve("src").resolve("main").resolve("resources").resolve("jooq-conf.xml")
    val jooqConfig: Configuration = GenerationTool.load(Files.newInputStream(jooqXmlPath))

    // Load storage.conf from the specified path
    val storageConfPath: Path = Path
      .of("workflow-core")
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("storage.conf")

    val conf: Config = ConfigFactory
      .parseFile(
        new File(storageConfPath.toString),
        ConfigParseOptions.defaults().setAllowMissing(false)
      )
      .resolve()

    // Extract JDBC configuration
    val jdbcConfig = conf.getConfig("storage.jdbc")

    val jooqJdbcConfig = new Jdbc
    jooqJdbcConfig.setDriver("org.postgresql.Driver")
    // Skip all the query params, otherwise it will omit the "texera_db." prefix on the field names.
    jooqJdbcConfig.setUrl(jdbcConfig.getString("url").split('?').head)
    jooqJdbcConfig.setUsername(jdbcConfig.getString("username"))
    jooqJdbcConfig.setPassword(jdbcConfig.getString("password"))

    jooqConfig.setJdbc(jooqJdbcConfig)

    // Generate the code
    GenerationTool.generate(jooqConfig)
  }
}
