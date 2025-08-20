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

import org.jooq.codegen.GenerationTool
import org.jooq.meta.jaxb.{Configuration, Database, Generator, Property}
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

object JooqCodeGenerator {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    // Load jOOQ configuration XML
    val jooqXmlPath: Path =
      Path.of("dao").resolve("src").resolve("main").resolve("resources").resolve("jooq-conf.xml")
    val jooqConfig: Configuration = GenerationTool.load(Files.newInputStream(jooqXmlPath))


    val ddlScripts: String = Seq("scripts/sql/texera_ddl.sql").mkString(",")

    // Load the DDL script
    // Ensure a Generator exists
    val generator = Option(jooqConfig.getGenerator).getOrElse(new Generator)
    jooqConfig.setGenerator(generator)

    // Configure DDLDatabase
    val db = Option(generator.getDatabase).getOrElse(new Database)
    db.setName("org.jooq.meta.extensions.ddl.DDLDatabase")
    db.setProperties(
      Seq(
        new Property().withKey("scripts").withValue(ddlScripts),
        // Optional but useful for Postgres-style naming + public schema handling
        new Property().withKey("defaultNameCase").withValue("lower"),
        new Property().withKey("unqualifiedSchema").withValue("public"),
        // Make sure migrations are applied in a sensible order
        new Property().withKey("sort").withValue("semantic"),
        new Property().withKey("parseIgnoreComments").withValue("true"),
        new Property().withKey("parseIgnoreCommentStart").withValue("[jooq ignore start]"),
        new Property().withKey("parseIgnoreCommentStop").withValue("[jooq ignore stop]")
      ).asJava
    )
    generator.setDatabase(db)
    // 3) Make sure we don't use a live database
    jooqConfig.setJdbc(null)

    // 4) Generate
    GenerationTool.generate(jooqConfig)

  }
}
