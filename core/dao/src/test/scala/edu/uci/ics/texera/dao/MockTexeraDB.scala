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

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.jooq.{DSLContext, SQLDialect}
import org.jooq.impl.DSL

import java.nio.file.Paths
import java.sql.{Connection, DriverManager}
import scala.io.Source

trait MockTexeraDB {

  private var dbInstance: Option[EmbeddedPostgres] = None
  private var dslContext: Option[DSLContext] = None
  private val database: String = "texera_db"
  private val username: String = "postgres"
  private val password: String = ""

  def executeScriptInJDBC(conn: Connection, script: String): Unit = {
    assert(dbInstance.nonEmpty)
    conn.prepareStatement(script).execute()
    conn.close()
  }

  def getDSLContext: DSLContext = {
    dslContext match {
      case Some(value) => value
      case None =>
        throw new RuntimeException(
          "test database is not initialized. Did you call initializeDBAndReplaceDSLContext()?"
        )
    }
  }

  def getDBInstance: EmbeddedPostgres = {
    dbInstance match {
      case Some(value) => value
      case None =>
        throw new RuntimeException(
          "test database is not initialized. Did you call initializeDBAndReplaceDSLContext()?"
        )
    }
  }

  def shutdownDB(): Unit = {
    dbInstance match {
      case Some(value) =>
        value.close()
        dbInstance = None
        dslContext = None
      case None =>
      // do nothing
    }
  }

  def initializeDBAndReplaceDSLContext(): Unit = {
    assert(dbInstance.isEmpty && dslContext.isEmpty)

    val driver = new org.postgresql.Driver()
    DriverManager.registerDriver(driver)

    val embedded = EmbeddedPostgres.builder().start()

    dbInstance = Some(embedded)

    val ddlPath = {
      Paths.get("./scripts/sql/texera_ddl.sql").toRealPath()
    }
    val source = Source.fromFile(ddlPath.toString)
    val content =
      try {
        source.mkString
      } finally {
        source.close()
      }
    val parts: Array[String] = content.split("(?m)^\\\\c texera_db")
    def removeCCommands(sql: String): String =
      sql.linesIterator
        .filterNot(_.trim.startsWith("\\c"))
        .mkString("\n")
    executeScriptInJDBC(embedded.getPostgresDatabase.getConnection, removeCCommands(parts(0)))
    val texeraDB = embedded.getDatabase(username, database)
    var tablesAndIndexCreation = removeCCommands(parts(1))

    // remove indexes creation for pgroonga because we cannot install the plugin
    val blockPattern =
      """(?s)-- START Fulltext search index creation \(DO NOT EDIT THIS LINE\).*?-- END Fulltext search index creation \(DO NOT EDIT THIS LINE\)\n?""".r
    // replace with native fulltext indexes
    val replacementText =
      """CREATE INDEX idx_workflow_name_description_content
        |    ON workflow
        |    USING GIN (
        |    to_tsvector('english',
        |    COALESCE(name, '') || ' ' ||
        |    COALESCE(description, '') || ' ' ||
        |    COALESCE(content, '')
        |    )
        |    );
        |
        |CREATE INDEX idx_user_name
        |    ON "user"
        |    USING GIN (
        |    to_tsvector('english',
        |    COALESCE(name, '')
        |    )
        |    );
        |
        |CREATE INDEX idx_user_project_name_description
        |    ON project
        |    USING GIN (
        |    to_tsvector('english',
        |    COALESCE(name, '') || ' ' ||
        |    COALESCE(description, '')
        |    )
        |    );
        |
        |CREATE INDEX idx_dataset_name_description
        |    ON dataset
        |    USING GIN (
        |    to_tsvector('english',
        |    COALESCE(name, '') || ' ' ||
        |    COALESCE(description, '')
        |    )
        |    );
        |
        |CREATE INDEX idx_dataset_version_name
        |    ON dataset_version
        |    USING GIN (
        |    to_tsvector('english',
        |    COALESCE(name, '')
        |    )
        |    );""".stripMargin

    tablesAndIndexCreation = blockPattern.replaceAllIn(tablesAndIndexCreation, replacementText).trim
    executeScriptInJDBC(texeraDB.getConnection, tablesAndIndexCreation)
    SqlServer.initConnection(embedded.getJdbcUrl(username, database), username, password)
    val sqlServerInstance = SqlServer.getInstance()
    dslContext = Some(DSL.using(texeraDB, SQLDialect.POSTGRES))

    sqlServerInstance.replaceDSLContext(dslContext.get)
  }
}
