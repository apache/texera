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

package org.apache.texera.dao

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.jooq.impl.DSL
import org.jooq.{DSLContext, SQLDialect}

import java.nio.file.Paths
import java.sql.{Connection, DriverManager}
import scala.io.Source

/**
  * Provides a JVM-singleton EmbeddedPostgres for tests. Multiple specs that mix
  * in this trait share one Postgres instance for the lifetime of the JVM, which
  * avoids the OverlappingFileLockException that occurs when each spec tries to
  * extract the embedded Postgres binaries into the same directory in parallel.
  */
object MockTexeraDB {
  private val database: String = "texera_db"
  private val username: String = "postgres"
  private val password: String = ""

  private var dbInstance: Option[EmbeddedPostgres] = None
  private var dslContext: Option[DSLContext] = None

  def ensureInitialized(): Unit =
    synchronized {
      if (dbInstance.isDefined) return

      val driver = new org.postgresql.Driver()
      DriverManager.registerDriver(driver)

      val embedded = EmbeddedPostgres.builder().start()
      dbInstance = Some(embedded)

      val ddlPath = Paths.get("sql/texera_ddl.sql").toRealPath()
      val source = Source.fromFile(ddlPath.toString)
      val content =
        try source.mkString
        finally source.close()
      val parts: Array[String] = content.split("(?m)^CREATE DATABASE :\"DB_NAME\";")
      def removeCCommands(sql: String): String =
        sql.linesIterator.filterNot(_.trim.startsWith("\\c")).mkString("\n")
      val createDBStatement =
        """DROP DATABASE IF EXISTS texera_db;
          |CREATE DATABASE texera_db;""".stripMargin
      executeScriptInJDBC(embedded.getPostgresDatabase.getConnection, createDBStatement)
      val texeraDB = embedded.getDatabase(username, database)
      var tablesAndIndexCreation = removeCCommands(parts(1))

      // remove indexes creation for pgroonga because we cannot install the plugin
      val blockPattern =
        """(?s)-- START Fulltext search index creation \(DO NOT EDIT THIS LINE\).*?-- END Fulltext search index creation \(DO NOT EDIT THIS LINE\)\n?""".r
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

      tablesAndIndexCreation =
        blockPattern.replaceAllIn(tablesAndIndexCreation, replacementText).trim
      executeScriptInJDBC(texeraDB.getConnection, tablesAndIndexCreation)
      SqlServer.initConnection(embedded.getJdbcUrl(username, database), username, password)
      val sqlServerInstance = SqlServer.getInstance()
      val ctx = DSL.using(texeraDB, SQLDialect.POSTGRES)
      dslContext = Some(ctx)
      sqlServerInstance.replaceDSLContext(ctx)
    }

  def getDSLContext: DSLContext =
    dslContext.getOrElse(
      throw new RuntimeException(
        "test database is not initialized. Did you call initializeDBAndReplaceDSLContext()?"
      )
    )

  def getDBInstance: EmbeddedPostgres =
    dbInstance.getOrElse(
      throw new RuntimeException(
        "test database is not initialized. Did you call initializeDBAndReplaceDSLContext()?"
      )
    )

  private def executeScriptInJDBC(conn: Connection, script: String): Unit = {
    conn.prepareStatement(script).execute()
    conn.close()
  }
}

trait MockTexeraDB {

  def executeScriptInJDBC(conn: Connection, script: String): Unit = {
    conn.prepareStatement(script).execute()
    conn.close()
  }

  def getDSLContext: DSLContext = MockTexeraDB.getDSLContext

  def getDBInstance: EmbeddedPostgres = MockTexeraDB.getDBInstance

  def initializeDBAndReplaceDSLContext(): Unit = MockTexeraDB.ensureInitialized()

  /**
    * No-op. The singleton EmbeddedPostgres lives for the lifetime of the JVM,
    * so individual specs should not shut it down. Kept for API compatibility
    * with existing afterAll hooks.
    */
  def shutdownDB(): Unit = ()
}
