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
import org.scalatest.Outcome
import org.scalatest.flatspec.AnyFlatSpecLike

import java.nio.file.Paths
import java.sql.{Connection, DriverManager, Savepoint}
import scala.io.Source
import scala.util.Using

/**
  * Provides a JVM-singleton EmbeddedPostgres for tests. Multiple specs that mix
  * in this trait share one Postgres instance for the lifetime of the JVM, which
  * avoids the OverlappingFileLockException that occurs when each spec tries to
  * extract the embedded Postgres binaries into the same directory in parallel.
  */
object MockTexeraDB {
  private val username: String = "postgres"
  private val password: String = ""

  @volatile private var dbInstance: Option[EmbeddedPostgres] = None
  @volatile private var ddlScript: Option[String] = None

  def ensureInitialized(): Unit = synchronized {
    if (dbInstance.isDefined) return

    val driver = new org.postgresql.Driver()
    DriverManager.registerDriver(driver)

    // Boot the heavy JVM engine exactly once
    val embedded = EmbeddedPostgres.builder().start()
    dbInstance = Some(embedded)

    val ddlPath = Paths.get("sql/texera_ddl.sql").toRealPath()
    val source = Source.fromFile(ddlPath.toString)
    val content = try source.mkString finally source.close()

    val parts: Array[String] = content.split("(?m)^CREATE DATABASE :\"DB_NAME\";")
    val sqlBody = if (parts.length > 1) parts(1) else content

    def removeCCommands(sql: String): String =
      sql.linesIterator.filterNot(_.trim.startsWith("\\c")).mkString("\n")

    var tablesAndIndexCreation = removeCCommands(sqlBody)

    val blockPattern =
      """(?s)-- START Fulltext search index creation \(DO NOT EDIT THIS LINE\).*?-- END Fulltext search index creation \(DO NOT EDIT THIS LINE\)\n?""".r
    val replacementText =
      """CREATE INDEX idx_workflow_name_description_content ON workflow USING GIN (to_tsvector('english', COALESCE(name, '') || ' ' || COALESCE(description, '') || ' ' || COALESCE(content, '')));
        |CREATE INDEX idx_user_name ON "user" USING GIN (to_tsvector('english', COALESCE(name, '')));
        |CREATE INDEX idx_user_project_name_description ON project USING GIN (to_tsvector('english', COALESCE(name, '') || ' ' || COALESCE(description, '')));
        |CREATE INDEX idx_dataset_name_description ON dataset USING GIN (to_tsvector('english', COALESCE(name, '') || ' ' || COALESCE(description, '')));
        |CREATE INDEX idx_dataset_version_name ON dataset_version USING GIN (to_tsvector('english', COALESCE(name, '')));""".stripMargin

    // Cache the cleaned script so parallel suites don't have to re-read the file
    ddlScript = Some(blockPattern.replaceAllIn(tablesAndIndexCreation, replacementText).trim)
  }

  def getDBInstance: EmbeddedPostgres = dbInstance.getOrElse(throw new RuntimeException("DB not initialized"))
  def getDDLScript: String = ddlScript.getOrElse(throw new RuntimeException("DDL not loaded"))
}

trait MockTexeraDB extends AnyFlatSpecLike {
  private var testScopedContext: Option[DSLContext] = None
  private var connection: Option[Connection] = None
  private var uniqueDbName: String = ""

  def initializeDBAndReplaceDSLContext(): Unit = synchronized {
    if (connection.isEmpty || connection.get.isClosed) {
      MockTexeraDB.ensureInitialized()
      val embedded = MockTexeraDB.getDBInstance

      // 1. Create a 100% isolated logical database for this specific test suite
      uniqueDbName = "texera_db_" + java.util.UUID.randomUUID().toString.replace("-", "")
      Using.resource(embedded.getPostgresDatabase.getConnection) { defaultConn =>
        Using.resource(defaultConn.createStatement()) { stmt =>
          stmt.execute(s"CREATE DATABASE $uniqueDbName")
        }
      }

      // 2. Connect to the isolated DB and apply the DDL
      val conn = embedded.getDatabase("postgres", uniqueDbName).getConnection

      // AutoCommit is TRUE by default, meaning any records inserted in beforeAll()
      // will be permanently committed to this suite's specific isolated database!
      Using.resource(conn.createStatement()) { stmt =>
        stmt.execute(MockTexeraDB.getDDLScript)
      }

      connection = Some(conn)
      val scopedCtx = DSL.using(conn, SQLDialect.POSTGRES)
      testScopedContext = Some(scopedCtx)

      // Point the Texera backend exactly to this suite's isolated database
      SqlServer.initConnection(embedded.getJdbcUrl("postgres", uniqueDbName), "postgres", "")
      SqlServer.getInstance().replaceDSLContext(scopedCtx)
    }
  }

  override def withFixture(test: NoArgTest): Outcome = {
    initializeDBAndReplaceDSLContext()

    val conn = connection.get
    val sqlServerInstance = SqlServer.getInstance()
    val activeContext = testScopedContext.get

    // 3. Turn OFF autocommit to sandbox this individual test case
    conn.setAutoCommit(false)

    try {
      sqlServerInstance.replaceDSLContext(activeContext)
      super.withFixture(test)
    } finally {
      try {
        if (!conn.isClosed) {
          // 4. Perform a FULL rollback. This wipes dirty test data and clears any
          // aborted SQL errors. Because beforeAll data was already auto-committed, it survives!
          conn.rollback()
          // 5. Restore autoCommit for afterEach / afterAll hooks
          conn.setAutoCommit(true)
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  def getDSLContext: DSLContext = synchronized {
    if (testScopedContext.isEmpty) {
      initializeDBAndReplaceDSLContext()
    }
    testScopedContext.get
  }

  def getDBInstance: EmbeddedPostgres = MockTexeraDB.getDBInstance

  def shutdownDB(): Unit = synchronized {
    try {
      connection.foreach { conn =>
        if (!conn.isClosed) {
          conn.close()
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      connection = None
      testScopedContext = None
    }
  }
}