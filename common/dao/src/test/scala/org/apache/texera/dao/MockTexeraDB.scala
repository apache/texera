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

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.apache.texera.dao.MockTexeraDB.{MaxPoolSize, password, username}
import org.jooq.impl.{DSL, DataSourceConnectionProvider, DefaultConfiguration}
import org.jooq.{DSLContext, SQLDialect}
import org.scalatest.{Outcome, TestSuite, TestSuiteMixin}

import java.nio.file.Paths
import java.sql.DriverManager
import scala.io.Source
import scala.util.Using

/**
  * Provides a JVM-singleton EmbeddedPostgres for tests. Multiple specs that mix
  * in this trait share one Postgres instance for the lifetime of the JVM.
  */
object MockTexeraDB {
  private val username: String = "postgres"
  private val password: String = ""
  private val texeraDDLPath = "sql/texera_ddl.sql"
  private val splitDatabaseRegex = "(?m)^CREATE DATABASE :\"DB_NAME\";"

  val MaxPoolSize: Int = math.max(10, Runtime.getRuntime.availableProcessors() * 2)

  @volatile private var dbInstance: Option[EmbeddedPostgres] = None
  @volatile private var ddlScript: Option[String] = None

  def ensureInitialized(): Unit =
    synchronized {
      if (dbInstance.isDefined && ddlScript.isDefined) return

      if (dbInstance.isEmpty) {
        val driver = new org.postgresql.Driver()
        DriverManager.registerDriver(driver)

        // Boot the heavy JVM engine exactly once
        dbInstance = Some(EmbeddedPostgres.builder().start())
      }

      val ddlPath = Paths.get(texeraDDLPath).toRealPath()
      val source = Source.fromFile(ddlPath.toString)
      val content =
        try source.mkString
        finally source.close()

      val parts: Array[String] = content.split(splitDatabaseRegex)
      val sqlBody = parts
        .lift(1)
        .getOrElse(
          throw new RuntimeException(
            s"Couldn't split SQL body from $texeraDDLPath: " +
              s"expected it to match pattern $splitDatabaseRegex"
          )
        )

      def removeCCommands(sql: String): String =
        sql.linesIterator.filterNot(_.trim.startsWith("\\c")).mkString("\n")

      val tablesAndIndexCreation = removeCCommands(sqlBody)

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

  def getDBInstance: EmbeddedPostgres =
    dbInstance.getOrElse(throw new RuntimeException("DB not initialized"))
  def getDDLScript: String = ddlScript.getOrElse(throw new RuntimeException("DDL not loaded"))
}

trait MockTexeraDB extends TestSuiteMixin { this: TestSuite =>
  private var testScopedContext: Option[DSLContext] = None
  protected var dataSource: Option[HikariDataSource] = None
  protected var uniqueDbName: String = ""

  def createHikariConfig(jbdcUrl: String): HikariConfig = {
    val hikariConfig = new HikariConfig()
    hikariConfig.setJdbcUrl(jbdcUrl)
    hikariConfig.setUsername(username)
    hikariConfig.setPassword(password)
    hikariConfig.setMaximumPoolSize(MaxPoolSize)
    hikariConfig
  }

  def initializeDBAndReplaceDSLContext(): Unit =
    synchronized {
      if (dataSource.isEmpty || dataSource.get.isClosed) {
        MockTexeraDB.ensureInitialized()
        val embedded = MockTexeraDB.getDBInstance

        uniqueDbName = "texera_db_" + java.util.UUID.randomUUID().toString.replace("-", "")
        Using.resource(embedded.getPostgresDatabase.getConnection) { defaultConn =>
          Using.resource(defaultConn.createStatement()) { stmt =>
            stmt.execute(s"CREATE DATABASE $uniqueDbName")
          }
        }

        // Run the DDL once via a throwaway connection (autoCommit is TRUE by default,
        // so the schema is permanently committed to this suite's isolated database).
        Using.resource(embedded.getDatabase("postgres", uniqueDbName).getConnection) { conn =>
          Using.resource(conn.createStatement()) { stmt =>
            stmt.execute(MockTexeraDB.getDDLScript)
          }
        }

        val jdbcUrl = embedded.getJdbcUrl("postgres", uniqueDbName)
        val ds = new HikariDataSource(createHikariConfig(jbdcUrl = jdbcUrl))
        dataSource = Some(ds)

        val jooqCfg = new DefaultConfiguration()
        jooqCfg.set(new DataSourceConnectionProvider(ds))
        jooqCfg.set(SQLDialect.POSTGRES)
        val scopedCtx = DSL.using(jooqCfg)
        testScopedContext = Some(scopedCtx)

        SqlServer.initConnection(jdbcUrl, username, password)
        SqlServer.getInstance().replaceDSLContext(scopedCtx)
      }
    }

  abstract override def withFixture(test: NoArgTest): Outcome = {
    initializeDBAndReplaceDSLContext()

    val sqlServerInstance = SqlServer.getInstance()
    val activeContext = testScopedContext.get

    try {
      sqlServerInstance.replaceDSLContext(activeContext)
      super.withFixture(test)
    } finally {
      /*TODO: Need to truncate texeraDB tables when the fixture is complete.
         This will require refactoring the spec tests using MockTexeraDB
         to move initialization logic outside of BeforeAll into BeforeEach
       */
    }
  }

  def getDSLContext: DSLContext =
    synchronized {
      if (testScopedContext.isEmpty) {
        initializeDBAndReplaceDSLContext()
      }
      testScopedContext.get
    }

  def getDBInstance: EmbeddedPostgres = MockTexeraDB.getDBInstance
  def newRawConnection(): java.sql.Connection = {
    MockTexeraDB.getDBInstance.getDatabase(username, uniqueDbName).getConnection
  }

  def shutdownDB(): Unit =
    synchronized {
      try dataSource.foreach(ds => if (!ds.isClosed) ds.close())
      catch { case e: Exception => e.printStackTrace() }
      finally { dataSource = None; testScopedContext = None }
    }
}
