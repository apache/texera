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
    executeScriptInJDBC(texeraDB.getConnection, removeCCommands(parts(1)))

    SqlServer.initConnection(embedded.getJdbcUrl(username, database), username, password)
    val sqlServerInstance = SqlServer.getInstance()
    dslContext = Some(DSL.using(texeraDB, SQLDialect.POSTGRES))

    sqlServerInstance.replaceDSLContext(dslContext.get)
  }
}
