package edu.uci.ics.texera.dao

import com.mysql.cj.jdbc.MysqlDataSource
import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import org.jooq.DSLContext
import org.jooq.impl.DSL

import java.io.{File, FileInputStream, InputStream}
import java.nio.file.{Path, Paths}
import java.sql.{Connection, DriverManager, SQLException, Statement}
import java.util.Scanner

trait MockTexeraDB {

  private var dbInstance: Option[EmbeddedPostgres] = None
  private var dslContext: Option[DSLContext] = None
  private val database: String = "texera_db"
  private val username: String = "root"
  private val password: String = ""

  def executeScriptInJDBC(path: Path): Unit = {
    assert(dbInstance.nonEmpty)
    val sqlFile = new File(path.toString)
    val in = new FileInputStream(sqlFile)
    val conn =
      DriverManager.getConnection(
        dbInstance.get.getJdbcUrl("texera_db"),
        username,
        password
      )
    importSQL(conn, in)
    conn.close()
  }

  @throws[SQLException]
  private def importSQL(conn: Connection, in: InputStream): Unit = {
    val s = new Scanner(in)
    s.useDelimiter(";")
    var st: Statement = null
    try {
      st = conn.createStatement()
      while ({
        s.hasNext
      }) {
        var line = s.next
        if (line.startsWith("/*!") && line.endsWith("*/")) {
          val i = line.indexOf(' ')
          line = line.substring(i + 1, line.length - " */".length)
        }
        if (line.trim.nonEmpty) {
          // mock DB cannot use SET PERSIST keyword
          line = line.replaceAll("(?i)SET PERSIST", "SET GLOBAL")
          st.execute(line)
        }
      }
    } finally if (st != null) st.close()
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

  def getDBInstance: DB = {
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
        value.stop()
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

    val embedded = EmbeddedPostgresRules.singleInstance().getEmbeddedPostgres

    dbInstance = Some(embedded)
    dslContext = Some(DSL.using(dataSource, sqlServerInstance.SQL_DIALECT))

    val ddlPath = {
      Paths.get("./scripts/sql/texera_ddl.sql").toRealPath()
    }
    executeScriptInJDBC(ddlPath)

    sqlServerInstance.replaceDSLContext(dslContext.get)
  }
}
