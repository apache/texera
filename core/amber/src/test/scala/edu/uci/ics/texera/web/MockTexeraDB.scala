package edu.uci.ics.texera.web

import ch.vorburger.mariadb4j.DB
import com.mysql.cj.jdbc.MysqlDataSource
import org.jooq.DSLContext
import org.jooq.impl.DSL
import ch.vorburger.mariadb4j.{DB, DBConfigurationBuilder}
import edu.uci.ics.texera.Utils

import java.io.{File, FileInputStream, InputStream}
import java.nio.file.Path
import java.sql.{Connection, DriverManager, SQLException, Statement}
import java.util.Scanner

trait MockTexeraDB {

  private var dbInstance: Option[DB] = None
  private var dslContext: Option[DSLContext] = None
  private val database: String = "texera_db"
  private val username: String = "root"
  private val password: String = ""

  def executeScriptInJDBC(path: Path): Unit = {
    assert(dbInstance.nonEmpty)
    val sqlFile = new File(path.toString)
    val in = new FileInputStream(sqlFile)
    val conn =
      DriverManager.getConnection(dbInstance.get.getConfiguration.getURL(""), username, password)
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

    val config = DBConfigurationBuilder.newBuilder
      .setPort(0) // 0 => automatically detect free port
      .addArg("--default-time-zone=-8:00")
      .setSecurityDisabled(true)
      .setDeletingTemporaryBaseAndDataDirsOnShutdown(true)
      .build()

    val db = DB.newEmbeddedDB(config)
    db.start()

    val dataSource = new MysqlDataSource
    dataSource.setUrl(config.getURL(database))
    dataSource.setUser(username)
    dataSource.setPassword(password)

    dbInstance = Some(db)
    dslContext = Some(DSL.using(dataSource, SqlServer.SQL_DIALECT))

    val ddlPath = {
      Utils.amberHomePath.resolve("../scripts/sql/texera_ddl.sql").toRealPath()
    }
    executeScriptInJDBC(ddlPath)

    SqlServer.replaceDSLContext(dslContext.get)
  }
}
