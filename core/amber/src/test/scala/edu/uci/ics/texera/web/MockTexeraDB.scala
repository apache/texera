package edu.uci.ics.texera.web

import ch.vorburger.mariadb4j.DB
import com.mysql.cj.jdbc.MysqlDataSource
import org.jooq.DSLContext
import org.jooq.impl.DSL
import ch.vorburger.mariadb4j.{DB, DBConfigurationBuilder}
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.Utils

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

trait MockTexeraDB {

  private var dbInstance: Option[DB] = None
  private var dslContext: Option[DSLContext] = None

  def getDSLContext: DSLContext = {
    dslContext match {
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
    val database: String = "texera_db"
    val username: String = "root"
    val password: String = ""
    val ddlPath = {
      Utils.amberHomePath.resolve("../scripts/sql/texera_ddl.sql").toRealPath()
    }
    val content = new String(Files.readAllBytes(ddlPath), StandardCharsets.UTF_8)
    val config = DBConfigurationBuilder.newBuilder
      .setPort(0) // 0 => automatically detect free port
      .addArg("--default-time-zone=-8:00")
      .setSecurityDisabled(true)
      .setDeletingTemporaryBaseAndDataDirsOnShutdown(true)
      .build()

    val db = DB.newEmbeddedDB(config)
    db.start()
    db.run(content)

    val dataSource = new MysqlDataSource
    dataSource.setUrl(config.getURL(database))
    dataSource.setUser(username)
    dataSource.setPassword(password)

    dbInstance = Some(db)
    dslContext = Some(DSL.using(dataSource, SqlServer.SQL_DIALECT))
    SqlServer.replaceDSLContext(dslContext.get)
  }
}
