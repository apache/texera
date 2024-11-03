package edu.uci.ics.texera.dao

import com.mysql.cj.jdbc.MysqlDataSource
import org.jooq.{DSLContext, SQLDialect}
import org.jooq.impl.DSL

/**
  * SqlServer class that manages a connection to a MySQL database using jOOQ.
  *
  * @param url The database connection URL, specifying the MySQL server and database name.
  * @param user The username for authenticating with the MySQL database.
  * @param password The password for authenticating with the MySQL database.
  */
class SqlServer private (url: String, user: String, password: String) {
  val SQL_DIALECT: SQLDialect = SQLDialect.MYSQL
  private val dataSource: MysqlDataSource = new MysqlDataSource()
  var context: DSLContext = _

  {
    dataSource.setUrl(url)
    dataSource.setUser(user)
    dataSource.setPassword(password)
    context = DSL.using(dataSource, SQL_DIALECT)
  }

  def createDSLContext(): DSLContext = context

  def replaceDSLContext(newContext: DSLContext): Unit = {
    context = newContext
  }
}

object SqlServer {
  @volatile private var instance: Option[SqlServer] = None

  def getInstance(url: String, user: String, password: String): SqlServer = {
    instance match {
      case Some(server) => server
      case None =>
        synchronized {
          instance match {
            case Some(server) => server
            case None =>
              val server = new SqlServer(url, user, password)
              instance = Some(server)
              server
          }
        }
    }
  }

  /**
    * A utility function for create a transaction block using given sql context
    * @param dsl the sql context
    * @param block the code block to execute within the transaction
    * @tparam T the value will be returned by the code block
    * @return
    */
  def withTransaction[T](dsl: DSLContext)(block: DSLContext => T): T = {
    var result: Option[T] = None

    dsl.transaction(configuration => {
      val ctx = DSL.using(configuration)
      result = Some(block(ctx))
    })

    result.getOrElse(throw new RuntimeException("Transaction failed without result!"))
  }
}
