package web.model

import com.mysql.cj.jdbc.MysqlDataSource
import config.ApplicationConf.appConfig
import org.jooq.impl.DSL
import org.jooq.{DSLContext, SQLDialect}

object SqlServer {
  private val mysqlConfig = appConfig.mysqlConfig

  val SQL_DIALECT = SQLDialect.MYSQL
  val dataSource: MysqlDataSource = new MysqlDataSource()
  dataSource.setUrl(mysqlConfig.url)
  dataSource.setUser(mysqlConfig.username)
  dataSource.setPassword(mysqlConfig.password)

  val context: DSLContext = DSL.using(dataSource, SQL_DIALECT)

  def createDSLContext(): DSLContext = {
    context
  }
}
