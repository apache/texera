package edu.uci.ics.texera.workflow.operators.source.sql.mysql
import java.sql.{Connection, DriverManager, SQLException}

object MySQLConnUtil {
  @throws[SQLException]
  def connect(
      host: String,
      port: String,
      database: String,
      username: String,
      password: String
  ): Connection = {
    val url =
      "jdbc:mysql://" + host + ":" + port + "/" + database + "?autoReconnect=true&useSSL=true"
    val connection = DriverManager.getConnection(url, username, password)
    // set to readonly to improve efficiency
    connection.setReadOnly(true)
    connection
  }
}
