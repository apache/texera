package edu.uci.ics.amber.operator.source.sql.postgresql

import java.sql.{Connection, DriverManager, SQLException}

object PostgreSQLConnUtil {
  @throws[SQLException]
  def connect(
      host: String,
      port: String,
      database: String,
      username: String,
      password: String
  ): Connection = {
    val url = "jdbc:postgresql://" + host + ":" + port + "/" + database
    val connection = DriverManager.getConnection(url, username, password)
    // set to readonly to improve efficiency
    connection.setReadOnly(true)
    connection
  }
}
