package edu.uci.ics.texera.workflow.operators.source.mysql

import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.operators.source.SQLSourceOpExec
import edu.uci.ics.texera.workflow.operators.source.mysql.MySQLConnUtil.connect

import java.sql._

class MySQLSourceOpExec private[mysql] (
    schema: Schema,
    host: String,
    port: String,
    database: String,
    table: String,
    username: String,
    password: String,
    limit: Long,
    offset: Long,
    column: String,
    keywords: String,
    progressive: Boolean,
    batchByColumn: String,
    interval: Long
) extends SQLSourceOpExec(
      schema,
      host,
      if (port.trim().equals("default")) "3306" else port,
      database,
      table,
      username,
      password,
      limit,
      offset,
      column,
      keywords,
      progressive,
      batchByColumn,
      interval
    ) {

  val FETCH_TABLE_NAMES_SQL =
    "SELECT table_name FROM information_schema.tables WHERE table_schema = ?;"

  @throws[SQLException]
  override def establishConn: Connection = connect(host, port, database, username, password)

  @throws[SQLException]
  override protected def loadTableNames(): Unit = {
    val preparedStatement = connection.prepareStatement(FETCH_TABLE_NAMES_SQL)
    preparedStatement.setString(1, database)
    val resultSet = preparedStatement.executeQuery
    while ({ resultSet.next }) tableNames.add(resultSet.getString(1))
    resultSet.close()
    preparedStatement.close()
  }
}
