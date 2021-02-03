package edu.uci.ics.texera.workflow.operators.source.postgresql

import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.source.SQLSourceOpExec
import edu.uci.ics.texera.workflow.operators.source.postgresql.PostgreSQLConnUtil.connect

import java.sql._

class PostgreSQLSourceOpExec private[postgresql] (
    schema: Schema,
    host: String,
    port: String,
    database: String,
    table: String,
    username: String,
    password: String,
    limit: Option[Long],
    offset: Option[Long],
    column: Option[String],
    keywords: Option[String],
    progressive: Boolean,
    batchByColumn: Option[String],
    interval: Long
) extends SQLSourceOpExec(
      schema,
      table,
      limit,
      offset,
      column,
      keywords,
      progressive,
      batchByColumn,
      interval
    ) {
  val FETCH_TABLE_NAMES_SQL =
    "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE';"

  @throws[SQLException]
  override def establishConn(): Connection = connect(host, port, database, username, password)

  override def addKeywordSearch(queryBuilder: StringBuilder): Unit = {
    val columnType = schema.getAttribute(column.get).getType
    // TODO: check if index exists (e.g., fulltext index is needed to do fulltext search)
    if (columnType == AttributeType.STRING) {
      // TODO: implement postgresql's fulltext search
      throw new NotImplementedError()
    } else
      throw new RuntimeException("Can't do keyword search on type " + columnType.toString)
  }

  @throws[SQLException]
  override protected def loadTableNames(): Unit = {
    val preparedStatement = connection.prepareStatement(FETCH_TABLE_NAMES_SQL)
    val resultSet = preparedStatement.executeQuery
    while ({ resultSet.next }) {
      tableNames += resultSet.getString(1)
    }
    resultSet.close()
    preparedStatement.close()
  }
}
