package edu.uci.ics.amber.operator.source.sql.postgresql

import edu.uci.ics.amber.core.tuple.AttributeType
import edu.uci.ics.amber.operator.source.sql.SQLSourceOpExec
import edu.uci.ics.amber.operator.source.sql.postgresql.PostgreSQLConnUtil.connect
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import java.sql._

class PostgreSQLSourceOpExec private[postgresql] (descString: String)
    extends SQLSourceOpExec(descString) {
  override val desc: PostgreSQLSourceOpDesc =
    objectMapper.readValue(descString, classOf[PostgreSQLSourceOpDesc])
  schema = desc.sourceSchema()
  val FETCH_TABLE_NAMES_SQL =
    "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE';"

  @throws[SQLException]
  override def establishConn(): Connection =
    connect(desc.host, desc.port, desc.database, desc.username, desc.password)

  @throws[RuntimeException]
  override def addFilterConditions(queryBuilder: StringBuilder): Unit = {
    val keywordSearchByColumn = desc.keywordSearchByColumn.orNull
    if (
      desc.keywordSearch.getOrElse(false) && keywordSearchByColumn != null && desc.keywords != null
    ) {
      val columnType = schema.getAttribute(keywordSearchByColumn).getType

      if (columnType == AttributeType.STRING) {
        // in sql prepared statement, column name cannot be inserted using PreparedStatement.setString either
        queryBuilder ++= " AND " + keywordSearchByColumn + " @@ to_tsquery(?)"

        // OPTIMIZE: no fulltext index is required, having a built fulltext index can help performance on large dataset.

        // OPTIMIZE: limited support on the default language, english. equivalent `to_tsquery('english', ?)`
      } else
        throw new RuntimeException("Can't do keyword search on type " + columnType.toString)
    }
  }

  @throws[SQLException]
  override protected def loadTableNames(): Unit = {
    val preparedStatement = connection.prepareStatement(FETCH_TABLE_NAMES_SQL)
    val resultSet = preparedStatement.executeQuery
    while ({
      resultSet.next
    }) {
      tableNames += resultSet.getString(1)
    }
    resultSet.close()
    preparedStatement.close()
  }
}
