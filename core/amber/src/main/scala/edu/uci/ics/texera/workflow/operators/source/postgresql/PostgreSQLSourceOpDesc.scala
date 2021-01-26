package edu.uci.ics.texera.workflow.operators.source.postgresql

import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.operators.source.postgresql.PostgreSQLConnUtil.connect
import edu.uci.ics.texera.workflow.operators.source.{SQLSourceOpDesc, SQLSourceOpExecConfig}

import java.sql.{Connection, SQLException}

class PostgreSQLSourceOpDesc extends SQLSourceOpDesc {

  override def operatorExecutor =
    new SQLSourceOpExecConfig(
      operatorIdentifier,
      (worker: Any) =>
        new PostgreSQLSourceOpExec(
          querySchema,
          host,
          port,
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
        )
    )
  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "PostgreSQL Source",
      "Read data from a PostgreSQL instance",
      OperatorGroupConstants.SOURCE_GROUP,
      0,
      1
    )

  @throws[SQLException]
  override def establishConn: Connection = connect(host, port, database, username, password)

  override protected def updatePort(): Unit =
    port = if (port.trim().equals("default")) "5432" else port
}
