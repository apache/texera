package edu.uci.ics.texera.workflow.operators.source.postgresql

import edu.uci.ics.texera.workflow.common.metadata.{
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.operators.source.{SQLSourceOpDesc, SQLSourceOpExecConfig}
import edu.uci.ics.texera.workflow.operators.source.postgresql.PostgreSQLConnUtil.connect

import java.sql.{Connection, SQLException}
import java.util.Collections.singletonList
import scala.jdk.CollectionConverters.asScalaBuffer

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
          search,
          searchByColumn,
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
      List.empty,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )

  @throws[SQLException]
  override def establishConn: Connection = connect(host, port, database, username, password)

  override protected def updatePort(): Unit =
    port = if (port.trim().equals("default")) "5432" else port
}
