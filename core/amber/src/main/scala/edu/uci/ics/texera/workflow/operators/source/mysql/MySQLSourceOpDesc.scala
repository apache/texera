package edu.uci.ics.texera.workflow.operators.source.mysql

import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.operators.source.mysql.MySQLConnUtil.connect
import edu.uci.ics.texera.workflow.operators.source.{SQLSourceOpDesc, SQLSourceOpExecConfig}

import java.sql.{Connection, SQLException}

class MySQLSourceOpDesc extends SQLSourceOpDesc {

  override def operatorExecutor =
    new SQLSourceOpExecConfig(
      this.operatorIdentifier,
      (worker: Any) =>
        new MySQLSourceOpExec(
          this.querySchema,
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
      "MySQL Source",
      "Read data from a MySQL instance",
      OperatorGroupConstants.SOURCE_GROUP,
      0,
      1
    )

  @throws[SQLException]
  override def establishConn: Connection = connect(host, port, database, username, password)

  override def updatePort(): Unit = port = if (port.trim().equals("default")) "3306" else port

}
