package edu.uci.ics.texera.workflow.operators.source.sql.mysql

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.texera.workflow.common.metadata.{
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo
import edu.uci.ics.texera.workflow.operators.source.sql.SQLSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.sql.mysql.MySQLConnUtil.connect

import java.sql.{Connection, SQLException}
import java.util.Collections.singletonList
import scala.jdk.CollectionConverters.asScalaBuffer

class MySQLSourceOpDesc extends SQLSourceOpDesc {

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig =
    OpExecConfig.sourceLayer(
      this.operatorIdentifier,
      OpExecInitInfo(_ =>
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
          progressive,
          batchByColumn,
          min,
          max,
          interval,
          keywordSearch.getOrElse(false),
          keywordSearchByColumn.orNull,
          keywords.orNull
        )
      )
    )

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "MySQL Source",
      "Read data from a MySQL instance",
      OperatorGroupConstants.SOURCE_GROUP,
      List.empty,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )

  @throws[SQLException]
  override def establishConn: Connection = connect(host, port, database, username, password)

  override def updatePort(): Unit = port = if (port.trim().equals("default")) "3306" else port

}
