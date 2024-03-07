package edu.uci.ics.texera.workflow.operators.source.sql.mysql

import edu.uci.ics.amber.engine.architecture.deploysemantics.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.common.workflow.OutputPort
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.operators.source.sql.SQLSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.sql.mysql.MySQLConnUtil.connect

import java.sql.{Connection, SQLException}

class MySQLSourceOpDesc extends SQLSourceOpDesc {

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        this.operatorIdentifier,
        OpExecInitInfo((_, _) =>
          new MySQLSourceOpExec(
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
            keywords.orNull,
            () => sourceSchema()
          )
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "MySQL Source",
      "Read data from a MySQL instance",
      OperatorGroupConstants.DATABASE_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )

  @throws[SQLException]
  override def establishConn: Connection = connect(host, port, database, username, password)

  override def updatePort(): Unit = port = if (port.trim().equals("default")) "3306" else port

}
