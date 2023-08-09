package edu.uci.ics.texera.workflow.operators.ifstatement

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
class IfStatementOpDesc extends OperatorDescriptor {
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    OpExecConfig
      .manyToOneLayer(operatorIdentifier, _ => new IfStatementOpExec())
      .copy(
        blockingInputs = List(0),
        dependency = Map(1 -> 0)
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "If Statement",
      "If Statement",
      OperatorGroupConstants.CONTROL_GROUP,
      inputPorts = List(InputPort("Condition"), InputPort("Data")),
      outputPorts = List(OutputPort("True"), OutputPort("False"))
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = schemas(1)
  override def getOutputSchemas(schemas: Array[Schema]): Array[Schema] =
    Array(schemas(1), schemas(1))
}
