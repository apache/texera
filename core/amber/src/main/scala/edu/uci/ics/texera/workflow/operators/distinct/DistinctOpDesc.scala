package edu.uci.ics.texera.workflow.operators.distinct

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.{HashOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Schema, OperatorSchemaInfo}

class DistinctOpDesc extends OperatorDescriptor {

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    new HashOpExecConfig(operatorIdentifier, _ => new DistinctOpExec())
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Distinct",
      "Remove duplicate tuples",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.forall(_ == schemas(0)))
    schemas(0)
  }

}
