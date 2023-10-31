package edu.uci.ics.texera.workflow.operators.union

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

class UnionOpDesc extends OperatorDescriptor {

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo) = {
    OpExecConfig.oneToOneLayer(operatorIdentifier, _ => new UnionOpExec())
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Union",
      "Unions the output rows from multiple input operators",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort(allowMultiInputs = true)),
      outputPorts = List(OutputPort()),
      supportRetractableInput = true
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.forall(_ == schemas(0)))
    schemas(0)
  }

}
