package edu.uci.ics.texera.workflow.operators.intersection

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.{ManyToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

class IntersectOpDesc extends OperatorDescriptor {

  override def operatorExecutor: OpExecConfig = {
    new ManyToOneOpExecConfig(operatorIdentifier, _ => new IntersectOpExec())
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Intersect",
      "Take the intersection of two inputs",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort(), InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.forall(_ == schemas(0)))
    schemas(0)
  }

}
