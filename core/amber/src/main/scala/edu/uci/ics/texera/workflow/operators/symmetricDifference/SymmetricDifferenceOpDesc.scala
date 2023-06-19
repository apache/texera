package edu.uci.ics.texera.workflow.operators.symmetricDifference

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

class SymmetricDifferenceOpDesc extends OperatorDescriptor {

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo) = {
    OpExecConfig.hashLayer(
      operatorIdentifier,
      _ => new SymmetricDifferenceOpExec(),
      operatorSchemaInfo.inputSchemas(0).getAttributes.toArray.indices.toArray
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "SymmetricDifference",
      "find the symmetric difference (the set of elements which are in either of the sets, but not in their intersection) of two inputs",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort(), InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.forall(_ == schemas(0)))
    schemas(0)
  }

}
