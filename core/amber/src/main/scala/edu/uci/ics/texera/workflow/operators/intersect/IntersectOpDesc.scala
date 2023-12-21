package edu.uci.ics.texera.workflow.operators.intersect

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.ExecutionIdentity
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

class IntersectOpDesc extends LogicalOp {

  override def getPhysicalOp(
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalOp = {
    PhysicalOp.hashPhysicalOp(
      executionId,
      operatorIdentifier,
      OpExecInitInfo(_ => new IntersectOpExec()),
      operatorSchemaInfo.inputSchemas(0).getAttributes.toArray.indices.toArray
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Intersect",
      "Take the intersect of two inputs",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort(), InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.forall(_ == schemas(0)))
    schemas(0)
  }

}
