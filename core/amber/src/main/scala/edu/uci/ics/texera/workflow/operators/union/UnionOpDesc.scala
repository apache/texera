package edu.uci.ics.texera.workflow.operators.union

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.workflow.common.metadata.{ OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.{NewInputPort, NewOutputPort, PortIdentity}

class UnionOpDesc extends LogicalOp {

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalOp = {
    PhysicalOp.oneToOnePhysicalOp(
      workflowId,
      executionId,
      operatorIdentifier,
      OpExecInitInfo((_, _, _) => new UnionOpExec())
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Union",
      "Unions the output rows from multiple input operators",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(NewInputPort(PortIdentity(0), allowMultipleLinks = true)),
      outputPorts = List(NewOutputPort.default),
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.forall(_ == schemas(0)))
    schemas(0)
  }

}
