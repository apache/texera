package edu.uci.ics.texera.workflow.common.operators.map

import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.workflow.common.operators.{LogicalOp, StateTransferFunc}
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

import scala.util.{Failure, Success, Try}

abstract class MapOpDesc extends LogicalOp {

  override def runtimeReconfiguration(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      newOpDesc: LogicalOp,
      operatorSchemaInfo: OperatorSchemaInfo
  ): Try[(PhysicalOp, Option[StateTransferFunc])] = {
    val newSchemas = newOpDesc.getOutputSchema(operatorSchemaInfo.inputSchemas)
    if (!newSchemas.equals(operatorSchemaInfo.outputSchemas(0))) {
      Failure(
        new UnsupportedOperationException(
          "reconfigurations that change output schema are not supported"
        )
      )
    } else {
      Success(newOpDesc.getPhysicalOp(workflowId, executionId, operatorSchemaInfo), None)
    }
  }

}
