package edu.uci.ics.amber.operator.map

import edu.uci.ics.amber.core.workflow.PhysicalOp
import edu.uci.ics.amber.operator.{LogicalOp, StateTransferFunc}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}

import scala.util.{Success, Try}

abstract class MapOpDesc extends LogicalOp {

  override def runtimeReconfiguration(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      oldOpDesc: LogicalOp,
      newOpDesc: LogicalOp
  ): Try[(PhysicalOp, Option[StateTransferFunc])] = {
    Success(newOpDesc.getPhysicalOp(workflowId, executionId), None)
  }
}
