package edu.uci.ics.amber.operator.filter

import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.PhysicalOp
import edu.uci.ics.amber.operator.{LogicalOp, StateTransferFunc}

import scala.util.{Success, Try}

abstract class FilterOpDesc extends LogicalOp {

  override def runtimeReconfiguration(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      oldOpDesc: LogicalOp,
      newOpDesc: LogicalOp
  ): Try[(PhysicalOp, Option[StateTransferFunc])] = {
    Success(newOpDesc.getPhysicalOp(workflowId, executionId), None)
  }

}
