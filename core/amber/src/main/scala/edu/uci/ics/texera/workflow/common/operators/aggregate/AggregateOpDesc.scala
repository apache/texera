package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

abstract class AggregateOpDesc extends LogicalOp {

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    throw new UnsupportedOperationException("should implement `getPhysicalPlan` instead")
  }

  override def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalPlan = {
    var plan = aggregateOperatorExecutor(workflowId, executionId)
    plan.operators.foreach(op => plan = plan.setOperator(op.withIsOneToManyOp(true)))
    plan
  }

  def aggregateOperatorExecutor(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalPlan

}
