package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PhysicalLink, PortIdentity}
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.common.workflow.{HashPartition, PhysicalPlan}

import scala.collection.mutable

object AggregateOpDesc {


}

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
