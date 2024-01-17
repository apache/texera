package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.architecture.deploysemantics.{PhysicalLink, PhysicalOp}
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

object AggregateOpDesc {

  def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      id: OperatorIdentity,
      aggFuncs: List[DistributedAggregation[Object]],
      groupByKeys: List[String],
      schemaInfo: OperatorSchemaInfo
  ): PhysicalPlan = {
    val partialPhysicalOp =
      PhysicalOp
        .oneToOnePhysicalOp(
          PhysicalOpIdentity(id, "localAgg"),
          workflowId,
          executionId,
          OpExecInitInfo((_, _, _) => new PartialAggregateOpExec(aggFuncs, groupByKeys, schemaInfo))
        )
        .withIsOneToManyOp(true)
        // a hacky solution to have unique port names for reference purpose
        .copy(inputPorts = List(InputPort("in")))

    val finalPhysicalOp = if (groupByKeys == null || groupByKeys.isEmpty) {
      PhysicalOp
        .localPhysicalOp(
          PhysicalOpIdentity(id, "globalAgg"),
          workflowId,
          executionId,
          OpExecInitInfo((_, _, _) => new FinalAggregateOpExec(aggFuncs, groupByKeys, schemaInfo))
        )
        .withParallelizable(false)
        .withIsOneToManyOp(true)
        // a hacky solution to have unique port names for reference purpose
        .withOutputPorts(List(OutputPort("out")))
    } else {
      val partitionColumns: List[Int] =
        if (groupByKeys == null) List()
        else groupByKeys.indices.toList // group by columns are always placed in the beginning

      PhysicalOp
        .hashPhysicalOp(
          PhysicalOpIdentity(id, "globalAgg"),
          workflowId,
          executionId,
          OpExecInitInfo((_, _, _) => new FinalAggregateOpExec(aggFuncs, groupByKeys, schemaInfo)),
          partitionColumns
        )
        .withParallelizable(false)
        .withIsOneToManyOp(true)
        // a hacky solution to have unique port names for reference purpose
        .withOutputPorts(List(OutputPort("out")))
    }

    new PhysicalPlan(
      operators = Set(partialPhysicalOp, finalPhysicalOp),
      links = Set(PhysicalLink(partialPhysicalOp, 0, finalPhysicalOp, 0))
    )
  }

}

abstract class AggregateOpDesc extends LogicalOp {

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalOp = {
    throw new UnsupportedOperationException("should implement `getPhysicalPlan` instead")
  }

  override def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalPlan = {
    var plan = aggregateOperatorExecutor(workflowId, executionId, operatorSchemaInfo)
    plan.operators.foreach(op => plan = plan.setOperator(op.withIsOneToManyOp(true)))
    plan
  }

  def aggregateOperatorExecutor(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalPlan

}
