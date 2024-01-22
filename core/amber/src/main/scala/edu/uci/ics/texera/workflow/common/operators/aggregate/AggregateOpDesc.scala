package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PhysicalLink, PortIdentity}
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
    val outputPort = OutputPort(PortIdentity(internal = true))
    val partialPhysicalOp =
      PhysicalOp
        .oneToOnePhysicalOp(
          PhysicalOpIdentity(id, "localAgg"),
          workflowId,
          executionId,
          OpExecInitInfo((_, _, _) => new PartialAggregateOpExec(aggFuncs, groupByKeys, schemaInfo))
        )
        .withIsOneToManyOp(true)
        .withInputPorts(List(InputPort(PortIdentity())))
        .withOutputPorts(List(outputPort))

    val inputPort = InputPort(PortIdentity(0, internal = true))
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
        .withInputPorts(List(inputPort))
        .withOutputPorts(List(OutputPort(PortIdentity(0))))
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
        .withInputPorts(List(inputPort))
        .withOutputPorts(List(OutputPort(PortIdentity(0))))
    }

    new PhysicalPlan(
      operators = Set(partialPhysicalOp, finalPhysicalOp),
      links = Set(
        PhysicalLink(partialPhysicalOp.id, outputPort.id, finalPhysicalOp.id, inputPort.id)
      )
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
