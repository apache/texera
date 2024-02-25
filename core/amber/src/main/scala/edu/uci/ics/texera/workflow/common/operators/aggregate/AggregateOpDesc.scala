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
import edu.uci.ics.texera.workflow.common.operators.aggregate.PartialAggregateOpExec.getOutputSchema
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

import scala.collection.mutable

object AggregateOpDesc {

  def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      id: OperatorIdentity,
      aggFuncs: List[DistributedAggregation[Object]],
      groupByKeys: List[String],
      inputSchema: Schema,
      outputSchema: Schema
  ): PhysicalPlan = {
    val outputPort = OutputPort(PortIdentity(internal = true))

    val partialPhysicalOp =
      PhysicalOp
        .oneToOnePhysicalOp(
          PhysicalOpIdentity(id, "localAgg"),
          workflowId,
          executionId,
          OpExecInitInfo((_, _, _) =>
            new PartialAggregateOpExec(aggFuncs, groupByKeys, inputSchema)
          )
        )
        .withIsOneToManyOp(true)
        .withInputPorts(List(InputPort(PortIdentity())), mutable.Map(PortIdentity() -> inputSchema))
        // assume partial op's output is the same as global op's
        .withOutputPorts(
          List(outputPort),
          mutable.Map(outputPort.id -> getOutputSchema(inputSchema, aggFuncs, groupByKeys))
        )

    val inputPort = InputPort(PortIdentity(0, internal = true))
    val finalPhysicalOp = if (groupByKeys == null || groupByKeys.isEmpty) {
      PhysicalOp
        .localPhysicalOp(
          PhysicalOpIdentity(id, "globalAgg"),
          workflowId,
          executionId,
          OpExecInitInfo((_, _, _) => new FinalAggregateOpExec(aggFuncs, groupByKeys, outputSchema))
        )
        .withParallelizable(false)
        .withIsOneToManyOp(true)
        // assume partial op's output is the same as global op's
        .withInputPorts(List(inputPort), mutable.Map(inputPort.id -> outputSchema))
        .withOutputPorts(
          List(OutputPort(PortIdentity(0))),
          mutable.Map(PortIdentity() -> outputSchema)
        )
    } else {
      val partitionColumns: List[Int] =
        if (groupByKeys == null) List()
        else groupByKeys.indices.toList // group by columns are always placed in the beginning

      PhysicalOp
        .hashPhysicalOp(
          PhysicalOpIdentity(id, "globalAgg"),
          workflowId,
          executionId,
          OpExecInitInfo((_, _, _) =>
            new FinalAggregateOpExec(aggFuncs, groupByKeys, outputSchema)
          ),
          partitionColumns
        )
        .withParallelizable(false)
        .withIsOneToManyOp(true)
        // assume partial op's output is the same as global op's
        .withInputPorts(List(inputPort), mutable.Map(inputPort.id -> outputSchema))
        .withOutputPorts(
          List(OutputPort(PortIdentity(0))),
          mutable.Map(PortIdentity() -> outputSchema)
        )
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
