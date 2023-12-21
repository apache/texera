package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.architecture.deploysemantics.{PhysicalLink, PhysicalOp}
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

object AggregateOpDesc {

  def getPhysicalPlan(
      executionId: ExecutionIdentity,
      id: OperatorIdentity,
      aggFuncs: List[DistributedAggregation[Object]],
      groupByKeys: List[String],
      schemaInfo: OperatorSchemaInfo
  ): PhysicalPlan = {
    val partialPhysicalOp =
      PhysicalOp
        .oneToOnePhysicalOp(
          executionId,
          PhysicalOpIdentity(id, "localAgg"),
          OpExecInitInfo(_ => new PartialAggregateOpExec(aggFuncs, groupByKeys, schemaInfo))
        )
        // a hacky solution to have unique port names for reference purpose
        .copy(isOneToManyOp = true, inputPorts = List(InputPort("in")))

    val finalPhysicalOp = if (groupByKeys == null || groupByKeys.isEmpty) {
      PhysicalOp
        .localPhysicalOp(
          executionId,
          PhysicalOpIdentity(id, "globalAgg"),
          OpExecInitInfo(_ => new FinalAggregateOpExec(aggFuncs, groupByKeys, schemaInfo))
        )
        // a hacky solution to have unique port names for reference purpose
        .copy(isOneToManyOp = true, outputPorts = List(OutputPort("out")))
    } else {
      val partitionColumns: Array[Int] =
        if (groupByKeys == null) Array()
        else groupByKeys.indices.toArray // group by columns are always placed in the beginning

      PhysicalOp
        .hashPhysicalOp(
          executionId,
          PhysicalOpIdentity(id, "globalAgg"),
          OpExecInitInfo(_ => new FinalAggregateOpExec(aggFuncs, groupByKeys, schemaInfo)),
          partitionColumns
        )
        .copy(isOneToManyOp = true, outputPorts = List(OutputPort("out")))
    }

    new PhysicalPlan(
      operators = Set(partialPhysicalOp, finalPhysicalOp),
      links = Set(PhysicalLink(partialPhysicalOp, 0, finalPhysicalOp, 0))
    )
  }

}

abstract class AggregateOpDesc extends LogicalOp {

  override def getPhysicalOp(
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalOp = {
    throw new UnsupportedOperationException("should implement `getPhysicalPlan` instead")
  }

  override def getPhysicalPlan(
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalPlan = {
    var plan = aggregateOperatorExecutor(executionId, operatorSchemaInfo)
    plan.operators.foreach(op => plan = plan.setOperator(op.copy(isOneToManyOp = true)))
    plan
  }

  def aggregateOperatorExecutor(
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalPlan

}
