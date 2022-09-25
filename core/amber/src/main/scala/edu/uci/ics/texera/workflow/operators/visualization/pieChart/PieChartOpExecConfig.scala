package edu.uci.ics.texera.workflow.operators.visualization.pieChart

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer.WorkerLayer
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentityUtil.makeLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}
import edu.uci.ics.texera.workflow.common.operators.aggregate.{
  AggregateOpDesc,
  DistributedAggregation
}
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

object PieChartOpExecConfig {

  /**
    * Generic config for a visualization operator that supports aggregation internally.
    * @param id A descriptor's OperatorIdentity.
    * @param aggFunc Custom aggregation function to be applied on the data, the first two layers.
    * @param operatorSchemaInfo The descriptor's OperatorSchemaInfo.
    * @tparam P The type of the aggregation data.
    */
  def opExecPhysicalPlan[P <: AnyRef](
      id: OperatorIdentity,
      aggFunc: DistributedAggregation[P],
      pieChartPartialExec: ((Int, WorkerLayer)) => IOperatorExecutor,
      pieChartFinalExec: ((Int, WorkerLayer)) => IOperatorExecutor,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalPlan = {

    val aggregateOperators = AggregateOpDesc.opExecPhysicalPlan(id, aggFunc, operatorSchemaInfo)
    val tailAggregateOp = aggregateOperators.sinkOperators.last

    val partialLayer = WorkerLayer.oneToOneLayer(
      makeLayer(id, "localPieChartProcessor"),
      pieChartPartialExec
    )
    val finalLayer = WorkerLayer.localLayer(
      makeLayer(id, "globalPieChartProcessor"),
      pieChartFinalExec
    )

    new PhysicalPlan(
      partialLayer :: finalLayer :: aggregateOperators.operatorList,
      LinkIdentity(tailAggregateOp, partialLayer.id) :: LinkIdentity(
        partialLayer.id,
        finalLayer.id
      ) :: aggregateOperators.links
    )
  }
}
