package edu.uci.ics.texera.workflow.operators.visualization

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.{
  FollowPrevious,
  ForceLocal,
  UseAll
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.{
  RandomDeployment,
  RoundRobinDeployment
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{WorkerLayer, WorkerLayerImpl}
import edu.uci.ics.amber.engine.architecture.linksemantics.{HashBasedShuffle, OneToOne}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.HashBasedShufflePartitioning
import edu.uci.ics.amber.engine.common.{Constants, IOperatorExecutor}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentityUtil.makeLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.texera.workflow.common.operators.aggregate.{
  AggregateOpDesc,
  DistributedAggregation,
  FinalAggregateOpExec,
  PartialAggregateOpExec
}
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

object AggregatedVizOpExecConfig {

  /**
    * Generic config for a visualization operator that supports aggregation internally.
    * @param id A descriptor's OperatorIdentity.
    * @param aggFunc Custom aggregation function to be applied on the data, the first two layers.
    * @param exec The final layer, wraps things up for whatever is needed by the frontend.
    * @param operatorSchemaInfo The descriptor's OperatorSchemaInfo.
    * @tparam P The type of the aggregation data.
    */
  def opExecPhysicalPlan[P <: AnyRef](
      id: OperatorIdentity,
      aggFunc: DistributedAggregation[P],
      exec: ((Int, WorkerLayer)) => IOperatorExecutor,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalPlan = {

    val aggregateOperators = AggregateOpDesc.opExecPhysicalPlan(id, aggFunc, operatorSchemaInfo)
    val tailAggregateOp = aggregateOperators.sinkOperators.last

    val vizLayer = WorkerLayer.oneToOneLayer(makeLayer(id, "visualize"), exec)

    new PhysicalPlan(
      vizLayer :: aggregateOperators.operatorList,
      LinkIdentity(tailAggregateOp, vizLayer.id) :: aggregateOperators.links
    )
  }

}
