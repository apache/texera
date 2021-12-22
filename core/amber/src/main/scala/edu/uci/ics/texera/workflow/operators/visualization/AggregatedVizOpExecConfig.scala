package edu.uci.ics.texera.workflow.operators.visualization

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.{
  FollowPrevious,
  UseAll
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.{HashBasedShuffle, OneToOne}
import edu.uci.ics.amber.engine.common.{Constants, IOperatorExecutor}
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.aggregate.{
  DistributedAggregation,
  FinalAggregateOpExec,
  PartialAggregateOpExec
}
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

/**
  * Generic config for a visualization operator that supports aggregation internally.
  * @param id A descriptor's OperatorIdentity.
  * @param aggFunc Custom aggregation function to be applied on the data, the first two layers.
  * @param exec The final layer, wraps things up for whatever is needed by the frontend.
  * @param operatorSchemaInfo The descriptor's OperatorSchemaInfo.
  * @tparam P The type of the aggregation data.
  */
class AggregatedVizOpExecConfig[P <: AnyRef](
    id: OperatorIdentity,
    val aggFunc: DistributedAggregation[P],
    exec: IOperatorExecutor,
    operatorSchemaInfo: OperatorSchemaInfo
) extends OpExecConfig(id) {

  override lazy val topology: Topology = {
    val partialLayer = new WorkerLayer(
      makeLayer(id, "localAgg"),
      _ => new PartialAggregateOpExec(aggFunc),
      Constants.currentWorkerNum,
      UseAll(),
      RoundRobinDeployment()
    )
    val finalLayer = new WorkerLayer(
      makeLayer(id, "globalAgg"),
      _ => new FinalAggregateOpExec(aggFunc),
      Constants.currentWorkerNum,
      FollowPrevious(),
      RoundRobinDeployment()
    )
    val vizLayer = new WorkerLayer(
      makeLayer(id, "visualize"),
      _ => exec,
      Constants.currentWorkerNum,
      FollowPrevious(),
      RoundRobinDeployment()
    )
    new Topology(
      Array(
        partialLayer,
        finalLayer,
        vizLayer
      ),
      Array(
        new HashBasedShuffle(
          partialLayer,
          finalLayer,
          Constants.defaultBatchSize,
          getPartitionColumnIndices(partialLayer.id)
        ),
        new OneToOne(
          finalLayer,
          vizLayer,
          Constants.defaultBatchSize
        )
      )
    )
  }

  override def getPartitionColumnIndices(layer: LayerIdentity): Array[Int] = {
    aggFunc
      .groupByFunc(operatorSchemaInfo.inputSchemas(0))
      .getAttributes
      .toArray
      .indices
      .toArray
  }

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
