package edu.uci.ics.texera.workflow.common.operators.aggregate

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
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.{AllToOne, HashBasedShuffle}
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

class AggregateOpExecConfig[P <: AnyRef](
    id: OperatorIdentity,
    val aggFunc: DistributedAggregation[P],
    operatorSchemaInfo: OperatorSchemaInfo
) extends OpExecConfig(id) {

  override lazy val topology: Topology = {

    if (aggFunc.groupByFunc == null) {
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
        1,
        ForceLocal(),
        RandomDeployment()
      )
      new Topology(
        Array(
          partialLayer,
          finalLayer
        ),
        Array(
          new AllToOne(partialLayer, finalLayer, Constants.defaultBatchSize)
        )
      )
    } else {
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
      new Topology(
        Array(
          partialLayer,
          finalLayer
        ),
        Array(
          new HashBasedShuffle(
            partialLayer,
            finalLayer,
            Constants.defaultBatchSize,
            getPartitionColumnIndices(partialLayer.id)
          )
        )
      )
    }
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
