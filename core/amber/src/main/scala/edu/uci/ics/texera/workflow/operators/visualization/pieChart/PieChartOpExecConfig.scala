package edu.uci.ics.texera.workflow.operators.visualization.pieChart

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
import edu.uci.ics.amber.engine.architecture.linksemantics.{AllToOne, HashBasedShuffle, OneToOne}
import edu.uci.ics.amber.engine.common.Constants
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

class PieChartOpExecConfig[P <: AnyRef](
    tag: OperatorIdentity,
    val numWorkers: Int,
    val nameColumn: String,
    val dataColumn: String,
    val pruneRatio: Double,
    val aggFunc: DistributedAggregation[P],
    operatorSchemaInfo: OperatorSchemaInfo
) extends OpExecConfig(tag) {

  override lazy val topology: Topology = {
    val aggPartialLayer = new WorkerLayer(
      makeLayer(id, "localAgg"),
      _ => new PartialAggregateOpExec(aggFunc),
      numWorkers,
      UseAll(),
      RoundRobinDeployment()
    )
    val aggFinalLayer = new WorkerLayer(
      makeLayer(id, "globalAgg"),
      _ => new FinalAggregateOpExec(aggFunc),
      numWorkers,
      FollowPrevious(),
      RoundRobinDeployment()
    )
    val partialLayer = new WorkerLayer(
      makeLayer(tag, "localPieChartProcessor"),
      _ => new PieChartOpPartialExec(nameColumn, dataColumn),
      numWorkers,
      UseAll(),
      RoundRobinDeployment()
    )
    val finalLayer = new WorkerLayer(
      makeLayer(tag, "globalPieChartProcessor"),
      _ => new PieChartOpFinalExec(pruneRatio, dataColumn),
      1,
      ForceLocal(),
      RandomDeployment()
    )
    new Topology(
      Array(
        aggPartialLayer,
        aggFinalLayer,
        partialLayer,
        finalLayer
      ),
      Array(
        new HashBasedShuffle(
          aggPartialLayer,
          aggFinalLayer,
          Constants.defaultBatchSize,
          getPartitionColumnIndices(partialLayer.id)
        ),
        new OneToOne(
          aggFinalLayer,
          partialLayer,
          Constants.defaultBatchSize
        ),
        new AllToOne(
          partialLayer,
          finalLayer,
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
