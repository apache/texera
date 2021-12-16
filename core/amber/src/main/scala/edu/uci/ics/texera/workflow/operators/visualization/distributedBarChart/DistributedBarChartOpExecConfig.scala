package edu.uci.ics.texera.workflow.operators.visualization.distributedBarChart

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.{FollowPrevious, UseAll}
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.{HashBasedShuffle, OneToOne}
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.aggregate.{DistributedAggregation, FinalAggregateOpExec, PartialAggregateOpExec}
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

class DistributedBarChartOpExecConfig(
                                         id: OperatorIdentity,
                                         val aggFunc: DistributedAggregation[java.lang.Double],
                                         desc: DistributedBarChartDesc,
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
      val barChartLayer = new WorkerLayer(
        makeLayer(id, "barChart"),
        _ => new DistributedBarChartExec(desc, operatorSchemaInfo),
        Constants.currentWorkerNum,
        FollowPrevious(),
        RoundRobinDeployment()
      )
      new Topology(
        Array(
          partialLayer,
          finalLayer,
          barChartLayer
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
            barChartLayer,
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
