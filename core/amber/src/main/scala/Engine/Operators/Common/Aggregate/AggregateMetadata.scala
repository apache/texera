package Engine.Operators.Common.Aggregate

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.DeploySemantics.DeployStrategy.{RandomDeployment, RoundRobinDeployment}
import Engine.Architecture.DeploySemantics.DeploymentFilter.{FollowPrevious, UseAll}
import Engine.Architecture.DeploySemantics.Layer.{ActorLayer, ProcessorWorkerLayer}
import Engine.Architecture.LinkSemantics.AllToOne
import Engine.Architecture.Worker.WorkerState
import Engine.Common.AmberTag.{LayerTag, OperatorTag}
import Engine.Common.AmberTuple.Tuple
import Engine.Common.Constants
import Engine.Operators.OperatorMetadata
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.ExecutionContext


class AggregateMetadata
(tag:OperatorTag, val numWorkers:Int,
 val aggFunc: DistributedAggregation, val groupByKeys: Seq[Int]) extends OperatorMetadata(tag){

  override lazy val topology: Topology = {

    val partialLayer = new ProcessorWorkerLayer(LayerTag(tag,"localAgg"),_ =>
      new PartialAggregateProcessor(aggFunc, groupByKeys), numWorkers, UseAll(), RoundRobinDeployment())
    val finalLayer = new ProcessorWorkerLayer(LayerTag(tag,"globalAgg"),_ =>
      new FinalAggregateProcessor(aggFunc, groupByKeys), if (groupByKeys.isEmpty) 1 else numWorkers, FollowPrevious(), RoundRobinDeployment())
    new Topology(Array(
      partialLayer,
      finalLayer
    ),Array(
      new AllToOne(partialLayer,finalLayer,Constants.defaultBatchSize)
    ),Map())
  }
  override def assignBreakpoint(topology: Array[ActorLayer], states: mutable.AnyRefMap[ActorRef, WorkerState.Value], breakpoint: GlobalBreakpoint)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Unit = {
    breakpoint.partition(topology(0).layer.filter(states(_)!= WorkerState.Completed))
  }
}
