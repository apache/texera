package texera.common.operators.aggregate

import engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import engine.architecture.deploysemantics.deploystrategy.{RandomDeployment, RoundRobinDeployment}
import engine.architecture.deploysemantics.deploymentfilter.{FollowPrevious, ForceLocal, UseAll}
import engine.architecture.deploysemantics.layer.{ActorLayer, ProcessorWorkerLayer}
import engine.architecture.linksemantics.{AllToOne, HashBasedShuffle}
import engine.architecture.worker.WorkerState
import engine.common.ambertag.{LayerTag, OperatorIdentifier}
import engine.common.Constants
import engine.operators.OpExecConfig
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import texera.common.tuple.TexeraTuple

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class TexeraAggregateOpExecConfig[P <: AnyRef](
    tag: OperatorIdentifier,
    val aggFunc: TexeraDistributedAggregation[P]
) extends OpExecConfig(tag) {

  override lazy val topology: Topology = {

    if (aggFunc.groupByFunc == null) {
      val partialLayer = new ProcessorWorkerLayer(
        LayerTag(tag, "localAgg"),
        _ => new TexeraPartialAggregateOpExec(aggFunc),
        Constants.defaultNumWorkers,
        UseAll(),
        RoundRobinDeployment()
      )
      val finalLayer = new ProcessorWorkerLayer(
        LayerTag(tag, "globalAgg"),
        _ => new TexeraFinalAggregateOpExec(aggFunc),
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
        ),
        Map()
      )
    } else {
      val partialLayer = new ProcessorWorkerLayer(
        LayerTag(tag, "localAgg"),
        _ => new TexeraPartialAggregateOpExec(aggFunc),
        Constants.defaultNumWorkers,
        UseAll(),
        RoundRobinDeployment()
      )
      val finalLayer = new ProcessorWorkerLayer(
        LayerTag(tag, "globalAgg"),
        _ => new TexeraFinalAggregateOpExec(aggFunc),
        Constants.defaultNumWorkers,
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
            x => {
              val tuple = x.asInstanceOf[TexeraTuple]
              aggFunc.groupByFunc(tuple).hashCode()
            }
          )
        ),
        Map()
      )
    }
  }
  override def assignBreakpoint(
      topology: Array[ActorLayer],
      states: mutable.AnyRefMap[ActorRef, WorkerState.Value],
      breakpoint: GlobalBreakpoint
  )(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter): Unit = {
    breakpoint.partition(topology(0).layer.filter(states(_) != WorkerState.Completed))
  }
}
