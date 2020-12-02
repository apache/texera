package edu.uci.ics.texera.workflow.common.operators.aggregate

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
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
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{
  ActorLayer,
  ProcessorWorkerLayer
}
import edu.uci.ics.amber.engine.architecture.linksemantics.{AllToOne, HashBasedShuffle}
import edu.uci.ics.amber.engine.architecture.worker.WorkerState
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class AggregateOpExecConfig[P <: AnyRef](
    tag: OperatorIdentifier,
    val aggFunc: DistributedAggregation[P]
) extends OpExecConfig(tag) {

  override lazy val topology: Topology = {

    if (aggFunc.groupByFunc == null) {
      val partialLayer = new ProcessorWorkerLayer(
        LayerTag(tag, "localAgg"),
        _ => new PartialAggregateOpExec(aggFunc),
        Constants.defaultNumWorkers,
        UseAll(),
        RoundRobinDeployment()
      )
      val finalLayer = new ProcessorWorkerLayer(
        LayerTag(tag, "globalAgg"),
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
          new AllToOne(partialLayer, finalLayer, Constants.defaultBatchSize, 0)
        ),
        Map()
      )
    } else {
      val partialLayer = new ProcessorWorkerLayer(
        LayerTag(tag, "localAgg"),
        _ => new PartialAggregateOpExec(aggFunc),
        Constants.defaultNumWorkers,
        UseAll(),
        RoundRobinDeployment()
      )
      val finalLayer = new ProcessorWorkerLayer(
        LayerTag(tag, "globalAgg"),
        _ => new FinalAggregateOpExec(aggFunc),
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
              val tuple = x.asInstanceOf[Tuple]
              aggFunc.groupByFunc(tuple).hashCode()
            },
            0
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

  override def getInputNum(from: OperatorIdentifier): Int = 0
}
