package edu.uci.ics.texera.workflow.common.operators.filter

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.FollowPrevious
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{ActorLayer, ProcessorWorkerLayer}
import edu.uci.ics.amber.engine.architecture.worker.WorkerState
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.engine.operators.OpExecConfig

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class FilterOpExecConfig(
    override val tag: OperatorIdentifier,
    val filterOpExec: () => FilterOpExec
) extends OpExecConfig(tag) {
  override lazy val topology: Topology = {
    new Topology(
      Array(
        new ProcessorWorkerLayer(
          LayerTag(tag, "main"),
          _ => filterOpExec.apply(),
          Constants.defaultNumWorkers,
          FollowPrevious(),
          RoundRobinDeployment()
        )
      ),
      Array(),
      Map()
    )
  }
  override def assignBreakpoint(
      topology: Array[ActorLayer],
      states: mutable.AnyRefMap[ActorRef, WorkerState.Value],
      breakpoint: GlobalBreakpoint
  )(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter): Unit = {
    breakpoint.partition(
      topology(0).layer.filter(states(_) != WorkerState.Completed)
    )
  }
}
