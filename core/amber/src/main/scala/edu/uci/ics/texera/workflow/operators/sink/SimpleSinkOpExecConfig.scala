package edu.uci.ics.texera.workflow.operators.sink

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.ForceLocal
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RandomDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{ActorLayer, ProcessorWorkerLayer}
import edu.uci.ics.amber.engine.architecture.worker.WorkerState
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.engine.operators.OpExecConfig

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class SimpleSinkOpExecConfig(tag: OperatorIdentifier) extends OpExecConfig(tag) {
  override lazy val topology = new Topology(
    Array(
      new ProcessorWorkerLayer(
        LayerTag(tag, "main"),
        _ => new SimpleSinkOpExec(),
        1,
        ForceLocal(),
        RandomDeployment()
      )
    ),
    Array(),
    Map()
  )

  override def assignBreakpoint(
      topology: Array[ActorLayer],
      states: mutable.AnyRefMap[ActorRef, WorkerState.Value],
      breakpoint: GlobalBreakpoint
  )(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter): Unit = {
    breakpoint.partition(topology(0).layer.filter(states(_) != WorkerState.Completed))
  }
}
