package edu.uci.ics.texera.workflow.operators.sink

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.ForceLocal
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RandomDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.worker.WorkerState
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.engine.operators.SinkOpExecConfig

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class SimpleSinkOpExecConfig(tag: OperatorIdentifier) extends SinkOpExecConfig(tag) {
  override lazy val topology = new Topology(
    Array(
      new WorkerLayer(
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
      breakpoint: GlobalBreakpoint
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
