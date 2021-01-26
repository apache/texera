package edu.uci.ics.texera.workflow.common.operators.mlmodel

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.FollowPrevious
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.worker.WorkerState
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class MLModelOpExecConfig(
    override val tag: OperatorIdentifier,
    val numWorkers: Int,
    val opExec: () => MLModelOpExec
) extends OpExecConfig(tag) {

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          LayerTag(tag, "main"),
          _ => opExec(),
          numWorkers,
          FollowPrevious(),
          RoundRobinDeployment()
        )
      ),
      Array(),
      Map()
    )
  }

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
