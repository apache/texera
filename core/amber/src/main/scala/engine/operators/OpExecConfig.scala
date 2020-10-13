package engine.operators

import engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import engine.architecture.controller.Workflow
import engine.architecture.deploysemantics.layer.ActorLayer
import engine.architecture.linksemantics.LinkStrategy
import engine.architecture.worker.WorkerState
import engine.common.ambertag.{AmberTag, LayerTag, OperatorIdentifier}
import engine.common.tuple.Tuple
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.ExecutionContext


/**
  *
  * @param tag
  */
abstract class OpExecConfig(val tag: OperatorIdentifier) extends Serializable {

  class Topology(
      var layers: Array[ActorLayer],
      var links: Array[LinkStrategy],
      var dependencies: Map[LayerTag, Set[LayerTag]]
  ) extends Serializable {
    assert(!dependencies.exists(x => x._2.contains(x._1)))
  }

  lazy val topology: Topology = null

  def runtimeCheck(
      workflow: Workflow
  ): Option[mutable.HashMap[AmberTag, mutable.HashMap[AmberTag, mutable.HashSet[LayerTag]]]] = {
    //do nothing by default
    None
  }

  def requiredShuffle: Boolean = false

  def getShuffleHashFunction(layerTag: LayerTag): Tuple => Int = ???

  def assignBreakpoint(
      topology: Array[ActorLayer],
      states: mutable.AnyRefMap[ActorRef, WorkerState.Value],
      breakpoint: GlobalBreakpoint
  )(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter)

}
