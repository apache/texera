package edu.uci.ics.amber.engine.operators

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.worker.WorkerState
import edu.uci.ics.amber.engine.common.ambertag.{AmberTag, LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.common.WorkflowLogger

import scala.collection.mutable
import scala.concurrent.ExecutionContext

/**
  * @param tag
  */
abstract class OpExecConfig(val tag: OperatorIdentifier) extends Serializable {

  class Topology(
      var layers: Array[WorkerLayer],
      var links: Array[LinkStrategy],
      var dependencies: Map[LayerTag, Set[LayerTag]]
  ) extends Serializable {
    assert(!dependencies.exists(x => x._2.contains(x._1)))
  }

  val opExecConfigLogger = WorkflowLogger(s"OpExecConfig ${tag.getGlobalIdentity}")
  lazy val topology: Topology = null
  var inputToOrdinalMapping = new mutable.HashMap[OperatorIdentifier, Int]()

  def runtimeCheck(
      workflow: Workflow
  ): Option[mutable.HashMap[AmberTag, mutable.HashMap[AmberTag, mutable.HashSet[LayerTag]]]] = {
    //do nothing by default
    None
  }

  def requiredShuffle: Boolean = false

  def setInputToOrdinalMapping(input: OperatorIdentifier, ordinal: Integer): Unit = {
    this.inputToOrdinalMapping.update(input, ordinal)
  }

  def getInputNum(from: OperatorIdentifier): Int = {
    assert(this.inputToOrdinalMapping.contains(from))
    this.inputToOrdinalMapping(from)
  }

  def getShuffleHashFunction(layerTag: LayerTag): ITuple => Int = ???

  def assignBreakpoint(
      topology: Array[WorkerLayer],
      states: mutable.AnyRefMap[ActorRef, WorkerState.Value],
      breakpoint: GlobalBreakpoint
  )(implicit timeout: Timeout, ec: ExecutionContext)

}
