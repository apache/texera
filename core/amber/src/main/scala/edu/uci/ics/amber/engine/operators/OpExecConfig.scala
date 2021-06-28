package edu.uci.ics.amber.engine.operators

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{WorkerInfo, WorkerLayer}
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.principal.{OperatorState, OperatorStatistics}
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager._
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}

import scala.collection.mutable

/**
  * @param id
  */
abstract class OpExecConfig(val id: OperatorIdentity) extends Serializable {

  class Topology(
      var layers: Array[WorkerLayer],
      var links: Array[LinkStrategy]
  ) extends Serializable

  val opExecConfigLogger = WorkflowLogger(s"OpExecConfig $id")
  lazy val topology: Topology = null
  var inputToOrdinalMapping = new mutable.HashMap[LinkIdentity, Int]()
  var attachedBreakpoints = new mutable.HashMap[String, GlobalBreakpoint[_]]()
  var results: List[ITuple] = List.empty

  def getState: OperatorState = {
    val workerStates = getAllWorkerStates
    if (workerStates.forall(_ == Completed)) {
      return OperatorState.Completed
    }
    if (workerStates.exists(_ == Running)) {
      return OperatorState.Running
    }
    val unCompletedWorkerStates = workerStates.filter(_ != Completed)
    if (unCompletedWorkerStates.forall(_ == Uninitialized)) {
      OperatorState.Uninitialized
    } else if (unCompletedWorkerStates.forall(_ == Paused)) {
      OperatorState.Paused
    } else if (unCompletedWorkerStates.forall(_ == Ready)) {
      OperatorState.Ready
    } else {
      OperatorState.Unknown
    }
  }

  def acceptResultTuples(tuples: List[ITuple]): Unit = {
    results ++= tuples
  }

  def getAllWorkers: Iterable[ActorVirtualIdentity] = topology.layers.flatMap(l => l.identifiers)

  def getAllWorkerStates: Iterable[WorkerState] = topology.layers.flatMap(l => l.states)

  def getWorker(id: ActorVirtualIdentity): WorkerInfo = {
    val layer = topology.layers.find(l => l.workers.contains(id)).get
    layer.workers(id)
  }

  def setAllWorkerState(state: WorkerState): Unit = {
    topology.layers.foreach { layer =>
      (0 until layer.numWorkers).foreach(layer.states.update(_, state))
    }
  }

  def getLayerFromWorkerID(id: ActorVirtualIdentity): WorkerLayer =
    topology.layers.find(_.identifiers.contains(id)).get

  def getInputRowCount: Long = topology.layers.head.statistics.map(_.inputRowCount).sum

  def getOutputRowCount: Long = topology.layers.last.statistics.map(_.outputRowCount).sum

  def getOperatorStatistics: OperatorStatistics =
    OperatorStatistics(getState, getInputRowCount, getOutputRowCount)

  def checkStartDependencies(workflow: Workflow): Unit = {
    //do nothing by default
  }

  def requiredShuffle: Boolean = false

  def setInputToOrdinalMapping(input: LinkIdentity, ordinal: Integer): Unit = {
    this.inputToOrdinalMapping.update(input, ordinal)
  }

  def getShuffleHashFunction(layerTag: LayerIdentity): ITuple => Int = ???

  def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity]

}
