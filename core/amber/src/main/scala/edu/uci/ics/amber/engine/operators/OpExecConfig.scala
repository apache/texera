package edu.uci.ics.amber.engine.operators

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.worker.WorkerStatistics
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.architecture.principal.{OperatorState, OperatorStatistics}
import edu.uci.ics.amber.engine.architecture.principal.OperatorState.OperatorState
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.{
  Completed,
  Paused,
  Ready,
  Running,
  Uninitialized,
  WorkerState
}
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  OperatorIdentity,
  VirtualIdentity
}

import scala.collection.mutable
import scala.concurrent.ExecutionContext

/**
  * @param id
  */
abstract class OpExecConfig(val id: OperatorIdentity) extends Serializable {

  class Topology(
      var layers: Array[WorkerLayer],
      var links: Array[LinkStrategy],
      var dependencies: Map[LayerIdentity, Set[LayerIdentity]]
  ) extends Serializable {
    assert(!dependencies.exists(x => x._2.contains(x._1)))
  }

  lazy val topology: Topology = null
  var inputToOrdinalMapping = new mutable.HashMap[OperatorIdentity, Int]()
  var attachedBreakpoints = new mutable.HashMap[String, GlobalBreakpoint[_]]()
  var results: List[ITuple] = List.empty

  def getState: OperatorState = {
    val workerStates = getAllWorkerStates
    if (workerStates.forall(_ == Uninitialized)) {
      OperatorState.Uninitialized
    } else if (workerStates.forall(_ == Running)) {
      OperatorState.Running
    } else if (workerStates.forall(_ == Paused)) {
      OperatorState.Paused
    } else if (workerStates.forall(_ == Completed)) {
      OperatorState.Completed
    } else if (workerStates.forall(_ == Ready)) {
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

  def setWorkerState(id: ActorVirtualIdentity, state: WorkerState): Unit = {
    val layer: WorkerLayer = getLayerFromWorkerID(id)
    val idx = layer.identifiers.indexOf(id)
    layer.states(idx) = state
  }

  def setAllWorkerState(state: WorkerState): Unit = {
    topology.layers.foreach { layer =>
      (0 until layer.numWorkers).foreach(layer.states.update(_, state))
    }
  }

  def setWorkerStatistics(id: ActorVirtualIdentity, stats: WorkerStatistics): Unit = {
    val layer: WorkerLayer = getLayerFromWorkerID(id)
    val idx = layer.identifiers.indexOf(id)
    layer.statistics(idx) = stats
  }

  def getLayerFromWorkerID(id: ActorVirtualIdentity): WorkerLayer =
    topology.layers.find(_.identifiers.contains(id)).get

  def getInputRowCount: Long = topology.layers.head.statistics.map(_.inputRowCount).sum

  def getOutputRowCount: Long = topology.layers.last.statistics.map(_.outputRowCount).sum

  def getOperatorStatistics: OperatorStatistics =
    OperatorStatistics(getState, getInputRowCount, getOutputRowCount)

  def runtimeCheck(
      workflow: Workflow
  ): Option[mutable.HashMap[VirtualIdentity, mutable.HashMap[VirtualIdentity, mutable.HashSet[
    LayerIdentity
  ]]]] = {
    //do nothing by default
    None
  }

  def requiredShuffle: Boolean = false

  def setInputToOrdinalMapping(input: OperatorIdentity, ordinal: Integer): Unit = {
    this.inputToOrdinalMapping.update(input, ordinal)
  }

  def getInputNum(from: OperatorIdentity): Int = {
    assert(this.inputToOrdinalMapping.contains(from))
    this.inputToOrdinalMapping(from)
  }

  def getShuffleHashFunction(layerTag: LayerIdentity): ITuple => Int = ???

  def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity]

}
