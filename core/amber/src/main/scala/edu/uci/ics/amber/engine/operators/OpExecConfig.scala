package edu.uci.ics.amber.engine.operators

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{WorkerInfo, WorkerLayer}
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.principal.{OperatorState, OperatorStatistics}
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.common.worker.WorkerState
import edu.uci.ics.amber.engine.common.worker.WorkerState._

import scala.collection.mutable

/**
  * @param id
  */
abstract class OpExecConfig(val id: OperatorIdentity) extends Serializable {

  lazy val topology: Topology = null
  val opExecConfigLogger: WorkflowLogger = WorkflowLogger(s"OpExecConfig $id")
  var inputToOrdinalMapping = new mutable.HashMap[LinkIdentity, Int]()
  var attachedBreakpoints = new mutable.HashMap[String, GlobalBreakpoint[_]]()

  def getAllWorkers: Iterable[ActorVirtualIdentity] = topology.layers.flatMap(l => l.identifiers)

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

  def getOperatorStatistics: OperatorStatistics =
    OperatorStatistics(getState, getInputRowCount, getOutputRowCount)

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

  def getAllWorkerStates: Iterable[WorkerState] = topology.layers.flatMap(l => l.states)

  def getInputRowCount: Long = topology.layers.head.statistics.map(_.inputRowCount).sum

  def getOutputRowCount: Long = topology.layers.last.statistics.map(_.outputRowCount).sum

  def checkStartDependencies(workflow: Workflow): Unit = {
    //do nothing by default
  }

  def requiredShuffle: Boolean = false

  def setInputToOrdinalMapping(input: LinkIdentity, ordinal: Integer): Unit = {
    this.inputToOrdinalMapping.update(input, ordinal)
  }

  def getPartitionColumnIndices(layer: LayerIdentity): Array[Int] = ???

  def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity]

  class Topology(
      var layers: Array[WorkerLayer],
      var links: Array[LinkStrategy]
  ) extends Serializable

}
