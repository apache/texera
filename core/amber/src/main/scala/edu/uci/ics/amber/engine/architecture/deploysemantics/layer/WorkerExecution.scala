package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.architecture.controller.execution.WorkerPortExecution
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.UNINITIALIZED
import edu.uci.ics.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.workflow.PortIdentity

import scala.collection.mutable

case class WorkerExecution() extends Serializable {

  private val inputPortExecutions: mutable.HashMap[PortIdentity, WorkerPortExecution] =
    mutable.HashMap()
  private val outputPortExecutions: mutable.HashMap[PortIdentity, WorkerPortExecution] =
    mutable.HashMap()

  private var state: WorkerState = UNINITIALIZED
  private var stats: WorkerStatistics = WorkerStatistics(Seq(), Seq(), 0, 0, 0)

  def getState: WorkerState = state

  def setState(state: WorkerState): Unit = {
    this.state = state
  }

  def getStats: WorkerStatistics = stats

  def setStats(stats: WorkerStatistics): Unit = {
    this.stats = stats
  }

  def getInputPortExecution(portId: PortIdentity): WorkerPortExecution = {
    if (!inputPortExecutions.contains(portId)) {
      inputPortExecutions(portId) = new WorkerPortExecution()
    }
    inputPortExecutions(portId)

  }

  def getOutputPortExecution(portId: PortIdentity): WorkerPortExecution = {
    if (!outputPortExecutions.contains(portId)) {
      outputPortExecutions(portId) = new WorkerPortExecution()
    }
    outputPortExecutions(portId)

  }
}
