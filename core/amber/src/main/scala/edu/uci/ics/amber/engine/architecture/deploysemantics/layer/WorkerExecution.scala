package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.architecture.controller.execution.WorkerPortExecution
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.UNINITIALIZED
import edu.uci.ics.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

import scala.collection.mutable

// TODO: remove redundant info
case class WorkerExecution(
    inputPortExecutions: mutable.HashMap[PortIdentity, WorkerPortExecution] = mutable.HashMap(),
    outputPortExecutions: mutable.HashMap[PortIdentity, WorkerPortExecution] = mutable.HashMap()
) extends Serializable {

  var state: WorkerState = UNINITIALIZED
  // TODO: move stats onto ports, and make this as an aggregation func.
  var stats: WorkerStatistics = WorkerStatistics(UNINITIALIZED, 0, 0, 0, 0, 0)

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
