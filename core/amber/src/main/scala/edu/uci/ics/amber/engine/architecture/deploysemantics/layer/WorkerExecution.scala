package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.architecture.controller.WorkerPortExecution
import edu.uci.ics.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

import scala.collection.mutable

// TODO: remove redundant info
case class WorkerExecution(
    id: ActorVirtualIdentity,
    var state: WorkerState,
    var stats: WorkerStatistics,
    inputPortExecutions: mutable.HashMap[PortIdentity, WorkerPortExecution] = mutable.HashMap(),
    outputPortExecutions: mutable.HashMap[PortIdentity, WorkerPortExecution] = mutable.HashMap()
) extends Serializable {

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
