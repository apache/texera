package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.architecture.controller.PortExecution
import edu.uci.ics.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

import scala.collection.mutable

// TODO: remove redundant info
case class WorkerExecution(
    id: ActorVirtualIdentity,
    var state: WorkerState,
    var stats: WorkerStatistics,
    inputPortExecutions: mutable.HashMap[PortIdentity, PortExecution] = mutable.HashMap(),
    outputPortExecutions: mutable.HashMap[PortIdentity, PortExecution] = mutable.HashMap()
) extends Serializable {

  def getInputPortExecution(portId: PortIdentity): PortExecution = {
    if (!inputPortExecutions.contains(portId)) {
      inputPortExecutions(portId) = new PortExecution()
    }
    inputPortExecutions(portId)

  }

  def getOutputPortExecution(portId: PortIdentity): PortExecution = {
    if (!outputPortExecutions.contains(portId)) {
      outputPortExecutions(portId) = new PortExecution()
    }
    outputPortExecutions(portId)

  }
}
