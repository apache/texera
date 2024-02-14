package edu.uci.ics.amber.engine.architecture.controller.execution

import edu.uci.ics.amber.engine.architecture.scheduling.Region
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, PhysicalOpIdentity}
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}

import scala.collection.mutable

case class RegionExecution(region: Region) {

  private val operatorExecutions: mutable.Map[PhysicalOpIdentity, OperatorExecution] =
    mutable.HashMap()

  def initOperatorExecution(
      physicalOpId: PhysicalOpIdentity,
      existingOpExecution: Option[OperatorExecution] = None
  ): OperatorExecution = {
    assert(!operatorExecutions.contains(physicalOpId))

    operatorExecutions.getOrElseUpdate(
      physicalOpId,
      existingOpExecution.getOrElse(new OperatorExecution())
    )
  }

  def getAllBuiltWorkers: Iterable[ActorVirtualIdentity] =
    operatorExecutions.values.flatMap(operator => operator.getWorkerIds)

  def getOperatorExecution(opId: PhysicalOpIdentity): OperatorExecution = operatorExecutions(opId)

  def hasOperatorExecution(opId: PhysicalOpIdentity): Boolean = operatorExecutions.contains(opId)

  def getAllOperatorExecutions: Iterable[(PhysicalOpIdentity, OperatorExecution)] =
    operatorExecutions

  def getStats: Map[String, OperatorRuntimeStats] = {
    // TODO: fix the aggregation here. The stats should be on port level.
    operatorExecutions.map {
      case (physicalOpId, operatorExecution) =>
        physicalOpId.logicalOpId.id -> operatorExecution.getStats
    }.toMap
  }

  def isCompleted: Boolean = getState == WorkflowAggregatedState.COMPLETED

  def getState: WorkflowAggregatedState = {
    if (
      region.getPorts.forall(globalPortId => {
        val operatorExecution = this.getOperatorExecution(globalPortId.opId)
        if (globalPortId.input) operatorExecution.isInputPortCompleted(globalPortId.portId)
        else operatorExecution.isOutputPortCompleted(globalPortId.portId)
      })
    ) {
      WorkflowAggregatedState.COMPLETED
    } else {
      WorkflowAggregatedState.RUNNING
    }
  }

}
