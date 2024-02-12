package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState._
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}

import scala.collection.mutable

class WorkflowExecution {

  private val operatorExecutions: mutable.Map[PhysicalOpIdentity, OperatorExecution] =
    mutable.HashMap()

  val builtChannels: mutable.Set[ChannelIdentity] = mutable.HashSet[ChannelIdentity]()

  def initOperatorState(
      physicalOpId: PhysicalOpIdentity
  ): OperatorExecution = {
    assert(!operatorExecutions.contains(physicalOpId))
    operatorExecutions.getOrElseUpdate(physicalOpId, new OperatorExecution())
  }

  def getAllBuiltWorkers: Iterable[ActorVirtualIdentity] =
    operatorExecutions.values
      .flatMap(operator => operator.getWorkerIds.map(worker => operator.getWorkerExecution(worker)))
      .map(_.id)

  def getOperatorExecution(opId: PhysicalOpIdentity): OperatorExecution = operatorExecutions(opId)

  def hasOperatorExecution(opId: PhysicalOpIdentity): Boolean = operatorExecutions.contains(opId)

  def getAllOperatorExecutions: Iterable[(PhysicalOpIdentity, OperatorExecution)] =
    operatorExecutions

  def getStats: Map[String, OperatorRuntimeStats] = {
    operatorExecutions.map {
      case (physicalOpId, operatorExecution) =>
        physicalOpId.logicalOpId.id -> operatorExecution.getStats
    }.toMap
  }

  def isCompleted: Boolean = getState == WorkflowAggregatedState.COMPLETED

  def getState: WorkflowAggregatedState = {
    val opStates = operatorExecutions.values.map(_.getState)
    if (opStates.isEmpty) {
      return WorkflowAggregatedState.UNINITIALIZED
    }
    if (opStates.forall(_ == COMPLETED)) {
      return WorkflowAggregatedState.COMPLETED
    }
    if (opStates.exists(_ == RUNNING)) {
      return WorkflowAggregatedState.RUNNING
    }
    val unCompletedOpStates = opStates.filter(_ != COMPLETED)
    val runningOpStates = unCompletedOpStates.filter(_ != UNINITIALIZED)
    if (unCompletedOpStates.forall(_ == UNINITIALIZED)) {
      return WorkflowAggregatedState.UNINITIALIZED
    }
    if (runningOpStates.forall(_ == PAUSED)) {
      WorkflowAggregatedState.PAUSED
    } else if (runningOpStates.forall(_ == READY)) {
      WorkflowAggregatedState.READY
    } else {
      WorkflowAggregatedState.UNKNOWN
    }
  }

}
