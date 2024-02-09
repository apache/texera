package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.scheduling.Region
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState._
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}

import scala.collection.mutable

class ExecutionState(workflow: Workflow) {

  private val operatorExecutions: mutable.Map[PhysicalOpIdentity, OperatorExecution] =
    mutable.HashMap()

  val builtChannels: mutable.Set[ChannelIdentity] = mutable.HashSet[ChannelIdentity]()

  def initOperatorState(
      physicalOpId: PhysicalOpIdentity,
      operatorConfig: OperatorConfig
  ): OperatorExecution = {
    operatorExecutions += physicalOpId -> new OperatorExecution(
      workflow.context.workflowId,
      workflow.context.executionId,
      physicalOpId,
      operatorConfig.workerConfigs.length
    )
    operatorExecutions(physicalOpId)
  }

  def getAllBuiltWorkers: Iterable[ActorVirtualIdentity] =
    operatorExecutions.values
      .flatMap(operator =>
        operator.getBuiltWorkerIds.map(worker => operator.getWorkerExecution(worker))
      )
      .map(_.id)

  def getOperatorExecution(op: PhysicalOpIdentity): OperatorExecution = {
    operatorExecutions(op)
  }
  def getOperatorExecution(worker: ActorVirtualIdentity): OperatorExecution = {
    operatorExecutions.values.foreach { execution =>
      val result = execution.getBuiltWorkerIds.find(x => x == worker)
      if (result.isDefined) {
        return execution
      }
    }
    throw new NoSuchElementException(s"cannot find operator with worker = $worker")
  }
  def getAllOperatorExecutions: Iterable[(PhysicalOpIdentity, OperatorExecution)] =
    operatorExecutions

  def getAllWorkersOfRegion(region: Region): Set[ActorVirtualIdentity] = {
    region.getOperators.flatMap(physicalOp =>
      getOperatorExecution(physicalOp.id).getBuiltWorkerIds.toList
    )
  }

  def getWorkflowStatus: Map[String, OperatorRuntimeStats] = {
    operatorExecutions.map(op => (op._1.logicalOpId.id, op._2.getOperatorStatistics)).toMap
  }

  def isCompleted: Boolean =
    operatorExecutions.values.forall(op => op.getState == WorkflowAggregatedState.COMPLETED)

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
  def getAllWorkersForOperators(
      operators: Set[PhysicalOpIdentity]
  ): Set[ActorVirtualIdentity] = {
    operators.flatMap(physicalOpId => getOperatorExecution(physicalOpId).getBuiltWorkerIds)
  }
  def getPythonWorkerToOperatorExec(
      pythonPhysicalOpIds: Set[PhysicalOpIdentity]
  ): Iterable[(ActorVirtualIdentity, PhysicalOp)] = {
    pythonPhysicalOpIds
      .map(opId => workflow.physicalPlan.getOperator(opId))
      .filter(physicalOp => physicalOp.isPythonOperator)
      .flatMap(physicalOp =>
        getOperatorExecution(physicalOp.id).getBuiltWorkerIds.map(worker => (worker, physicalOp))
      )
  }

}
