package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.Address
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerInfo
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  PhysicalLinkIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState._
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}

class ExecutionState(workflow: Workflow) {

  private val linkExecutions: Map[PhysicalLinkIdentity, LinkExecution] =
    workflow.physicalPlan.links.map { link =>
      link.id -> new LinkExecution(link.totalReceiversCount)
    }.toMap
  private val operatorExecutions: Map[PhysicalOpIdentity, OperatorExecution] =
    workflow.physicalPlan.operators.map { physicalOp =>
      physicalOp.id -> new OperatorExecution(
        executionId = workflow.workflowId.executionId,
        physicalOp.id,
        physicalOp.numWorkers
      )
    }.toMap

  def getAllBuiltWorkers: Iterable[ActorVirtualIdentity] =
    operatorExecutions.values
      .flatMap(operator => operator.getBuiltWorkerIds.map(worker => operator.getWorkerInfo(worker)))
      .map(_.id)

  def getOperatorExecution(op: PhysicalOpIdentity): OperatorExecution = {
    operatorExecutions(op)
  }

  def getBuiltPythonWorkers: Iterable[ActorVirtualIdentity] =
    workflow.physicalPlan.operators
      .filter(operator => operator.isPythonOperator)
      .flatMap(op => getOperatorExecution(op.id).getBuiltWorkerIds)

  def getOperatorExecution(worker: ActorVirtualIdentity): OperatorExecution = {
    operatorExecutions.values.foreach { execution =>
      val result = execution.getBuiltWorkerIds.find(x => x == worker)
      if (result.isDefined) {
        return execution
      }
    }
    throw new NoSuchElementException(s"cannot find operator with worker = $worker")
  }

  def getLinkExecution(link: PhysicalLinkIdentity): LinkExecution = linkExecutions(link)

  def getAllOperatorExecutions: Iterable[(PhysicalOpIdentity, OperatorExecution)] =
    operatorExecutions

  def getAllWorkersOfRegion(region: PipelinedRegion): Array[ActorVirtualIdentity] = {
    val allOperatorsInRegion =
      region.getOperators ++ region.blockingDownstreamPhysicalOpIdsInOtherRegions.map(_._1)

    allOperatorsInRegion.flatMap(opId => getOperatorExecution(opId).getBuiltWorkerIds.toList)
  }

  def getAllWorkerInfoOfAddress(address: Address): Iterable[WorkerInfo] = {
    operatorExecutions.values
      .flatMap(x => {
        x.getBuiltWorkerIds.map(x.getWorkerInfo)
      })
      .filter(info => info.ref.path.address == address)
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

  def physicalOpToWorkersMapping: Iterable[(PhysicalOpIdentity, Seq[ActorVirtualIdentity])] = {
    workflow.physicalPlan.operators
      .map(physicalOp => physicalOp.id)
      .map(physicalOpId => {
        (physicalOpId, getAllWorkersForOperators(Array(physicalOpId)).toSeq)
      })
  }

  def getAllWorkersForOperators(
      operators: Array[PhysicalOpIdentity]
  ): Array[ActorVirtualIdentity] = {
    operators.flatMap(physicalOpId => getOperatorExecution(physicalOpId).getBuiltWorkerIds)
  }

  def filterPythonPhysicalOpIds(
      fromOperatorsList: Array[PhysicalOpIdentity]
  ): Array[PhysicalOpIdentity] = {
    fromOperatorsList.filter(physicalOpId =>
      getOperatorExecution(physicalOpId).getBuiltWorkerIds.nonEmpty &&
        workflow.physicalPlan.getOperator(physicalOpId).isPythonOperator
    )
  }

  def getPythonWorkerToOperatorExec(
      pythonPhysicalOpIds: Array[PhysicalOpIdentity]
  ): Iterable[(ActorVirtualIdentity, PhysicalOp)] = {
    pythonPhysicalOpIds
      .map(opId => workflow.physicalPlan.getOperator(opId))
      .filter(physicalOp => physicalOp.isPythonOperator)
      .flatMap(physicalOp =>
        getOperatorExecution(physicalOp.id).getBuiltWorkerIds.map(worker => (worker, physicalOp))
      )
      .toList
  }

}
