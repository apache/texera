package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.Address
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{OpExecConfig, WorkerInfo}
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity
}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState._
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}
import edu.uci.ics.texera.workflow.operators.udf.python.PythonUDFOpExecV2

class ExecutionState(workflow: Workflow) {

  private val linkExecutions: Map[LinkIdentity, LinkExecution] =
    workflow.physicalPlan.linkStrategies.map { link =>
      link._1 -> new LinkExecution(link._2.totalReceiversCount)
    }
  private val operatorExecutions: Map[LayerIdentity, OperatorExecution] =
    workflow.getAllOperators.map { opConf =>
      opConf.id -> new OperatorExecution(opConf.id, opConf.numWorkers)
    }.toMap

  def getAllBuiltWorkers: Iterable[ActorVirtualIdentity] =
    operatorExecutions.values
      .flatMap(operator => operator.getBuiltWorkerIds.map(worker => operator.getWorkerInfo(worker)))
      .map(_.id)

  def getOperatorExecution(op: LayerIdentity): OperatorExecution = {
    operatorExecutions(op)
  }

  def getBuiltPythonWorkers: Iterable[ActorVirtualIdentity] =
    workflow.physicalPlan.operators
      .filter(operator => operator.opExecClass == classOf[PythonUDFOpExecV2])
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

  def getLinkExecution(link: LinkIdentity): LinkExecution = linkExecutions(link)

  def getAllOperatorExecutions: Iterable[(LayerIdentity, OperatorExecution)] = operatorExecutions

  def getAllWorkersOfRegion(region: PipelinedRegion): Array[ActorVirtualIdentity] = {
    val allOperatorsInRegion =
      region.getOperators() ++ region.blockingDownstreamOperatorsInOtherRegions.map(_._1)

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
    operatorExecutions.map(op => (op._1.operator, op._2.getOperatorStatistics)).toMap
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

  def getOperatorToWorkers: Iterable[(LayerIdentity, Seq[ActorVirtualIdentity])] = {
    workflow.physicalPlan.allOperatorIds.map(opId => {
      (opId, getAllWorkersForOperators(Array(opId)).toSeq)
    })
  }

  def getAllWorkersForOperators(
      operators: Array[LayerIdentity]
  ): Array[ActorVirtualIdentity] = {
    operators.flatMap(opId => getOperatorExecution(opId).getBuiltWorkerIds)
  }

  def getPythonOperators(fromOperatorsList: Array[LayerIdentity]): Array[LayerIdentity] = {
    fromOperatorsList.filter(opId =>
      getOperatorExecution(opId).getBuiltWorkerIds.nonEmpty &&
        workflow.physicalPlan.operatorMap(opId).isPythonOperator
    )
  }

  def getPythonWorkerToOperatorExec(
      pythonOperators: Array[LayerIdentity]
  ): Iterable[(ActorVirtualIdentity, OpExecConfig)] = {
    pythonOperators
      .map(opId => workflow.physicalPlan.operatorMap(opId))
      .filter(op => op.isPythonOperator)
      .flatMap(op => getOperatorExecution(op.id).getBuiltWorkerIds.map(worker => (worker, op)))
      .toList
  }

}
