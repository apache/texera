package edu.uci.ics.amber.engine.architecture.controller.execution

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerExecution
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState._
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}

import java.util
import scala.jdk.CollectionConverters._

case class OperatorExecution() {

  private val workerExecutions =
    new util.concurrent.ConcurrentHashMap[ActorVirtualIdentity, WorkerExecution]()

  /**
    * Initializes a `WorkerExecution` for the specified workerId and adds it to the workerExecutions map.
    * If a `WorkerExecution` for the given workerId already exists, an AssertionError is thrown.
    * After successfully adding the new `WorkerExecution`, it retrieves and returns the newly added instance.
    *
    * @param workerId The `ActorVirtualIdentity` representing the unique identity of the worker.
    * @return The `WorkerExecution` instance associated with the specified workerId.
    * @throws AssertionError if a `WorkerExecution` already exists for the given workerId.
    */
  def initWorkerExecution(workerId: ActorVirtualIdentity): WorkerExecution = {
    assert(
      !workerExecutions.contains(workerId),
      s"WorkerExecution already exists for workerId: $workerId"
    )
    workerExecutions.put(workerId, WorkerExecution())
    getWorkerExecution(workerId)
  }

  /**
    * Retrieves the `WorkerExecution` instance associated with the specified workerId.
    */
  def getWorkerExecution(workerId: ActorVirtualIdentity): WorkerExecution =
    workerExecutions.get(workerId)

  /**
    * Retrieves the set of all workerIds for which `WorkerExecution` instances have been initialized.
    */
  def getWorkerIds: Set[ActorVirtualIdentity] = workerExecutions.keys.asScala.toSet

  def getState: WorkflowAggregatedState = {
    val workerStates = workerExecutions.values.asScala.map(_.getState).toArray
    if (workerStates.isEmpty) {
      return WorkflowAggregatedState.UNINITIALIZED
    }
    if (workerStates.forall(_ == COMPLETED)) {
      return WorkflowAggregatedState.COMPLETED
    }
    if (workerStates.contains(RUNNING)) {
      return WorkflowAggregatedState.RUNNING
    }
    val unCompletedWorkerStates = workerStates.filter(_ != COMPLETED)
    if (unCompletedWorkerStates.forall(_ == UNINITIALIZED)) {
      WorkflowAggregatedState.UNINITIALIZED
    } else if (unCompletedWorkerStates.forall(_ == PAUSED)) {
      WorkflowAggregatedState.PAUSED
    } else if (unCompletedWorkerStates.forall(_ == READY)) {
      WorkflowAggregatedState.READY
    } else {
      WorkflowAggregatedState.UNKNOWN
    }
  }

  def getStats: OperatorRuntimeStats =
    OperatorRuntimeStats(
      getState,
      inputCount = workerExecutions.values.asScala.map(_.getStats).map(_.inputTupleCount).sum,
      outputCount = workerExecutions.values.asScala.map(_.getStats).map(_.outputTupleCount).sum,
      getWorkerIds.size,
      dataProcessingTime =
        workerExecutions.values.asScala.map(_.getStats).map(_.dataProcessingTime).sum,
      controlProcessingTime =
        workerExecutions.values.asScala.map(_.getStats).map(_.controlProcessingTime).sum,
      idleTime = workerExecutions.values.asScala.map(_.getStats).map(_.idleTime).sum
    )

  def isInputPortCompleted(portId: PortIdentity): Boolean = {
    workerExecutions
      .values()
      .asScala
      .map(workerExecution => workerExecution.getInputPortExecution(portId))
      .forall(_.completed)
  }

  def isOutputPortCompleted(portId: PortIdentity): Boolean = {
    workerExecutions
      .values()
      .asScala
      .map(workerExecution => workerExecution.getOutputPortExecution(portId))
      .forall(_.completed)
  }
}
