package edu.uci.ics.amber.engine.architecture.controller.execution

import edu.uci.ics.amber.engine.architecture.controller.execution.ExecutionUtils.aggregateStates
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerExecution
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
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
    val workerStates = workerExecutions.values.asScala.map(_.getState.value)
    aggregateStates(
      workerStates,
      WorkerState.COMPLETED.value,
      WorkerState.RUNNING.value,
      WorkerState.UNINITIALIZED.value,
      WorkerState.PAUSED.value,
      WorkerState.READY.value
    )
  }

  def getStats: OperatorRuntimeStats =
    OperatorRuntimeStats(
      getState,
      inputCount = workerExecutions.values.asScala
        .flatMap(_.getStats.inputTupleCount)
        .groupBy(_._1)
        .view
        .mapValues(_.map(_._2).sum)
        .toMap,
      outputCount = workerExecutions.values.asScala
        .flatMap(_.getStats.outputTupleCount)
        .groupBy(_._1)
        .view
        .mapValues(_.map(_._2).sum)
        .toMap,
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
