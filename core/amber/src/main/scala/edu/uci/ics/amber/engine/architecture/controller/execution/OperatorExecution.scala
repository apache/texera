package edu.uci.ics.amber.engine.architecture.controller.execution

import edu.uci.ics.amber.engine.architecture.controller.execution.ExecutionUtils.aggregateStates
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerExecution
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import edu.uci.ics.amber.engine.architecture.worker.statistics.{PortTupleCountMapping, WorkerState}
import edu.uci.ics.amber.engine.common.executionruntimestate.{OperatorMetrics, OperatorStatistics}
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.core.workflow.PortIdentity

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
    val workerStates = workerExecutions.values.asScala.map(_.getState)
    aggregateStates(
      workerStates,
      WorkerState.COMPLETED,
      WorkerState.RUNNING,
      WorkerState.UNINITIALIZED,
      WorkerState.PAUSED,
      WorkerState.READY
    )
  }

  private[this] def computeOperatorPortStats(
      workerPortStats: Iterable[PortTupleCountMapping]
  ): Seq[PortTupleCountMapping] = {
    workerPortStats
      .map(_.portId)
      .toSet
      .map { portId =>
        PortTupleCountMapping(
          portId,
          workerPortStats.filter(_.portId == portId).map(_.tupleCount).sum
        )
      }
      .toSeq
  }

  def getStats: OperatorMetrics = {
    val workerRawStats = workerExecutions.values.asScala.map(_.getStats)
    val inputPortStats = workerRawStats.flatMap(_.inputTupleCount)
    val outputPortStats = workerRawStats.flatMap(_.outputTupleCount)
    OperatorMetrics(
      getState,
      OperatorStatistics(
        inputCount = computeOperatorPortStats(inputPortStats),
        outputCount = computeOperatorPortStats(outputPortStats),
        getWorkerIds.size,
        dataProcessingTime = workerRawStats.map(_.dataProcessingTime).sum,
        controlProcessingTime = workerRawStats.map(_.controlProcessingTime).sum,
        idleTime = workerRawStats.map(_.idleTime).sum
      )
    )
  }

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
