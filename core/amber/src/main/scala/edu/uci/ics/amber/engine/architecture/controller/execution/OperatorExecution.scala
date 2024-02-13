package edu.uci.ics.amber.engine.architecture.controller.execution

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerExecution
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState._
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class OperatorExecution extends Serializable {

  private val workerExecutions =
    new util.concurrent.ConcurrentHashMap[ActorVirtualIdentity, WorkerExecution]()

  var attachedBreakpoints = new mutable.HashMap[String, GlobalBreakpoint[_]]()

  def initWorkerExecution(workerId: ActorVirtualIdentity): Unit = {
    assert(!workerExecutions.contains(workerId))
    workerExecutions.put(workerId, WorkerExecution())
  }
  def getWorkerExecution(workerId: ActorVirtualIdentity): WorkerExecution =
    workerExecutions.get(workerId)


  def getWorkerIds: Set[ActorVirtualIdentity] = workerExecutions.keys().asScala.toSet

  def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Set[ActorVirtualIdentity] = getWorkerIds

  def getState: WorkflowAggregatedState = {
    val workerStates = workerExecutions.values.asScala.map(_.state).toArray
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
      inputCount = workerExecutions.values.asScala.map(_.stats).map(_.inputTupleCount).sum,
      outputCount = workerExecutions.values.asScala.map(_.stats).map(_.outputTupleCount).sum,
      getWorkerIds.size,
      dataProcessingTime =
        workerExecutions.values.asScala.map(_.stats).map(_.dataProcessingTime).sum,
      controlProcessingTime =
        workerExecutions.values.asScala.map(_.stats).map(_.controlProcessingTime).sum,
      idleTime = workerExecutions.values.asScala.map(_.stats).map(_.idleTime).sum
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
