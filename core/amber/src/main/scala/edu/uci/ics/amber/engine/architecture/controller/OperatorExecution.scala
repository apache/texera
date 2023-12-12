package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{WorkerInfo, WorkerWorkloadInfo}
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState._
import edu.uci.ics.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.ambermessage.ChannelID
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

class OperatorExecution(val executionId: Long, layerIdentity: LayerIdentity, numWorkers: Int)
    extends Serializable {
  /*
   * Variables related to runtime information
   */

  // workers of this operator
  private val workers =
    new util.concurrent.ConcurrentHashMap[ActorVirtualIdentity, WorkerInfo]()

  var attachedBreakpoints = new mutable.HashMap[String, GlobalBreakpoint[_]]()
  var workerToWorkloadInfo = new mutable.HashMap[ActorVirtualIdentity, WorkerWorkloadInfo]()

  def states: Array[WorkerState] = workers.values.asScala.map(_.state).toArray

  def statistics: Array[WorkerStatistics] = workers.values.asScala.map(_.stats).toArray

  def getWorkerInfo(id: ActorVirtualIdentity): WorkerInfo = {
    if (!workers.containsKey(id)) {
      workers.put(
        id,
        WorkerInfo(
          id,
          UNINITIALIZED,
          WorkerStatistics(UNINITIALIZED, 0, 0),
          mutable.HashSet(ChannelID(CONTROLLER, id, isControl = true)),
          null
        )
      )
    }
    workers.get(id)
  }

  def getWorkerWorkloadInfo(id: ActorVirtualIdentity): WorkerWorkloadInfo = {
    if (!workerToWorkloadInfo.contains(id)) {
      workerToWorkloadInfo(id) = WorkerWorkloadInfo(0L, 0L)
    }
    workerToWorkloadInfo(id)
  }

  def getAllWorkerStates: Iterable[WorkerState] = states

  def getInputRowCount: Long = statistics.map(_.inputTupleCount).sum

  def getOutputRowCount: Long = statistics.map(_.outputTupleCount).sum

  def getBuiltWorkerIds: Array[ActorVirtualIdentity] = workers.values.asScala.map(_.id).toArray

  def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity] = {
    getBuiltWorkerIds
  }

  def setAllWorkerState(state: WorkerState): Unit = {
    (0 until numWorkers).foreach { i =>
      getWorkerInfo(
        VirtualIdentityUtils.createWorkerIdentity(executionId, layerIdentity, i)
      ).state = state
    }
  }

  def getState: WorkflowAggregatedState = {
    val workerStates = getAllWorkerStates
    if (workerStates.isEmpty) {
      return WorkflowAggregatedState.UNINITIALIZED
    }
    if (workerStates.forall(_ == COMPLETED)) {
      return WorkflowAggregatedState.COMPLETED
    }
    if (workerStates.exists(_ == RUNNING)) {
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

  def getOperatorStatistics: OperatorRuntimeStats =
    OperatorRuntimeStats(getState, getInputRowCount, getOutputRowCount)
}
