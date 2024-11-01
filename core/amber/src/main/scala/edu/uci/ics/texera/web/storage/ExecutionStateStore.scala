package edu.uci.ics.texera.web.storage

import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import edu.uci.ics.amber.engine.common.Utils.maptoStatusCode
import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService
import edu.uci.ics.amber.engine.common.workflowruntimestate.{
  ExecutionBreakpointStore,
  ExecutionConsoleStore,
  ExecutionMetadataStore,
  ExecutionStatsStore
}

import java.sql.Timestamp

object ExecutionStateStore {

  // Update the state of the specified execution if user system is enabled.
  // Update the execution only from backend
  def updateWorkflowState(
      state: WorkflowAggregatedState,
      metadataStore: ExecutionMetadataStore
  ): ExecutionMetadataStore = {
    ExecutionsMetadataPersistService.tryUpdateExistingExecution(metadataStore.executionId) {
      execution =>
        execution.setStatus(maptoStatusCode(state))
        execution.setLastUpdateTime(new Timestamp(System.currentTimeMillis()))
    }
    metadataStore.withState(state)
  }
}

// states that within one execution.
class ExecutionStateStore {
  val statsStore = new StateStore(ExecutionStatsStore())
  val metadataStore = new StateStore(ExecutionMetadataStore())
  val consoleStore = new StateStore(ExecutionConsoleStore())
  val breakpointStore = new StateStore(ExecutionBreakpointStore())
  val reconfigurationStore = new StateStore(ExecutionReconfigurationStore())

  def getAllStores: Iterable[StateStore[_]] = {
    Iterable(statsStore, consoleStore, breakpointStore, metadataStore, reconfigurationStore)
  }
}
