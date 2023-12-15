package edu.uci.ics.texera.web.storage

import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService

import edu.uci.ics.texera.web.workflowruntimestate.{
  JobBreakpointStore,
  JobMetadataStore,
  JobConsoleStore,
  JobStatsStore,
  WorkflowAggregatedState
}

object ExecutionStateStore {

  // Update the state of the specified execution if user system is enabled.
  // Update the execution only from backend
  def updateWorkflowState(
      state: WorkflowAggregatedState,
      metadataStore: JobMetadataStore
  ): JobMetadataStore = {
    ExecutionsMetadataPersistService.tryUpdateExistingExecution(metadataStore.executionId, state)
    metadataStore.withState(state)
  }
}

// states that within one execution.
class ExecutionStateStore {
  val statsStore = new StateStore(JobStatsStore())
  val metadataStore = new StateStore(JobMetadataStore())
  val consoleStore = new StateStore(JobConsoleStore())
  val breakpointStore = new StateStore(JobBreakpointStore())
  val reconfigurationStore = new StateStore(JobReconfigurationStore())

  def getAllStores: Iterable[StateStore[_]] = {
    Iterable(statsStore, consoleStore, breakpointStore, metadataStore, reconfigurationStore)
  }
}
