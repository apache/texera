package edu.uci.ics.texera.web.storage

import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService

import edu.uci.ics.texera.web.workflowruntimestate.{
  JobBreakpointStore,
  JobMetadataStore,
  JobPythonStore,
  JobStatsStore,
  WorkflowAggregatedState
}

object JobStateStore {

  // Update the state of the specified execution if user system is enabled.
  // Update the execution only from backend
  def updateWorkflowState(
      state: WorkflowAggregatedState,
      metadataStore: JobMetadataStore
  ): JobMetadataStore = {
    ExecutionsMetadataPersistService.tryUpdateExistingExecution(metadataStore.eid, state)
    metadataStore.withState(state)
  }
}

// states that within one execution.
class JobStateStore {
  val statsStore = new StateStore(JobStatsStore())
  val jobMetadataStore = new StateStore(JobMetadataStore())
  val pythonStore = new StateStore(JobPythonStore())
  val breakpointStore = new StateStore(JobBreakpointStore())
  val reconfigurationStore = new StateStore(JobReconfigurationStore())

  def getAllStores: Iterable[StateStore[_]] = {
    Iterable(statsStore, pythonStore, breakpointStore, jobMetadataStore, reconfigurationStore)
  }
}
