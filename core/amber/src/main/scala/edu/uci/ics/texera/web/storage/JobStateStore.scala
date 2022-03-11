package edu.uci.ics.texera.web.storage

import edu.uci.ics.texera.web.workflowruntimestate.{
  JobBreakpointStore,
  JobMetadataStore,
  JobPythonStore,
  JobStatsStore
}

// states that within one execution.
class JobStateStore {
  val statsStore = new StateStore(JobStatsStore())
  val jobMetadataStore = new StateStore(JobMetadataStore())
  val pythonStore = new StateStore(JobPythonStore())
  val breakpointStore = new StateStore(JobBreakpointStore())

  def getAllStores: Iterable[StateStore[_]] = {
    Iterable(statsStore, pythonStore, breakpointStore, jobMetadataStore)
  }
}
