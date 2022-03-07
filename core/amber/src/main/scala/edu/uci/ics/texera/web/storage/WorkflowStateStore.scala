package edu.uci.ics.texera.web.storage

import edu.uci.ics.texera.web.workflowcachestate.WorkflowCacheStore
import edu.uci.ics.texera.web.workflowresultstate.WorkflowResultStore

// states that across executions.
class WorkflowStateStore {
  val cacheStore = new StateStore(WorkflowCacheStore())
  val resultStore = new StateStore(WorkflowResultStore())

  def getAllStores: Iterable[StateStore[_]] = {
    Iterable(cacheStore, resultStore)
  }

}
