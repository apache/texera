package edu.uci.ics.texera.web.storage

import edu.uci.ics.amber.core.storage.result.WorkflowResultStore

// states that across executions.
class WorkflowStateStore {
  val resultStore = new StateStore(WorkflowResultStore())

  def getAllStores: Iterable[StateStore[_]] = {
    Iterable(resultStore)
  }

}
