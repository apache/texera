package edu.uci.ics.texera.web.storage

import edu.uci.ics.texera.web.workflowresultstate.WorkflowResultStore

// states that across executions.
class WorkflowStateStore {

  val resultStore = new StateStore(WorkflowResultStore())

  def getAllStores: Iterable[StateStore[_]] = {
    Iterable(resultStore)
  }

}
