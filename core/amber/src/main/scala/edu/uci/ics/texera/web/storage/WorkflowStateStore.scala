package edu.uci.ics.texera.web.storage

// states that across executions.
class WorkflowStateStore {
  val resultStore = new StateStore(WorkflowResultStore())

  def getAllStores: Iterable[StateStore[_]] = {
    Iterable(resultStore)
  }

}
