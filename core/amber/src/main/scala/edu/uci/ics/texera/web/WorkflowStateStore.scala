package edu.uci.ics.texera.web

import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.workflowcachestate.WorkflowCacheStore
import edu.uci.ics.texera.web.workflowresultstate.WorkflowResultStore
import edu.uci.ics.texera.web.workflowruntimestate.{
  JobBreakpointStore,
  JobPythonStore,
  JobStateStore,
  JobStatsStore
}
import rx.lang.scala.{Observable, Observer, Subject, Subscription}

class WorkflowStateStore {
  val cacheStore = new StateStore(WorkflowCacheStore())
  val statsStore = new StateStore(JobStatsStore())
  val jobStateStore = new StateStore(JobStateStore())
  val pythonStore = new StateStore(JobPythonStore())
  val breakpointStore = new StateStore(JobBreakpointStore())
  val resultStore = new StateStore(WorkflowResultStore())

  def getAllStores: Iterable[StateStore[_]] = {
    Iterable(cacheStore, statsStore, pythonStore, breakpointStore, resultStore, jobStateStore)
  }

}
