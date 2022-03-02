package edu.uci.ics.texera.web

import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.web.service.WorkflowService
import io.reactivex.rxjava3.disposables.Disposable

import javax.websocket.Session
import scala.collection.mutable

object SessionState {
  private val sessionIdToSessionState = new mutable.HashMap[String, SessionState]()

  def getState(sId: String): SessionState = {
    sessionIdToSessionState(sId)
  }

  def setState(sId: String, state: SessionState): Unit = {
    sessionIdToSessionState.put(sId, state)
  }

  def removeState(sId: String): Unit = {
    sessionIdToSessionState(sId).unsubscribe()
    sessionIdToSessionState.remove(sId)
  }
}

class SessionState(session: Session) {
  private var currentWorkflowState: Option[WorkflowService] = None
  private var workflowSubscription = Disposable.empty()
  private var jobSubscription = Disposable.empty()

  def getCurrentWorkflowState: Option[WorkflowService] = currentWorkflowState

  def unsubscribe(): Unit = {
    workflowSubscription.dispose()
    jobSubscription.dispose()
    if (currentWorkflowState.isDefined) {
      currentWorkflowState.get.disconnect()
      currentWorkflowState = None
    }
  }

  def subscribe(workflowService: WorkflowService): Unit = {
    unsubscribe()
    currentWorkflowState = Some(workflowService)
    workflowSubscription = workflowService.connect(evt =>
      session.getAsyncRemote.sendText(objectMapper.writeValueAsString(evt))
    )
    jobSubscription = workflowService.connectToJob(evt =>
      session.getAsyncRemote.sendText(objectMapper.writeValueAsString(evt))
    )

  }
}
