package edu.uci.ics.texera.web

import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.service.WorkflowService
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.disposables.Disposable

import javax.websocket.Session

class SessionState(session: Session) {
  private val observer: Observer[TexeraWebSocketEvent] = new WebsocketSubscriber(session)
  private var currentWorkflowState: Option[WorkflowService] = None
  private var subscription = Disposable.empty()

  def getCurrentWorkflowState: Option[WorkflowService] = currentWorkflowState

  def unsubscribe(): Unit = {
    subscription.dispose()
    if (currentWorkflowState.isDefined) {
      currentWorkflowState.get.disconnect()
      currentWorkflowState = None
    }
  }

  def subscribe(workflowService: WorkflowService): Unit = {
    unsubscribe()
    currentWorkflowState = Some(workflowService)
    subscription = workflowService.connect(observer)
  }
}
