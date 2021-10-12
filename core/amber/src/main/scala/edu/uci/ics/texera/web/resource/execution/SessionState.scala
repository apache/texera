package edu.uci.ics.texera.web.resource.execution

import edu.uci.ics.texera.web.model.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.resource.WebsocketSubscriber
import javax.websocket.Session
import rx.lang.scala.subscriptions.CompositeSubscription
import rx.lang.scala.{Observer, Subscription}

import scala.collection.mutable

object SessionState {
  private val sessionIdToSessionState = new mutable.HashMap[String, SessionState]()

  def getState(sId: String): SessionState = {
    sessionIdToSessionState(sId)
  }

  def registerState(sId: String, state: SessionState): Unit = {
    sessionIdToSessionState.put(sId, state)
  }

  def unregisterState(sId: String): Unit = {
    sessionIdToSessionState(sId).unbind()
    sessionIdToSessionState.remove(sId)
  }
}

class SessionState(session: Session) {
  private var wId: String = _
  private var subscription: Subscription = Subscription()
  private val observer: Observer[TexeraWebSocketEvent] = new WebsocketSubscriber(session)
  private var currentWorkflowState: Option[WorkflowState] = None

  def getCurrentWorkflowState: Option[WorkflowState] = currentWorkflowState

  def changeWorkflow(newWId: String): Unit = {
    wId = newWId
  }

  def unbind(): Unit = {
    subscription.unsubscribe()
    if (currentWorkflowState.isDefined) {
      currentWorkflowState.get.disconnect()
    }
  }

  def bind(executionState: WorkflowState): Unit = {
    unbind()
    currentWorkflowState = Some(executionState)
    executionState.connect()
    val opCacheSubscription = SnapshotMulticast.syncState(executionState.operatorCache, observer)
    executionState.executionState match {
      case Some(value) =>
        subscription = CompositeSubscription(value.subscribeAll(observer), opCacheSubscription)
      case None =>
        subscription = opCacheSubscription
    }
  }
}
