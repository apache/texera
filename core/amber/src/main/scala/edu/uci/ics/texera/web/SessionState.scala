package edu.uci.ics.texera.web

import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.resource.WebsocketSubscriber
import edu.uci.ics.texera.web.service.WorkflowService
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
  private var subscription: Subscription = Subscription()
  private val observer: Observer[TexeraWebSocketEvent] = new WebsocketSubscriber(session)
  private var currentWorkflowState: Option[WorkflowService] = None

  def getCurrentWorkflowState: Option[WorkflowService] = currentWorkflowState

  def unbind(): Unit = {
    subscription.unsubscribe()
    if (currentWorkflowState.isDefined) {
      currentWorkflowState.get.disconnect()
      currentWorkflowState = None
    }
  }

  def bind(workflowService: WorkflowService): Unit = {
    unbind()
    currentWorkflowState = Some(workflowService)
    workflowService.connect()
    val opCacheSubscription = SnapshotMulticast.syncState(workflowService.operatorCache, observer)
    subscription = CompositeSubscription(
      opCacheSubscription,
      workflowService.getJobServiceObservable.subscribe(jobService => {
        subscription = CompositeSubscription(
          jobService.subscribeRuntimeComponents(observer),
          jobService.subscribe(observer),
          subscription
        )
      })
    )
  }
}
