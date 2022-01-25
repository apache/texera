package edu.uci.ics.texera.web

import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.service.WorkflowService
import javax.websocket.Session
import rx.lang.scala.subscriptions.CompositeSubscription
import rx.lang.scala.{Observer, Subscription}

import scala.collection.mutable

class SessionState(session: Session) {
  private var operatorCacheSubscription: Subscription = Subscription()
  private var jobSubscription: Subscription = Subscription()
  private var jobUpdateSubscription: Subscription = Subscription()
  private val observer: Observer[TexeraWebSocketEvent] = new WebsocketSubscriber(session)
  private var currentWorkflowState: Option[WorkflowService] = None

  def getCurrentWorkflowState: Option[WorkflowService] = currentWorkflowState

  def unsubscribe(): Unit = {
    operatorCacheSubscription.unsubscribe()
    jobSubscription.unsubscribe()
    jobUpdateSubscription.unsubscribe()
    if (currentWorkflowState.isDefined) {
      currentWorkflowState.get.disconnect()
      currentWorkflowState = None
    }
  }

  def subscribe(workflowService: WorkflowService): Unit = {
    unsubscribe()
    currentWorkflowState = Some(workflowService)
    workflowService.connect()
    operatorCacheSubscription = workflowService.operatorCache.subscribe(observer)
    jobUpdateSubscription = workflowService.getJobServiceObservable.subscribe(jobService => {
      jobSubscription.unsubscribe()
      jobSubscription = CompositeSubscription(
        jobService.subscribeRuntimeComponents(observer),
        jobService.subscribe(observer)
      )
    })
  }
}
