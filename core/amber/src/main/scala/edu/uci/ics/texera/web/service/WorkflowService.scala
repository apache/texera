package edu.uci.ics.texera.web.service

import java.util.concurrent.ConcurrentHashMap
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.web.model.websocket.event.{
  TexeraWebSocketEvent,
  WorkflowErrorEvent,
  WorkflowExecutionErrorEvent
}
import edu.uci.ics.texera.web.{
  SubscriptionManager,
  TexeraWebApplication,
  WebsocketInput,
  WorkflowLifecycleManager,
  WorkflowStateStore
}
import edu.uci.ics.texera.web.model.websocket.request.{
  TexeraWebSocketRequest,
  WorkflowExecuteRequest,
  WorkflowKillRequest
}
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.disposables.{CompositeDisposable, Disposable}
import io.reactivex.rxjava3.subjects.{BehaviorSubject, Subject}
import org.jooq.types.UInteger

object WorkflowService {
  private val wIdToWorkflowState = new ConcurrentHashMap[String, WorkflowService]()
  val cleanUpDeadlineInSeconds: Int =
    AmberUtils.amberConfig.getInt("web-server.workflow-state-cleanup-in-seconds")
  def getOrCreate(wId: String, cleanupTimeout: Int = cleanUpDeadlineInSeconds): WorkflowService = {
    wIdToWorkflowState.compute(
      wId,
      (_, v) => {
        if (v == null) {
          new WorkflowService(wId, cleanupTimeout)
        } else {
          v
        }
      }
    )
  }
}

class WorkflowService(
    wid: String,
    cleanUpTimeout: Int
) extends SubscriptionManager
    with LazyLogging {
  // state across execution:
  var opResultStorage: OpResultStorage = new OpResultStorage(
    AmberUtils.amberConfig.getString("storage.mode").toLowerCase
  )
  private val errorSubject = BehaviorSubject.create[TexeraWebSocketEvent]().toSerialized
  val errorHandler: Throwable => Unit = { t =>
    {
      t.printStackTrace()
      errorSubject.onNext(
        WorkflowErrorEvent(generalErrors = Map("error" -> (t.getMessage + "\n" + t.getStackTrace.mkString("\n"))))
      )
    }
  }
  val wsInput = new WebsocketInput(errorHandler)
  val stateStore = new WorkflowStateStore()
  val resultService: JobResultService =
    new JobResultService(opResultStorage, stateStore)
  val exportService: ResultExportService = new ResultExportService(opResultStorage)
  val operatorCache: WorkflowCacheService =
    new WorkflowCacheService(opResultStorage, stateStore, wsInput)
  var jobService: Option[WorkflowJobService] = None
  val lifeCycleManager: WorkflowLifecycleManager = new WorkflowLifecycleManager(
    wid,
    cleanUpTimeout,
    () => {
      opResultStorage.close()
      WorkflowService.wIdToWorkflowState.remove(wid)
      wsInput.onNext(WorkflowKillRequest(), None)
      unsubscribeAll()
    }
  )

  addSubscription(
    wsInput.subscribe((evt: WorkflowExecuteRequest, uidOpt) => initJobService(evt, uidOpt))
  )

  def connect(observer: Observer[TexeraWebSocketEvent]): Disposable = {
    lifeCycleManager.increaseUserCount()
    val subscriptions = stateStore.getAllStores
      .map(_.getWebsocketEventObservable)
      .map(evtPub =>
        evtPub.subscribe { evts: Iterable[TexeraWebSocketEvent] => evts.foreach(observer.onNext) }
      )
      .toSeq
    val errorSubscription = errorSubject.subscribe(evt => observer.onNext(evt))
    new CompositeDisposable(subscriptions :+ errorSubscription: _*)
  }

  def disconnect(): Unit = {
    lifeCycleManager.decreaseUserCount(
      stateStore.jobStateStore.getStateThenConsume(_.state)
    )
  }

  def initJobService(req: WorkflowExecuteRequest, uidOpt: Option[UInteger]): Unit = {
    if (jobService.isDefined) {
      //unsubscribe all
      jobService.get.unsubscribeAll()
    }
    val job = new WorkflowJobService(
      stateStore,
      wsInput,
      operatorCache,
      resultService,
      uidOpt,
      req,
      errorHandler
    )
    lifeCycleManager.registerCleanUpOnStateChange(stateStore)
    jobService = Some(job)
    job.startWorkflow()
  }

  override def unsubscribeAll(): Unit = {
    super.unsubscribeAll()
    jobService.foreach(_.unsubscribeAll())
    operatorCache.unsubscribeAll()
    resultService.unsubscribeAll()
  }

}
