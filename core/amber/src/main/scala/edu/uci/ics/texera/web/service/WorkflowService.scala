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
  WebsocketOutput,
  WorkflowLifecycleManager,
  WorkflowStateStore
}
import edu.uci.ics.texera.web.model.websocket.request.{
  TexeraWebSocketRequest,
  WorkflowExecuteRequest,
  WorkflowKillRequest
}
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import org.jooq.types.UInteger
import rx.lang.scala.subjects.BehaviorSubject
import rx.lang.scala.{Observable, Observer, Subject, Subscription}

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
  val errorHandler: Throwable => Unit = { t =>
    {
      t.printStackTrace()
      wsOutput.onNext(
        WorkflowExecutionErrorEvent(t.getStackTrace.mkString("\n"))
      )
    }
  }
  val wsOutput = new WebsocketOutput()
  val wsInput = new WebsocketInput(errorHandler)
  val stateStore = new WorkflowStateStore(wsOutput)
  val resultService: JobResultService =
    new JobResultService(opResultStorage, stateStore, wsOutput)
  val exportService: ResultExportService = new ResultExportService(opResultStorage)
  val operatorCache: WorkflowCacheService =
    new WorkflowCacheService(opResultStorage, stateStore, wsInput, wsOutput)
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
  private val jobStateSubject = BehaviorSubject[WorkflowJobService]()

  addSubscription(
    wsInput.subscribe((evt: WorkflowExecuteRequest, uidOpt) => initJobService(evt, uidOpt))
  )

  def connect(observer: Observer[TexeraWebSocketEvent]): Subscription = {
    lifeCycleManager.increaseUserCount()
    stateStore.fetchSnapshotThenSendTo(observer)
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
      wsOutput,
      operatorCache,
      resultService,
      uidOpt,
      req,
      errorHandler
    )
    lifeCycleManager.registerCleanUpOnStateChange(stateStore)
    jobService = Some(job)
    jobStateSubject.onNext(job)
    job.startWorkflow()
  }

  def getJobServiceObservable: Observable[WorkflowJobService] = jobStateSubject.onTerminateDetach

  override def unsubscribeAll(): Unit = {
    super.unsubscribeAll()
    jobService.foreach(_.unsubscribeAll())
    operatorCache.unsubscribeAll()
    resultService.unsubscribeAll()
  }

}
