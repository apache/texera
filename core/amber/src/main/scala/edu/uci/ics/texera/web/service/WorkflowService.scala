package edu.uci.ics.texera.web.service

import java.util.concurrent.ConcurrentHashMap
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowExecutions
import edu.uci.ics.texera.web.model.websocket.event.{OperatorStatistics, TexeraWebSocketEvent, WorkflowErrorEvent}
import edu.uci.ics.texera.web.{SubscriptionManager, WebsocketInput, WorkflowLifecycleManager}
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowExecutionsResource.{ExecutionContent, getExecutionById, getLatestExecution}
import edu.uci.ics.texera.web.model.websocket.request.{CacheStatusUpdateRequest, CompareEIdExecutionRequest, RegisterEIdRequest, WorkflowExecuteRequest, WorkflowKillRequest}
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowVersionResource.getLatestVersion
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource
import edu.uci.ics.texera.web.service.WorkflowService.mkWorkflowStateId
import edu.uci.ics.texera.web.storage.{StatStorage, WorkflowStateStore}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowVersionResource.isVersionInRangeUnimportant
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService.maptoAggregatedState
import edu.uci.ics.texera.web.service.JobResultService.WebResultUpdate
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.{DBWorkflowToLogicalPlan, OperatorLink, WorkflowInfo}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import io.reactivex.rxjava3.disposables.{CompositeDisposable, Disposable}
import io.reactivex.rxjava3.subjects.BehaviorSubject
import org.jooq.types.UInteger

import scala.collection.mutable

object WorkflowService {
  private val wIdToWorkflowState = new ConcurrentHashMap[String, WorkflowService]()
  final val userSystemEnabled: Boolean = AmberUtils.amberConfig.getBoolean("user-sys.enabled")
  val cleanUpDeadlineInSeconds: Int =
    AmberUtils.amberConfig.getInt("web-server.workflow-state-cleanup-in-seconds")

  def mkWorkflowStateId(wId: Int, uidOpt: Option[UInteger]): String = {
    uidOpt match {
      case Some(user) =>
        wId.toString
      case None =>
        // use a fixed wid for reconnection
        "dummy wid"
    }
  }
  def getOrCreate(
      wId: Int,
      uidOpt: Option[UInteger],
      cleanupTimeout: Int = cleanUpDeadlineInSeconds
  ): WorkflowService = {
    wIdToWorkflowState.compute(
      mkWorkflowStateId(wId, uidOpt),
      (_, workflowService) => {
        // either it was never executed or cleared from memory
        if (workflowService == null) {
          retrieveFromStorageOrCreateNew(wId, uidOpt, cleanupTimeout)
        }
        // it exists in memory
        else {
          validateWorkflowVersion(wId, workflowService, uidOpt, cleanupTimeout)
        }
      }
    )
  }

  /**
   * Function retrieves an execution from storage
   * @return
   */
  def retrieveExecution(request: RegisterEIdRequest): WorkflowService = {
    retrieveExecution(request.eId, request.operators, request.links)
  }
  
  /**
   * Function retrieves executions from storage for comparison
   * @return
   */
  def retrieveExecutionsToCompare(request: CompareEIdExecutionRequest): (WorkflowService, WorkflowService) = {
    (retrieveExecution(request.eId1, request.operators1, request.links), retrieveExecution(request.eId2, request.operators2, request.links)) //WorkflowInfo doesn't do anything with the links
  }


  def retrieveExecution(
    eId: Int,
    operators: mutable.MutableList[OperatorDescriptor],
    links: mutable.MutableList[OperatorLink]
  ) : WorkflowService = {
    val execution: WorkflowExecutions = getExecutionById(UInteger.valueOf(eId))
    val retrievedWorkflowService = new WorkflowService(Some(execution.getUid), execution.getWid.intValue(), cleanUpDeadlineInSeconds)
    retrievedWorkflowService.status = maptoAggregatedState(execution.getStatus)
    retrievedWorkflowService.vId = execution.getVid.intValue()
    retrievedWorkflowService.executionID = eId
    val workflowInfo = WorkflowInfo(operators, links, mutable.MutableList())
    val job = new WorkflowJobService(
      new WorkflowContext(
        String.valueOf(WorkflowWebsocketResource.nextExecutionID.incrementAndGet),
        Some(execution.getUid),
        retrievedWorkflowService.vId,
        execution.getWid.intValue(),
        eId
      ),
      retrievedWorkflowService.wsInput,
      retrievedWorkflowService.operatorCache,
      retrievedWorkflowService.resultService,
      null,
      retrievedWorkflowService.errorHandler,
      workflowInfo
    )
    retrievedWorkflowService.jobService.onNext(job)
    val workflowDAG = workflowInfo.toDAG
    workflowDAG.getSinkOperators.foreach(sink => {
      retrievedWorkflowService.resultService.progressiveResults.put(
        sink,
        new ProgressiveResultService(
          workflowDAG.operators(sink).asInstanceOf[ProgressiveSinkOpDesc]
        )
      )
    })
    retrievedWorkflowService
  }

  /**
    * Function to create a new workflowService or retrieves an execution from storage
    * @return
    */
  def retrieveFromStorageOrCreateNew(
      wId: Int,
      uidOpt: Option[UInteger],
      cleanupTimeout: Int
  ): WorkflowService = {
    if (userSystemEnabled) {
      val latestExecution: Option[ExecutionContent] = getLatestExecution(UInteger.valueOf(wId))
      latestExecution match {
        case Some(latestExecution: ExecutionContent) =>
          if (
            isVersionInRangeUnimportant(
              latestExecution.vId,
              getLatestVersion(UInteger.valueOf(wId)),
              UInteger.valueOf(wId)
            )
          ) {
            val retrievedWorkflowService = new WorkflowService(uidOpt, wId, cleanupTimeout)
            retrievedWorkflowService.status = maptoAggregatedState(latestExecution.status)
            retrievedWorkflowService.vId = latestExecution.vId.intValue()
            retrievedWorkflowService.executionID = latestExecution.eId.longValue()
            val workflowInfo = retrievedWorkflowService.createWorkflowInfo(latestExecution.content)
            val job = new WorkflowJobService(
              new WorkflowContext(
                String.valueOf(WorkflowWebsocketResource.nextExecutionID.incrementAndGet),
                uidOpt,
                retrievedWorkflowService.vId,
                wId,
                latestExecution.eId.longValue()
              ),
              retrievedWorkflowService.wsInput,
              retrievedWorkflowService.operatorCache,
              retrievedWorkflowService.resultService,
              null,
              retrievedWorkflowService.errorHandler,
              workflowInfo
            )
            retrievedWorkflowService.jobService.onNext(job)
            val workflowDAG = workflowInfo.toDAG
            workflowDAG.getSinkOperators.foreach(sink => {
              retrievedWorkflowService.resultService.progressiveResults.put(
                sink,
                new ProgressiveResultService(
                  workflowDAG.operators.get(sink).get.asInstanceOf[ProgressiveSinkOpDesc]
                )
              )
            })
            return retrievedWorkflowService
          }
        case None =>
          return new WorkflowService(uidOpt, wId, cleanupTimeout)
      }
    }
    new WorkflowService(uidOpt, wId, cleanupTimeout)
  }

  /**
    * this function clears an execution from memory when the status is completed or aborted
    * @param wId
    */
  def removeWorkflowService(wId: String): Unit = {
    wIdToWorkflowState.remove(wId)
  }

  /**
    * This function validates if the version of the reattached workflow matches the one in memory
    * @param wId
    * @param service
    * @param uidOpt
    * @param cleanupTimeout
    * @return new or existing execution
    */
  def validateWorkflowVersion(
      wId: Int,
      service: WorkflowService,
      uidOpt: Option[UInteger],
      cleanupTimeout: Int = cleanUpDeadlineInSeconds
  ): WorkflowService = {
    if (userSystemEnabled) {
      // retrieve the version stored in memory as lowerBound and the latest one stored in mysql as upperBound
      if (
        !isVersionInRangeUnimportant(
          UInteger.valueOf(service.vId),
          getLatestVersion(UInteger.valueOf(wId)),
          UInteger.valueOf(wId)
        )
      ) {
        return new WorkflowService(uidOpt, wId, cleanupTimeout)
      }
    }
    service
  }
}

class WorkflowService(
    uidOpt: Option[UInteger],
    wId: Int,
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
        WorkflowErrorEvent(generalErrors =
          Map("error" -> (t.getMessage + "\n" + t.getStackTrace.mkString("\n")))
        )
      )
    }
  }
  val wsInput = new WebsocketInput(errorHandler)
  var status: WorkflowAggregatedState = WorkflowAggregatedState.UNINITIALIZED
  val stateStore = new WorkflowStateStore()
  val resultService: JobResultService =
    new JobResultService(opResultStorage, stateStore)
  val exportService: ResultExportService =
    new ResultExportService(opResultStorage, UInteger.valueOf(wId))
  val operatorCache: WorkflowCacheService =
    new WorkflowCacheService(opResultStorage, stateStore, wsInput)
  var jobService: BehaviorSubject[WorkflowJobService] = BehaviorSubject.create()
  var vId: Int = -1
  var executionID: Long = -1 // for every new execution,
  // reset it so that the value doesn't carry over across executions
  val lifeCycleManager: WorkflowLifecycleManager = new WorkflowLifecycleManager(
    s"uid=$uidOpt wid=$wId",
    cleanUpTimeout,
    () => {
      opResultStorage.close()
      WorkflowService.wIdToWorkflowState.remove(mkWorkflowStateId(wId, uidOpt))
      wsInput.onNext(WorkflowKillRequest(), None)
      unsubscribeAll()
    }
  )

  addSubscription(
    wsInput.subscribe((evt: WorkflowExecuteRequest, uidOpt) => initJobService(evt, uidOpt))
  )

  def getResultUpdateMessage(): Map[String, WebResultUpdate] = {
    resultService.progressiveResults.map {
      case (id, service) =>
        (
          id,
          service.convertWebResultUpdate(
            service.sink.getStorage.getCount.toInt,
            service.sink.getStorage.getCount.toInt
          )
        )
    }.toMap
  }

  def getComparisonResultUpdateMessage(): Map[String, WebResultUpdate] = {
    resultService.progressiveResults.map {
      case (id, service) =>
        (
          executionID.toString+"_"+id,
          service.convertWebResultUpdate(
            service.sink.getStorage.getCount.toInt,
            service.sink.getStorage.getCount.toInt
          )
        )
    }.toMap
  }

  def getWorkflowStatsMessage(eId: Int): Map[String, OperatorStatistics]= {
    StatStorage.getWorkflowOpsStats(eId)
  }

  def getComparisonWorkflowStatsMessage(eId: Int): Map[String, OperatorStatistics]= {
    val eIdToStat:Map[String, OperatorStatistics] = StatStorage.getWorkflowOpsStats(eId)
    eIdToStat.map(x => executionID.toString+"_"+x._1 -> x._2)
  }

  def createWorkflowInfo(workflowContent: String): WorkflowInfo = {
    val logicalPlanMapper: DBWorkflowToLogicalPlan = DBWorkflowToLogicalPlan(workflowContent)
    logicalPlanMapper.createLogicalPlan()
    logicalPlanMapper.getWorkflowLogicalPlan()
  }

  def connect(onNext: TexeraWebSocketEvent => Unit): Disposable = {
    lifeCycleManager.increaseUserCount()
    val subscriptions = stateStore.getAllStores
      .map(_.getWebsocketEventObservable)
      .map(evtPub =>
        evtPub.subscribe { evts: Iterable[TexeraWebSocketEvent] => evts.foreach(onNext) }
      )
      .toSeq
    val errorSubscription = errorSubject.subscribe { evt: TexeraWebSocketEvent => onNext(evt) }
    new CompositeDisposable(subscriptions :+ errorSubscription: _*)
  }

  def connectToJob(onNext: TexeraWebSocketEvent => Unit): Disposable = {
    var localDisposable = Disposable.empty()
    jobService.subscribe { job: WorkflowJobService =>
      localDisposable.dispose()
      val subscriptions = job.stateStore.getAllStores
        .map(_.getWebsocketEventObservable)
        .map(evtPub =>
          evtPub.subscribe { evts: Iterable[TexeraWebSocketEvent] => evts.foreach(onNext) }
        )
        .toSeq
      localDisposable = new CompositeDisposable(subscriptions: _*)
    }
  }

  def disconnect(): Unit = {
    lifeCycleManager.decreaseUserCount(
      Option(jobService.getValue).map(_.stateStore.jobMetadataStore.getState.state)
    )
  }

  private[this] def createWorkflowContext(request: WorkflowExecuteRequest): WorkflowContext = {
    val jobID: String = String.valueOf(WorkflowWebsocketResource.nextExecutionID.incrementAndGet)
    if (WorkflowCacheService.isAvailable) {
      operatorCache.updateCacheStatus(
        CacheStatusUpdateRequest(
          request.operators,
          request.links,
          request.breakpoints,
          request.cachedOperatorIds
        )
      )
    }

    if (WorkflowService.userSystemEnabled) {
      vId = getLatestVersion(UInteger.valueOf(wId)).intValue()
      executionID = ExecutionsMetadataPersistService.insertNewExecution(wId, vId, uidOpt)
    }
    new WorkflowContext(
      jobID,
      uidOpt,
      vId,
      wId,
      executionID
    )

  }

  def initJobService(req: WorkflowExecuteRequest, uidOpt: Option[UInteger]): Unit = {
    if (jobService.getValue != null) {
      //unsubscribe all
      jobService.getValue.unsubscribeAll()
    }
    val job = new WorkflowJobService(
      createWorkflowContext(req),
      wsInput,
      operatorCache,
      resultService,
      req,
      errorHandler,
      WorkflowInfo(req.operators, req.links, req.breakpoints)
    )
    lifeCycleManager.registerCleanUpOnStateChange(job.stateStore)
    jobService.onNext(job)
    job.startWorkflow()
  }

  override def unsubscribeAll(): Unit = {
    super.unsubscribeAll()
    Option(jobService.getValue).foreach(_.unsubscribeAll())
    operatorCache.unsubscribeAll()
    resultService.unsubscribeAll()
  }

}
