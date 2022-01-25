package edu.uci.ics.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.{SnapshotMulticast, TexeraWebApplication}
import edu.uci.ics.texera.web.model.websocket.event.{
  ExecutionStatusEnum,
  TexeraWebSocketEvent,
  Uninitialized,
  WorkflowStateEvent
}
import edu.uci.ics.texera.web.model.websocket.request.{
  CacheStatusUpdateRequest,
  ModifyLogicRequest,
  ResultExportRequest,
  WorkflowExecuteRequest
}
import edu.uci.ics.texera.web.model.websocket.response.ResultExportResponse
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler.ConstraintViolationException
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo.toJgraphtDAG
import edu.uci.ics.texera.workflow.common.workflow.{
  WorkflowCompiler,
  WorkflowInfo,
  WorkflowRewriter
}
import org.jooq.types.UInteger
import rx.lang.scala.subjects.BehaviorSubject
import rx.lang.scala.subscriptions.CompositeSubscription
import rx.lang.scala.{Observer, Subscription}

import scala.collection.mutable

class WorkflowJobService(
    operatorCache: WorkflowCacheService,
    uidOpt: Option[UInteger],
    request: WorkflowExecuteRequest,
    opResultStorage: OpResultStorage
) extends SnapshotMulticast[TexeraWebSocketEvent]
    with LazyLogging {

  // workflow status
  private val workflowStatus: BehaviorSubject[ExecutionStatusEnum] = createWorkflowStatus()

  // Compilation starts from here:
  val workflowContext: WorkflowContext = createWorkflowContext()
  val workflowInfo: WorkflowInfo = createWorkflowInfo(workflowContext)
  val workflowCompiler: WorkflowCompiler = createWorkflowCompiler(workflowInfo, workflowContext)
  val workflow: Workflow = workflowCompiler.amberWorkflow(
    WorkflowIdentity(workflowContext.jobId),
    opResultStorage
  )

  // Runtime starts from here:
  val client: AmberClient =
    TexeraWebApplication.createAmberRuntime(workflow, ControllerConfig.default)
  val workflowRuntimeService: JobRuntimeService = new JobRuntimeService(workflowStatus, client)

  // Result-related services start from here:
  val workflowResultService: JobResultService =
    new JobResultService(workflowInfo, client, opResultStorage)
  val resultExportService: ResultExportService = new ResultExportService()

  def startWorkflow(): Unit = {
    workflowResultService.updateAvailableResult(request.operators)
    for (pair <- workflowInfo.breakpoints) {
      workflowRuntimeService.addBreakpoint(pair.operatorID, pair.breakpoint)
    }
    workflowRuntimeService.startWorkflow()
  }

  private[this] def createWorkflowStatus(): BehaviorSubject[ExecutionStatusEnum] = {
    val status = BehaviorSubject[ExecutionStatusEnum](Uninitialized)
    status.onTerminateDetach.subscribe(x => send(WorkflowStateEvent(x)))
    status
  }

  private[this] def createWorkflowContext(): WorkflowContext = {
    val jobID: String = Integer.toString(WorkflowWebsocketResource.nextExecutionID.incrementAndGet)
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
    val context = new WorkflowContext
    context.jobId = jobID
    context.userId = uidOpt
    context
  }

  private[this] def createWorkflowInfo(context: WorkflowContext): WorkflowInfo = {
    var workflowInfo = WorkflowInfo(request.operators, request.links, request.breakpoints)
    if (WorkflowCacheService.isAvailable) {
      workflowInfo.cachedOperatorIds = request.cachedOperatorIds
      logger.debug(
        s"Cached operators: ${operatorCache.cachedOperators} with ${request.cachedOperatorIds}"
      )
      val workflowRewriter = new WorkflowRewriter(
        workflowInfo,
        operatorCache.cachedOperators,
        operatorCache.cacheSourceOperators,
        operatorCache.cacheSinkOperators,
        operatorCache.operatorRecord,
        opResultStorage
      )
      val newWorkflowInfo = workflowRewriter.rewrite
      val oldWorkflowInfo = workflowInfo
      workflowInfo = newWorkflowInfo
      workflowInfo.cachedOperatorIds = oldWorkflowInfo.cachedOperatorIds
      logger.info(
        s"Rewrite the original workflow: ${toJgraphtDAG(oldWorkflowInfo)} to be: ${toJgraphtDAG(workflowInfo)}"
      )
    }
    workflowInfo
  }

  private[this] def createWorkflowCompiler(
      workflowInfo: WorkflowInfo,
      context: WorkflowContext
  ): WorkflowCompiler = {
    val compiler = new WorkflowCompiler(workflowInfo, context)
    val violations = compiler.validate
    if (violations.nonEmpty) {
      throw new ConstraintViolationException(violations)
    }
    compiler
  }

  def subscribeRuntimeComponents(observer: Observer[TexeraWebSocketEvent]): Subscription = {
    CompositeSubscription(
      workflowRuntimeService.subscribeWithAmberClient(observer, client),
      workflowResultService.subscribeWithAmberClient(observer, client)
    )
  }

  def modifyLogic(request: ModifyLogicRequest): Unit = {
    workflowCompiler.initOperator(request.operator)
    workflowRuntimeService.modifyLogic(request.operator)
  }

  def exportResult(uid: UInteger, request: ResultExportRequest): ResultExportResponse = {
    resultExportService.exportResult(uid, opResultStorage, request)
  }

  override def sendSnapshotTo(observer: Observer[TexeraWebSocketEvent]): Unit = {
    observer.onNext(WorkflowStateEvent(workflowStatus.asJavaSubject.getValue))
  }
}
