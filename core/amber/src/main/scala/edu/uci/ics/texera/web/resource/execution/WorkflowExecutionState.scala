package edu.uci.ics.texera.web.resource.execution

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.AmberClient
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.TexeraWebApplication
import edu.uci.ics.texera.web.model.event.{ResultExportResponse, TexeraWebSocketEvent}
import edu.uci.ics.texera.web.model.request.{
  CacheStatusUpdateRequest,
  ExecuteWorkflowRequest,
  ModifyLogicRequest,
  ResultExportRequest
}
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource
import edu.uci.ics.texera.web.resource.auth.UserResource
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler.ConstraintViolationException
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo.toJgraphtDAG
import edu.uci.ics.texera.workflow.common.workflow.{
  WorkflowCompiler,
  WorkflowInfo,
  WorkflowRewriter
}
import javax.servlet.http.HttpSession
import org.jooq.types.UInteger
import rx.lang.scala.subscriptions.CompositeSubscription
import rx.lang.scala.{Observer, Subscription}

import scala.collection.mutable

class WorkflowExecutionState(
    operatorCache: OperatorCache,
    uidOpt: Option[UInteger],
    request: ExecuteWorkflowRequest,
    prevResults: mutable.HashMap[String, OperatorResultService]
) extends LazyLogging {

  val workflowContext: WorkflowContext = createWorkflowContext()
  val workflowInfo: WorkflowInfo = createWorkflowInfo(workflowContext)
  val workflowCompiler: WorkflowCompiler = createWorkflowCompiler(workflowInfo, workflowContext)
  val workflow: Workflow = workflowCompiler.amberWorkflow(WorkflowIdentity(workflowContext.jobId))
  val client: AmberClient =
    TexeraWebApplication.createAmberRuntime(workflow, ControllerConfig.default)
  val workflowRuntimeService: WorkflowRuntimeService = new WorkflowRuntimeService(workflow, client)
  val workflowResultService: WorkflowResultService =
    new WorkflowResultService(workflowInfo, OperatorCache.opResultStorage, client)
  val resultExportService: ResultExportService = new ResultExportService()

  if (OperatorCache.isAvailable) {
    workflowResultService.updateResultFromPreviousRun(prevResults, operatorCache.cachedOperators)
  }
  workflowResultService.updateAvailableResult(request.operators)
  for (pair <- workflowInfo.breakpoints) {
    workflowRuntimeService.addBreakpoint(pair.operatorID, pair.breakpoint)
  }
  workflowRuntimeService.startWorkflow()

  private[this] def createWorkflowContext(): WorkflowContext = {
    val jobID: String = Integer.toString(WorkflowWebsocketResource.nextExecutionID.incrementAndGet)
    if (OperatorCache.isAvailable) {
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
    if (OperatorCache.isAvailable) {
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
        OperatorCache.opResultStorage
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

  def subscribeAll(observer: Observer[TexeraWebSocketEvent]): Subscription = {
    CompositeSubscription(
      SnapshotMulticast.syncState(workflowRuntimeService, observer, client),
      SnapshotMulticast.syncState(workflowResultService, observer, client)
    )
  }

  def modifyLogic(request: ModifyLogicRequest): Unit = {
    workflowCompiler.initOperator(request.operator)
    workflowRuntimeService.modifyLogic(request.operator)
  }

  def exportResult(uid: UInteger, request: ResultExportRequest): ResultExportResponse = {
    resultExportService.exportResult(uid, workflowResultService, request)
  }

}
