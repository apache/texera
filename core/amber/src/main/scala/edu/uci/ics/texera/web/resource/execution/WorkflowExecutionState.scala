package edu.uci.ics.texera.web.resource.execution

import akka.actor.PoisonPill
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.{Controller, ControllerConfig, ControllerSubjects, Workflow}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ModifyLogicHandler.ModifyLogic
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RetryWorkflowHandler.RetryWorkflow
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.model.event.{OperatorAvailableResult, PaginatedResultEvent, ResultExportResponse, TexeraWebSocketEvent, WorkflowAvailableResultEvent, WorkflowErrorEvent, WorkflowPausedEvent, WorkflowResumedEvent}
import edu.uci.ics.texera.web.model.request.{AddBreakpointRequest, CacheStatusUpdateRequest, ExecuteWorkflowRequest, ModifyLogicRequest, RemoveBreakpointRequest, ResultExportRequest, ResultPaginationRequest, SkipTupleRequest}
import edu.uci.ics.texera.web.resource.{Observable, Observer, Subject, Subscription, WorkflowWebsocketResource}
import edu.uci.ics.texera.web.resource.auth.UserResource
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler.ConstraintViolationException
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo.toJgraphtDAG
import edu.uci.ics.texera.workflow.common.workflow.{WorkflowCompiler, WorkflowInfo, WorkflowRewriter, WorkflowVertex}
import edu.uci.ics.texera.workflow.operators.sink.CacheSinkOpDesc
import javax.servlet.http.HttpSession

import scala.collection.{breakOut, mutable}

class WorkflowExecutionState(val operatorCache:OperatorCache, httpSession: HttpSession, request: ExecuteWorkflowRequest, prevResults:mutable.HashMap[String, OperatorResultService]) extends LazyLogging {

  val workflowContext:WorkflowContext = createWorkflowContext()
  val workflowInfo: WorkflowInfo = createWorkflowInfo(workflowContext)
  val workflowCompiler: WorkflowCompiler = createWorkflowCompiler(workflowInfo, workflowContext)
  val workflow: Workflow = workflowCompiler.amberWorkflow(WorkflowIdentity(workflowContext.jobID))
  val controllerObservables:ControllerSubjects = ControllerSubjects()
  val workflowRuntimeService:WorkflowRuntimeService = new WorkflowRuntimeService(workflow, controllerObservables)
  val workflowResultService:WorkflowResultService = new WorkflowResultService(workflowInfo, OperatorCache.opResultStorage, controllerObservables)
  val executionStatus:ExecutionStatus = new ExecutionStatus(controllerObservables)
  val resultExportService:ResultExportService = new ResultExportService()

  if (OperatorCache.isAvailable) {
    workflowResultService.updateResultFromPreviousRun(prevResults, operatorCache.cachedOperators)
  }
  workflowResultService.updateAvailableResult(request.operators)
  for (pair <- workflowInfo.breakpoints) {
    workflowRuntimeService.addBreakpoint(pair.operatorID, pair.breakpoint)
  }
  workflowRuntimeService.startWorkflow()
  executionStatus.workflowStarted()

  private[this] def createWorkflowContext():WorkflowContext = {
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
    context.jobID = jobID
    context.userID = UserResource
      .getUser(httpSession)
      .map(u => u.getUid)
    context
  }

  private[this] def createWorkflowInfo(context:WorkflowContext): WorkflowInfo ={
    var workflowInfo = WorkflowInfo(request.operators, request.links, request.breakpoints)
    if (OperatorCache.isAvailable) {
      workflowInfo.cachedOperatorIds = request.cachedOperatorIds
      logger.debug(s"Cached operators: ${operatorCache.cachedOperators} with ${request.cachedOperatorIds}")
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

  private[this] def createWorkflowCompiler(workflowInfo: WorkflowInfo, context:WorkflowContext): WorkflowCompiler ={
    val compiler = new WorkflowCompiler(workflowInfo, context)
    val violations = compiler.validate
    if (violations.nonEmpty) {
      throw new ConstraintViolationException(violations)
    }
    compiler
  }


  def subscribeAll(observer: Observer[TexeraWebSocketEvent]): Subscription = {
    val subscription = workflowRuntimeService.subscribe(observer)
    subscription.add(workflowResultService.subscribe(observer))
    subscription.add(executionStatus.subscribe(observer))
    subscription
  }

  def modifyLogic(request:ModifyLogicRequest): Unit ={
    workflowCompiler.initOperator(request.operator)
    workflowRuntimeService.modifyLogic(request.operator)
  }

  def exportResult(httpSession: HttpSession, request: ResultExportRequest): ResultExportResponse = {
    resultExportService.exportResult(httpSession, workflowResultService, request)
  }

}
