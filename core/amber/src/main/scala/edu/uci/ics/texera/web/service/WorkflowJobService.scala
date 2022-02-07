package edu.uci.ics.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ModifyLogicHandler.ModifyLogic
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.model.websocket.request.{
  CacheStatusUpdateRequest,
  ModifyLogicRequest,
  WorkflowExecuteRequest
}
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{READY, RUNNING}
import edu.uci.ics.texera.web.{
  SubscriptionManager,
  TexeraWebApplication,
  WebsocketInput,
  WorkflowStateStore
}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler.ConstraintViolationException
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo.toJgraphtDAG
import edu.uci.ics.texera.workflow.common.workflow.{
  WorkflowCompiler,
  WorkflowInfo,
  WorkflowRewriter
}
import org.jooq.types.UInteger

class WorkflowJobService(
    workflowContext: WorkflowContext,
    stateStore: WorkflowStateStore,
    wsInput: WebsocketInput,
    operatorCache: WorkflowCacheService,
    resultService: JobResultService,
    uidOpt: Option[UInteger],
    request: WorkflowExecuteRequest,
    errorHandler: Throwable => Unit
) extends SubscriptionManager
    with LazyLogging {

  // Compilation starts from here:
  workflowContext.jobId = createWorkflowContext()
  val workflowInfo: WorkflowInfo = createWorkflowInfo()
  val workflowCompiler: WorkflowCompiler = createWorkflowCompiler(workflowInfo)
  val workflow: Workflow = workflowCompiler.amberWorkflow(
    WorkflowIdentity(workflowContext.jobId),
    resultService.opResultStorage
  )

  // Runtime starts from here:
  var client: AmberClient =
    TexeraWebApplication.createAmberRuntime(
      workflow,
      ControllerConfig.default,
      errorHandler
    )
  val jobBreakpointService = new JobBreakpointService(client, stateStore)
  val jobStatsService = new JobStatsService(client, stateStore)
  val jobRuntimeService =
    new JobRuntimeService(client, stateStore, wsInput, jobBreakpointService)
  val jobPythonService =
    new JobPythonService(client, stateStore, wsInput, jobBreakpointService)

  addSubscription(wsInput.subscribe((req: ModifyLogicRequest, uidOpt) => {
    workflowCompiler.initOperator(req.operator)
    client.sendAsync(ModifyLogic(req.operator))
  }))

  def startWorkflow(): Unit = {
    for (pair <- workflowInfo.breakpoints) {
      jobBreakpointService.addBreakpoint(pair.operatorID, pair.breakpoint)
    }
    resultService.attachToJob(workflowInfo, client)
    val f = client.sendAsync(StartWorkflow())
    stateStore.jobStateStore.updateState(jobInfo => jobInfo.withState(READY))
    f.onSuccess { _ =>
      stateStore.jobStateStore.updateState(jobInfo => jobInfo.withState(RUNNING))
    }
  }

  private[this] def createWorkflowContext(): String = {
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
    jobID
  }

  private[this] def createWorkflowInfo(): WorkflowInfo = {
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
        resultService.opResultStorage
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
      workflowInfo: WorkflowInfo
  ): WorkflowCompiler = {
    val compiler = new WorkflowCompiler(workflowInfo, workflowContext)
    val violations = compiler.validate
    if (violations.nonEmpty) {
      throw new ConstraintViolationException(violations)
    }
    compiler
  }

  override def unsubscribeAll(): Unit = {
    super.unsubscribeAll()
    jobBreakpointService.unsubscribeAll()
    jobRuntimeService.unsubscribeAll()
    jobPythonService.unsubscribeAll()
    jobStatsService.unsubscribeAll()
  }

}
