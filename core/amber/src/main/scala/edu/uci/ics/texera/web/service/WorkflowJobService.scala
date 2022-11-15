package edu.uci.ics.texera.web.service

import com.twitter.util.{Await, Duration}
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ModifyLogicHandler.ModifyLogic
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.model.websocket.request.{ModifyLogicRequest, WorkflowExecuteRequest}
import edu.uci.ics.texera.web.storage.{JobStateStore, WorkflowStateStore}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{READY, RUNNING}
import edu.uci.ics.texera.web.{SubscriptionManager, TexeraWebApplication, WebsocketInput}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler.ConstraintViolationException
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo.toJgraphtDAG
import edu.uci.ics.texera.workflow.common.workflow.{
  WorkflowCompiler,
  WorkflowInfo,
  WorkflowRewriter
}
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.source.PythonUDFSourceOpDescV2
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.{
  DualInputPortsPythonUDFOpDescV2,
  PythonUDFOpDescV2,
  PythonUDFOpExecV2
}

class WorkflowJobService(
    workflowContext: WorkflowContext,
    wsInput: WebsocketInput,
    operatorCache: WorkflowCacheService,
    resultService: JobResultService,
    request: WorkflowExecuteRequest,
    errorHandler: Throwable => Unit,
    engineVersion: String
) extends SubscriptionManager
    with LazyLogging {

  val stateStore = new JobStateStore()
  val workflowInfo: WorkflowInfo = createWorkflowInfo()
  val workflowCompiler: WorkflowCompiler = createWorkflowCompiler(workflowInfo)
  val workflow: Workflow = workflowCompiler.amberWorkflow(
    WorkflowIdentity(workflowContext.jobId),
    resultService.opResultStorage
  )
  private val controllerConfig = {
    val conf = ControllerConfig.default
    if (
      workflowInfo.operators.exists {
        case x: DualInputPortsPythonUDFOpDescV2 => true
        case x: PythonUDFOpDescV2               => true
        case x: PythonUDFSourceOpDescV2         => true
        case other                              => false
      }
    ) {
      conf.supportFaultTolerance = false
    }
    conf
  }

  // Runtime starts from here:
  var client: AmberClient =
    TexeraWebApplication.createAmberRuntime(
      workflow,
      controllerConfig,
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

  workflowContext.executionID = -1 // for every new execution,
  // reset it so that the value doesn't carry over across executions
  def startWorkflow(): Unit = {
    for (pair <- workflowInfo.breakpoints) {
      Await.result(
        jobBreakpointService.addBreakpoint(pair.operatorID, pair.breakpoint),
        Duration.fromSeconds(10)
      )
    }
    resultService.attachToJob(stateStore, workflowInfo, client)
    if (WorkflowService.userSystemEnabled) {
      workflowContext.executionID = ExecutionsMetadataPersistService.insertNewExecution(
        workflowContext.wId,
        workflowContext.userId,
        request.executionName,
        engineVersion
      )
    }
    stateStore.jobMetadataStore.updateState(jobInfo =>
      jobInfo.withState(READY).withEid(workflowContext.executionID).withError(null)
    )
    client.sendAsyncWithCallback[Unit](
      StartWorkflow(),
      _ => stateStore.jobMetadataStore.updateState(jobInfo => jobInfo.withState(RUNNING))
    )
  }

  private[this] def createWorkflowInfo(): WorkflowInfo = {
    var workflowInfo = WorkflowInfo(
      request.logicalPlan.operators,
      request.logicalPlan.links,
      request.logicalPlan.breakpoints
    )
    if (WorkflowCacheService.isAvailable) {
      workflowInfo.cachedOperatorIds = request.logicalPlan.cachedOperatorIds
      logger.debug(
        s"Cached operators: ${operatorCache.cachedOperators} with ${request.logicalPlan.cachedOperatorIds}"
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
