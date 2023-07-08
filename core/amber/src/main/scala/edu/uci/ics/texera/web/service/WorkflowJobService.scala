package edu.uci.ics.texera.web.service

import com.twitter.util.{Await, Duration}
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.model.websocket.request.WorkflowExecuteRequest
import edu.uci.ics.texera.web.storage.JobStateStore
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{READY, RUNNING}
import edu.uci.ics.texera.web.{SubscriptionManager, TexeraWebApplication, WebsocketInput}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler.ConstraintViolationException
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, WorkflowCompiler, WorkflowRewriter}
import edu.uci.ics.texera.workflow.operators.udf.python.source.PythonUDFSourceOpDescV2
import edu.uci.ics.texera.workflow.operators.udf.python.{
  DualInputPortsPythonUDFOpDescV2,
  PythonUDFOpDescV2
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
  val logicalPlan: LogicalPlan = createLogicalPlan()
  val workflowCompiler: WorkflowCompiler = createWorkflowCompiler(logicalPlan)
  val workflow: Workflow = workflowCompiler.amberWorkflow(
    WorkflowIdentity(workflowContext.jobId),
    resultService.opResultStorage
  )
  private val controllerConfig = {
    val conf = ControllerConfig.default
    if (
      logicalPlan.operators.exists {
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
  val jobReconfigurationService =
    new JobReconfigurationService(client, stateStore, workflowCompiler, workflow)
  val jobStatsService = new JobStatsService(client, stateStore)
  val jobRuntimeService =
    new JobRuntimeService(
      client,
      stateStore,
      wsInput,
      jobBreakpointService,
      jobReconfigurationService
    )
  val jobPythonService =
    new JobPythonService(client, stateStore, wsInput, jobBreakpointService)

  workflowContext.executionID = -1 // for every new execution,
  // reset it so that the value doesn't carry over across executions
  def startWorkflow(): Unit = {
    for (pair <- logicalPlan.breakpoints) {
      Await.result(
        jobBreakpointService.addBreakpoint(pair.operatorID, pair.breakpoint),
        Duration.fromSeconds(10)
      )
    }
    resultService.attachToJob(stateStore, logicalPlan, client)
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
    stateStore.statsStore.updateState(stats => stats.withStartTimeStamp(System.currentTimeMillis()))
    client.sendAsyncWithCallback[Unit](
      StartWorkflow(),
      _ => stateStore.jobMetadataStore.updateState(jobInfo => jobInfo.withState(RUNNING))
    )
  }

  private[this] def createLogicalPlan(): LogicalPlan = {
    var logicalPlan = LogicalPlan(request.logicalPlan)
    if (WorkflowCacheService.isAvailable) {
      logger.debug(
        s"Cached operators: ${operatorCache.cachedOperators} with ${logicalPlan.cachedOperatorIds}"
      )
      val workflowRewriter = new WorkflowRewriter(
        logicalPlan,
        operatorCache.cachedOperators,
        operatorCache.cacheSourceOperators,
        operatorCache.cacheSinkOperators,
        operatorCache.operatorRecord,
        resultService.opResultStorage
      )
      val newWorkflowInfo = workflowRewriter.rewrite
      val oldWorkflowInfo = logicalPlan
      logicalPlan = newWorkflowInfo
      logicalPlan.cachedOperatorIds = oldWorkflowInfo.cachedOperatorIds
      logger.info(
        s"Rewrite the original workflow: $oldWorkflowInfo to be: $logicalPlan"
      )
    }
    logicalPlan
  }

  private[this] def createWorkflowCompiler(
      logicalPlan: LogicalPlan
  ): WorkflowCompiler = {
    val compiler = new WorkflowCompiler(logicalPlan, workflowContext)
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
    jobReconfigurationService.unsubscribeAll()
  }

}
