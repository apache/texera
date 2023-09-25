package edu.uci.ics.texera.web.service

import com.twitter.util.{Await, Duration}
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.model.websocket.request.WorkflowExecuteRequest
import edu.uci.ics.texera.web.storage.JobStateStore
import edu.uci.ics.texera.web.storage.JobStateStore.updateWorkflowState
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{READY, RUNNING}
import edu.uci.ics.texera.web.{SubscriptionManager, TexeraWebApplication, WebsocketInput}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler.ConstraintViolationException
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, WorkflowCompiler}
import edu.uci.ics.texera.workflow.operators.udf.python.source.PythonUDFSourceOpDescV2
import edu.uci.ics.texera.workflow.operators.udf.python.{
  DualInputPortsPythonUDFOpDescV2,
  PythonUDFOpDescV2
}

class WorkflowJobService(
    workflowContext: WorkflowContext,
    wsInput: WebsocketInput,
    resultService: JobResultService,
    request: WorkflowExecuteRequest,
    errorHandler: Throwable => Unit,
    lastCompletedLogicalPlan: Option[LogicalPlan]
) extends SubscriptionManager
    with LazyLogging {

  val stateStore = new JobStateStore()
  val workflowCompiler: WorkflowCompiler = createWorkflowCompiler(LogicalPlan(request.logicalPlan))
  val workflow: Workflow = workflowCompiler.amberWorkflow(
    WorkflowIdentity(workflowContext.jobId),
    resultService.opResultStorage,
    lastCompletedLogicalPlan
  )
  private val controllerConfig = {
    val conf = ControllerConfig.default
    if (
      workflowCompiler.logicalPlan.operators.exists {
        case _: DualInputPortsPythonUDFOpDescV2 => true
        case _: PythonUDFOpDescV2               => true
        case _: PythonUDFSourceOpDescV2         => true
        case _                                  => false
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

  def startWorkflow(): Unit = {
    for (pair <- workflowCompiler.logicalPlan.breakpoints) {
      Await.result(
        jobBreakpointService.addBreakpoint(pair.operatorID, pair.breakpoint),
        Duration.fromSeconds(10)
      )
    }
    resultService.attachToJob(stateStore, workflowCompiler.logicalPlan, client)
    stateStore.jobMetadataStore.updateState(jobInfo =>
      updateWorkflowState(READY, jobInfo.withEid(workflowContext.executionID)).withError(null)
    )
    stateStore.statsStore.updateState(stats => stats.withStartTimeStamp(System.currentTimeMillis()))
    client.sendAsyncWithCallback[Unit](
      StartWorkflow(),
      _ => stateStore.jobMetadataStore.updateState(jobInfo => updateWorkflowState(RUNNING, jobInfo))
    )
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
