package edu.uci.ics.texera.web.service

import com.google.protobuf.timestamp.Timestamp
import com.twitter.util.{Await, Duration}
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.model.websocket.event.{
  TexeraWebSocketEvent,
  WorkflowErrorEvent,
  WorkflowStateEvent
}
import edu.uci.ics.texera.web.model.websocket.request.WorkflowExecuteRequest
import edu.uci.ics.texera.web.storage.JobStateStore
import edu.uci.ics.texera.web.storage.JobStateStore.updateWorkflowState
import edu.uci.ics.texera.web.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowFatalError
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{
  COMPLETED,
  FAILED,
  READY,
  RUNNING
}
import edu.uci.ics.texera.web.{SubscriptionManager, TexeraWebApplication, WebsocketInput}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, WorkflowCompiler}
import edu.uci.ics.texera.workflow.operators.udf.python.source.PythonUDFSourceOpDescV2
import edu.uci.ics.texera.workflow.operators.udf.python.{
  DualInputPortsPythonUDFOpDescV2,
  PythonUDFOpDescV2
}

import java.time.Instant
import scala.collection.mutable

class WorkflowJobService(
    workflowContext: WorkflowContext,
    resultService: JobResultService,
    request: WorkflowExecuteRequest,
    lastCompletedLogicalPlan: Option[LogicalPlan]
) extends SubscriptionManager
    with LazyLogging {

  val errorHandler: Throwable => Unit = { t =>
    {
      logger.error("error during execution", t)
      stateStore.statsStore.updateState(stats => stats.withEndTimeStamp(System.currentTimeMillis()))
      stateStore.jobMetadataStore.updateState { jobInfo =>
        updateWorkflowState(FAILED, jobInfo).addFatalErrors(
          WorkflowFatalError(
            EXECUTION_FAILURE,
            Timestamp(Instant.now),
            t.toString,
            t.getStackTrace.mkString("\n"),
            "unknown operator"
          )
        )
      }
    }
  }
  val wsInput = new WebsocketInput(errorHandler)
  val stateStore = new JobStateStore()

  addSubscription(
    stateStore.jobMetadataStore.registerDiffHandler((oldState, newState) => {
      val outputEvts = new mutable.ArrayBuffer[TexeraWebSocketEvent]()
      // Update workflow state
      if (newState.state != oldState.state || newState.isRecovering != oldState.isRecovering) {
        // Check if is recovering
        if (newState.isRecovering && newState.state != COMPLETED) {
          outputEvts.append(WorkflowStateEvent("Recovering"))
        } else {
          outputEvts.append(WorkflowStateEvent(Utils.aggregatedStateToString(newState.state)))
        }
      }
      // Check if new error occurred
      if (newState.fatalErrors != oldState.fatalErrors) {
        outputEvts.append(WorkflowErrorEvent(newState.fatalErrors))
      }
      outputEvts
    })
  )

  var logicalPlan: LogicalPlan = _
  var workflowCompiler: WorkflowCompiler = _
  var workflow: Workflow = _

  workflowCompilation()

  def workflowCompilation(): Unit = {
    logicalPlan = LogicalPlan(request.logicalPlan, workflowContext)
    logicalPlan.initializeLogicalPlan(stateStore)
    try {
      workflowCompiler = new WorkflowCompiler(logicalPlan)
      workflow = workflowCompiler.amberWorkflow(
        WorkflowIdentity(workflowContext.jobId),
        resultService.opResultStorage,
        lastCompletedLogicalPlan
      )
    } catch {
      case e: Throwable =>
        stateStore.jobMetadataStore.updateState { metadataStore =>
          updateWorkflowState(FAILED, metadataStore)
            .addFatalErrors(
              WorkflowFatalError(
                EXECUTION_FAILURE,
                Timestamp(Instant.now),
                e.toString,
                e.getStackTrace.mkString("\n"),
                "unknown operator"
              )
            )
        }
    }
  }

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
  var client: AmberClient = _
  var jobBreakpointService: JobBreakpointService = _
  var jobReconfigurationService: JobReconfigurationService = _
  var jobStatsService: JobStatsService = _
  var jobRuntimeService: JobRuntimeService = _
  var jobConsoleService: JobConsoleService = _

  def startWorkflow(): Unit = {
    client = TexeraWebApplication.createAmberRuntime(
      workflow,
      controllerConfig,
      errorHandler
    )
    jobBreakpointService = new JobBreakpointService(client, stateStore)
    jobReconfigurationService =
      new JobReconfigurationService(client, stateStore, workflowCompiler, workflow)
    jobStatsService = new JobStatsService(client, stateStore)
    jobRuntimeService = new JobRuntimeService(
      client,
      stateStore,
      wsInput,
      jobBreakpointService,
      jobReconfigurationService
    )
    jobConsoleService = new JobConsoleService(client, stateStore, wsInput, jobBreakpointService)

    for (pair <- workflowCompiler.logicalPlan.breakpoints) {
      Await.result(
        jobBreakpointService.addBreakpoint(pair.operatorID, pair.breakpoint),
        Duration.fromSeconds(10)
      )
    }
    resultService.attachToJob(stateStore, workflowCompiler.logicalPlan, client)
    stateStore.jobMetadataStore.updateState(jobInfo =>
      updateWorkflowState(READY, jobInfo.withEid(workflowContext.executionID))
        .withFatalErrors(Seq.empty)
    )
    stateStore.statsStore.updateState(stats => stats.withStartTimeStamp(System.currentTimeMillis()))
    client.sendAsyncWithCallback[Unit](
      StartWorkflow(),
      _ =>
        stateStore.jobMetadataStore.updateState(jobInfo =>
          if (jobInfo.state != FAILED) {
            updateWorkflowState(RUNNING, jobInfo)
          } else {
            jobInfo
          }
        )
    )
  }

  override def unsubscribeAll(): Unit = {
    super.unsubscribeAll()
    if (client != null) {
      // runtime created
      jobBreakpointService.unsubscribeAll()
      jobRuntimeService.unsubscribeAll()
      jobConsoleService.unsubscribeAll()
      jobStatsService.unsubscribeAll()
      jobReconfigurationService.unsubscribeAll()
    }
  }

}
