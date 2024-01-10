package edu.uci.ics.texera.web.service

import com.google.protobuf.timestamp.Timestamp
import com.twitter.util.{Await, Duration}
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.model.websocket.event.{
  TexeraWebSocketEvent,
  WorkflowErrorEvent,
  WorkflowStateEvent
}
import edu.uci.ics.texera.web.model.websocket.request.WorkflowExecuteRequest
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState
import edu.uci.ics.texera.web.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{
  COMPLETED,
  FAILED,
  READY,
  RUNNING
}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowFatalError
import edu.uci.ics.texera.web.{SubscriptionManager, TexeraWebApplication, WebsocketInput}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, WorkflowCompiler}

import java.time.Instant
import scala.collection.mutable

class WorkflowExecutionService(
    controllerConfig: ControllerConfig,
    workflowContext: WorkflowContext,
    resultService: ExecutionResultService,
    request: WorkflowExecuteRequest,
    lastCompletedLogicalPlan: Option[LogicalPlan]
) extends SubscriptionManager
    with LazyLogging {
  logger.info("Creating a new execution.")

  val errorHandler: Throwable => Unit = { t =>
    {
      logger.error("error during execution", t)
      executionStateStore.statsStore.updateState(stats =>
        stats.withEndTimeStamp(System.currentTimeMillis())
      )
      executionStateStore.metadataStore.updateState { metadataStore =>
        updateWorkflowState(FAILED, metadataStore).addFatalErrors(
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
  val executionStateStore = new ExecutionStateStore()

  addSubscription(
    executionStateStore.metadataStore.registerDiffHandler((oldState, newState) => {
      val outputEvents = new mutable.ArrayBuffer[TexeraWebSocketEvent]()
      // Update workflow state
      if (newState.state != oldState.state || newState.isRecovering != oldState.isRecovering) {
        // Check if is recovering
        if (newState.isRecovering && newState.state != COMPLETED) {
          outputEvents.append(WorkflowStateEvent("Recovering"))
        } else {
          outputEvents.append(WorkflowStateEvent(Utils.aggregatedStateToString(newState.state)))
        }
      }
      // Check if new error occurred
      if (newState.fatalErrors != oldState.fatalErrors) {
        outputEvents.append(WorkflowErrorEvent(newState.fatalErrors))
      }
      outputEvents
    })
  )

  var workflowCompiler: WorkflowCompiler = _
  var workflow: Workflow = _

  workflowCompilation()

  private def workflowCompilation(): Unit = {
    logger.info("Compiling the logical plan into a physical plan.")

    try {
      workflowCompiler = new WorkflowCompiler(workflowContext)
      workflow = workflowCompiler.compile(
        request.logicalPlan,
        resultService.opResultStorage,
        lastCompletedLogicalPlan,
        executionStateStore,
        controllerConfig
      )
    } catch {
      case e: Throwable =>
        logger.error("error occurred during physical plan compilation", e)
        executionStateStore.metadataStore.updateState { metadataStore =>
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

  // Runtime starts from here:
  logger.info("Initialing an AmberClient, runtime starting...")
  var client: AmberClient = _
  var executionBreakpointService: ExecutionBreakpointService = _
  var executionReconfigurationService: ExecutionReconfigurationService = _
  var executionStatsService: ExecutionStatsService = _
  var executionRuntimeService: ExecutionRuntimeService = _
  var executionConsoleService: ExecutionConsoleService = _

  def startWorkflow(): Unit = {
    client = TexeraWebApplication.createAmberRuntime(
      workflow,
      controllerConfig,
      errorHandler
    )

    executionBreakpointService = new ExecutionBreakpointService(client, executionStateStore)
    executionReconfigurationService =
      new ExecutionReconfigurationService(client, executionStateStore, workflow)
    executionStatsService = new ExecutionStatsService(client, executionStateStore, workflowContext)
    executionRuntimeService = new ExecutionRuntimeService(
      client,
      executionStateStore,
      wsInput,
      executionBreakpointService,
      executionReconfigurationService
    )
    executionConsoleService =
      new ExecutionConsoleService(client, executionStateStore, wsInput, executionBreakpointService)

    logger.info("Starting the workflow execution.")
    for (pair <- request.logicalPlan.breakpoints) {
      Await.result(
        executionBreakpointService.addBreakpoint(pair.operatorID, pair.breakpoint),
        Duration.fromSeconds(10)
      )
    }
    resultService.attachToExecution(executionStateStore, workflow.logicalPlan, client)
    executionStateStore.metadataStore.updateState(metadataStore =>
      updateWorkflowState(READY, metadataStore.withExecutionId(workflowContext.executionId))
        .withFatalErrors(Seq.empty)
    )
    executionStateStore.statsStore.updateState(stats =>
      stats.withStartTimeStamp(System.currentTimeMillis())
    )
    client.sendAsyncWithCallback[Unit](
      StartWorkflow(),
      _ =>
        executionStateStore.metadataStore.updateState(metadataStore =>
          if (metadataStore.state != FAILED) {
            updateWorkflowState(RUNNING, metadataStore)
          } else {
            metadataStore
          }
        )
    )
  }

  override def unsubscribeAll(): Unit = {
    super.unsubscribeAll()
    if (client != null) {
      // runtime created
      executionBreakpointService.unsubscribeAll()
      executionRuntimeService.unsubscribeAll()
      executionConsoleService.unsubscribeAll()
      executionStatsService.unsubscribeAll()
      executionReconfigurationService.unsubscribeAll()
    }
  }

}
