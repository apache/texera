package edu.uci.ics.texera.web.service

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
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{
  COMPLETED,
  FAILED,
  KILLED,
  PAUSED,
  READY,
  RUNNING
}
import edu.uci.ics.texera.web.{SubscriptionManager, TexeraWebApplication, WebsocketInput}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, WorkflowCompiler}
import edu.uci.ics.texera.web.resource.{EmailMessage, GmailResource}
import org.hibernate.validator.internal.constraintvalidators.hv.EmailValidator

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.collection.mutable

class WorkflowExecutionService(
    controllerConfig: ControllerConfig,
    val workflowContext: WorkflowContext,
    resultService: ExecutionResultService,
    request: WorkflowExecuteRequest,
    val executionStateStore: ExecutionStateStore,
    errorHandler: Throwable => Unit,
    lastCompletedLogicalPlan: Option[LogicalPlan]
) extends SubscriptionManager
    with LazyLogging {

  logger.info("Creating a new execution.")
  workflowContext.workflowSettings = request.workflowSettings

  val wsInput = new WebsocketInput(errorHandler)

  private val COMPLETED_PAUSED_OR_TERMINATED_STATES: Set[WorkflowAggregatedState] = Set(
    COMPLETED,
    PAUSED,
    FAILED,
    KILLED
  )

  private val emailValidator = new EmailValidator()

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

        // Send email notification if enabled and state transition is from RUNNING to COMPLETED, PAUSED, or TERMINATED
        if (
          request.emailNotificationEnabled &&
          emailValidator.isValid(request.userEmail, null) &&
          oldState.state == RUNNING &&
          COMPLETED_PAUSED_OR_TERMINATED_STATES.contains(newState.state)
        ) {
          sendWorkflowStatusEmail(newState.state)
        }
      }
      // Check if new error occurred
      if (newState.fatalErrors != oldState.fatalErrors) {
        outputEvents.append(WorkflowErrorEvent(newState.fatalErrors))
      }
      outputEvents
    })
  )

  var workflow: Workflow = _

  // Runtime starts from here:
  logger.info("Initialing an AmberClient, runtime starting...")
  var client: AmberClient = _
  var executionReconfigurationService: ExecutionReconfigurationService = _
  var executionStatsService: ExecutionStatsService = _
  var executionRuntimeService: ExecutionRuntimeService = _
  var executionConsoleService: ExecutionConsoleService = _

  def executeWorkflow(): Unit = {
    workflow = new WorkflowCompiler(workflowContext).compile(
      request.logicalPlan,
      resultService.opResultStorage,
      executionStateStore
    )

    client = TexeraWebApplication.createAmberRuntime(
      workflowContext,
      workflow.physicalPlan,
      resultService.opResultStorage,
      controllerConfig,
      errorHandler
    )
    executionReconfigurationService =
      new ExecutionReconfigurationService(client, executionStateStore, workflow)
    executionStatsService = new ExecutionStatsService(client, executionStateStore, workflowContext)
    executionRuntimeService = new ExecutionRuntimeService(
      client,
      executionStateStore,
      wsInput,
      executionReconfigurationService,
      controllerConfig.faultToleranceConfOpt
    )
    executionConsoleService = new ExecutionConsoleService(client, executionStateStore, wsInput)

    logger.info("Starting the workflow execution.")
    resultService.attachToExecution(executionStateStore, workflow.logicalPlan, client)
    executionStateStore.metadataStore.updateState(metadataStore =>
      updateWorkflowState(READY, metadataStore)
        .withFatalErrors(Seq.empty)
    )
    executionStateStore.statsStore.updateState(stats =>
      stats.withStartTimeStamp(System.currentTimeMillis())
    )
    client.sendAsyncWithCallback[WorkflowAggregatedState](
      StartWorkflow(),
      state =>
        executionStateStore.metadataStore.updateState(metadataStore =>
          if (metadataStore.state != FAILED) {
            updateWorkflowState(state, metadataStore)
          } else {
            metadataStore
          }
        )
    )
  }

  private def sendWorkflowStatusEmail(state: WorkflowAggregatedState): Unit = {
    val timestamp = DateTimeFormatter
      .ofPattern("MMMM d, yyyy, h:mm:ss a '(UTC)'")
      .withZone(ZoneOffset.UTC)
      .format(Instant.now())

    val dashboardUrl =
      s"${request.baseUrl}/dashboard/user/workspace/${workflowContext.workflowId.id}"

    val subject =
      s"[Texera] Workflow ${request.workflowName} (${workflowContext.workflowId.id}) Status: $state"
    val content = s"""
      |Hello,
      |
      |The workflow with the following details has changed its state:
      |
      |- Workflow ID: ${workflowContext.workflowId.id}
      |- Workflow Name: ${request.workflowName}
      |- State: $state
      |- Timestamp: $timestamp
      |
      |You can view more details by visiting: $dashboardUrl
      |
      |Regards,
      |Texera Team
    """.stripMargin

    val emailMessage = EmailMessage(
      receiver = request.userEmail,
      subject = subject,
      content = content
    )

    GmailResource.sendEmail(emailMessage, request.userEmail)
  }

  override def unsubscribeAll(): Unit = {
    super.unsubscribeAll()
    if (client != null) {
      // runtime created
      executionRuntimeService.unsubscribeAll()
      executionConsoleService.unsubscribeAll()
      executionStatsService.unsubscribeAll()
      executionReconfigurationService.unsubscribeAll()
    }
  }

}
