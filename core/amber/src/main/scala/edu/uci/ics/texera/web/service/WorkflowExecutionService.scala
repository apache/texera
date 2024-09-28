package edu.uci.ics.texera.web.service

import akka.actor.Cancellable
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.ExecutionStateUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.TakeGlobalCheckpointHandler.TakeGlobalCheckpoint
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.AmberRuntime
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelMarkerIdentity
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.model.websocket.event.{TexeraWebSocketEvent, WorkflowErrorEvent, WorkflowStateEvent}
import edu.uci.ics.texera.web.model.websocket.request.WorkflowExecuteRequest
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{COMPLETED, FAILED, KILLED, READY, RUNNING}
import edu.uci.ics.texera.web.{SubscriptionManager, TexeraWebApplication, WebsocketInput}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, WorkflowCompiler}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.concurrent.duration.DurationInt

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
  private var interactionHandle: Cancellable = _
  val wsInput = new WebsocketInput(errorHandler)

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

  var workflow: Workflow = _

  // Runtime starts from here:
  logger.info("Initialing an AmberClient, runtime starting...")
  var client: AmberClient = _
  var executionReconfigurationService: ExecutionReconfigurationService = _
  var executionStatsService: ExecutionStatsService = _
  var executionRuntimeService: ExecutionRuntimeService = _
  var executionConsoleService: ExecutionConsoleService = _

  def executeWorkflow(): Unit = {
    if (interactionHandle != null && !interactionHandle.isCancelled) {
      interactionHandle.cancel()
    }

    workflow = new WorkflowCompiler(workflowContext).compile(
      request.logicalPlan,
      resultService.opResultStorage,
      lastCompletedLogicalPlan,
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
      controllerConfig.faultToleranceConfOpt,
      workflow.physicalPlan
    )
    executionConsoleService = new ExecutionConsoleService(client, executionStateStore, wsInput)

    addSubscription(
      client
        .registerCallback[ExecutionStateUpdate](evt => {
          if (evt.state == COMPLETED || evt.state == FAILED || evt.state == KILLED) {
            if (interactionHandle != null && !interactionHandle.isCancelled) {
              interactionHandle.cancel()
            }
          }
        })
    )

    logger.info("Starting the workflow execution.")
    resultService.attachToExecution(executionStateStore, workflow.originalLogicalPlan, client)
    executionStateStore.metadataStore.updateState(metadataStore =>
      updateWorkflowState(READY, metadataStore)
        .withFatalErrors(Seq.empty)
    )
    executionStateStore.statsStore.updateState(stats =>
      stats.withStartTimeStamp(System.currentTimeMillis())
    )
    client.sendAsyncWithCallback[WorkflowAggregatedState](
      StartWorkflow(),
      state => {
        if (this.request.periodicalInteraction > 0) {
          interactionHandle = AmberRuntime
            .scheduleRecurringCallThroughActorSystem(
              this.request.periodicalInteraction.seconds,
              this.request.periodicalInteraction.seconds
            ) {
              client.sendAsync(PauseWorkflow()).map{
                ret => {
                  val now = LocalDateTime.now()
                  val formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
                  val timestamp = now.format(formatter)
                  val checkpointId = ChannelMarkerIdentity(s"Interaction_${timestamp}")
                  val uri = controllerConfig.faultToleranceConfOpt.get.writeTo.resolve(checkpointId.toString)
                  client.sendAsync(TakeGlobalCheckpoint(interactionOnly = false, checkpointId, uri)).map{
                    ret =>
                      client.sendAsync(ResumeWorkflow()).map(ret =>
                        executionStateStore.metadataStore.updateState(metadataStore =>
                        updateWorkflowState(RUNNING, metadataStore)
                      ))
                  }
                }
              }
            }
        }
        executionStateStore.metadataStore.updateState(metadataStore =>
          if (metadataStore.state != FAILED) {
            updateWorkflowState(state, metadataStore)
          } else {
            metadataStore
          }
        )
      }
    )
  }


  override def unsubscribeAll(): Unit = {
    super.unsubscribeAll()
    if (interactionHandle != null && !interactionHandle.isCancelled) {
      interactionHandle.cancel()
    }
    if (client != null) {
      // runtime created
      executionRuntimeService.unsubscribeAll()
      executionConsoleService.unsubscribeAll()
      executionStatsService.unsubscribeAll()
      executionReconfigurationService.unsubscribeAll()
    }
  }

}
