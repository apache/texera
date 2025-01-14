package edu.uci.ics.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState._
import edu.uci.ics.amber.engine.common.{AmberConfig, Utils}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.executionruntimestate.ExecutionMetadataStore
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.OperatorExecutions
import edu.uci.ics.texera.web.model.websocket.event.{
  TexeraWebSocketEvent,
  WorkflowErrorEvent,
  WorkflowStateEvent
}
import edu.uci.ics.texera.web.model.websocket.request.WorkflowExecuteRequest
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState
import edu.uci.ics.texera.web.{ComputingUnitMaster, SubscriptionManager, WebsocketInput}
import edu.uci.ics.texera.workflow.{LogicalPlan, WorkflowCompiler}
import org.jooq.types.{UInteger, ULong}

import java.net.URI
import java.util
import scala.collection.mutable

class WorkflowExecutionService(
    controllerConfig: ControllerConfig,
    val workflowContext: WorkflowContext,
    resultService: ExecutionResultService,
    request: WorkflowExecuteRequest,
    val executionStateStore: ExecutionStateStore,
    errorHandler: Throwable => Unit,
    lastCompletedLogicalPlan: Option[LogicalPlan],
    userEmailOpt: Option[String],
    sessionUri: URI
) extends SubscriptionManager
    with LazyLogging {

  workflowContext.workflowSettings = request.workflowSettings
  val wsInput = new WebsocketInput(errorHandler)

  private val emailNotificationService = userEmailOpt.map(email =>
    new EmailNotificationService(
      new WorkflowEmailNotifier(
        workflowContext.workflowId.id,
        email,
        sessionUri
      )
    )
  )

  addSubscription(
    executionStateStore.metadataStore.registerDiffHandler((oldState, newState) => {
      val outputEvents = new mutable.ArrayBuffer[TexeraWebSocketEvent]()

      if (newState.state != oldState.state || newState.isRecovering != oldState.isRecovering) {
        outputEvents.append(createStateEvent(newState))

        if (request.emailNotificationEnabled && emailNotificationService.nonEmpty) {
          emailNotificationService.get.sendEmailNotification(oldState.state, newState.state)
        }
      }

      if (newState.fatalErrors != oldState.fatalErrors) {
        outputEvents.append(WorkflowErrorEvent(newState.fatalErrors))
      }

      outputEvents
    })
  )

  private def createStateEvent(state: ExecutionMetadataStore): WorkflowStateEvent = {
    if (state.isRecovering && state.state != COMPLETED) {
      WorkflowStateEvent("Recovering")
    } else {
      WorkflowStateEvent(Utils.aggregatedStateToString(state.state))
    }
  }

  var workflow: Workflow = _

  // Runtime starts from here:
  logger.info("Initialing an AmberClient, runtime starting...")
  var client: AmberClient = _
  var executionReconfigurationService: ExecutionReconfigurationService = _
  var executionStatsService: ExecutionStatsService = _
  var executionRuntimeService: ExecutionRuntimeService = _
  var executionConsoleService: ExecutionConsoleService = _

  def executeWorkflow(): Unit = {
    try {
      workflow = new WorkflowCompiler(workflowContext)
        .compile(request.logicalPlan)
    } catch {
      case err: Throwable =>
        errorHandler(err)
    }

    client = ComputingUnitMaster.createAmberRuntime(
      workflowContext,
      workflow.physicalPlan,
      controllerConfig,
      errorHandler
    )
    executionReconfigurationService =
      new ExecutionReconfigurationService(client, executionStateStore, workflow)
    // Create the operatorId to executionId map
    val operatorIdToExecutionId: Map[String, ULong] =
      if (AmberConfig.isUserSystemEnabled)
        createOperatorIdToExecutionIdMap(workflow)
      else
        Map.empty
    executionStatsService =
      new ExecutionStatsService(client, executionStateStore, operatorIdToExecutionId)
    executionRuntimeService = new ExecutionRuntimeService(
      client,
      executionStateStore,
      wsInput,
      executionReconfigurationService,
      controllerConfig.faultToleranceConfOpt
    )
    executionConsoleService = new ExecutionConsoleService(client, executionStateStore, wsInput)

    logger.info("Starting the workflow execution.")
    resultService.attachToExecution(executionStateStore, workflow.physicalPlan, client)
    executionStateStore.metadataStore.updateState(metadataStore =>
      updateWorkflowState(READY, metadataStore)
        .withFatalErrors(Seq.empty)
    )
    executionStateStore.statsStore.updateState(stats =>
      stats.withStartTimeStamp(System.currentTimeMillis())
    )
    client.controllerInterface
      .startWorkflow(EmptyRequest(), ())
      .onSuccess(resp =>
        executionStateStore.metadataStore.updateState(metadataStore =>
          if (metadataStore.state != FAILED) {
            updateWorkflowState(resp.workflowState, metadataStore)
          } else {
            metadataStore
          }
        )
      )
  }

  private def createOperatorIdToExecutionIdMap(workflow: Workflow): Map[String, ULong] = {
    val executionList: util.ArrayList[OperatorExecutions] = new util.ArrayList[OperatorExecutions]()
    val operatorIdToExecutionId = scala.collection.mutable.Map[String, ULong]()

    workflow.logicalPlan.operators.foreach { operator =>
      val operatorId = operator.operatorIdentifier.id
      val execution = new OperatorExecutions()
      execution.setWorkflowExecutionId(UInteger.valueOf(workflowContext.executionId.id))
      execution.setOperatorId(operatorId)
      executionList.add(execution)
    }

    val insertedExecutionIds = WorkflowExecutionsResource.insertOperatorExecutions(executionList)
    insertedExecutionIds.forEach {
      case (operatorId, executionId) =>
        operatorIdToExecutionId += (operatorId -> executionId)
    }

    operatorIdToExecutionId.toMap
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
    if (emailNotificationService.nonEmpty) {
      emailNotificationService.get.shutdown()
    }

  }

}
