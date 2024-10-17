package edu.uci.ics.texera.web.resource

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.clustering.ClusterListener
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  PAUSED,
  RUNNING
}
import edu.uci.ics.amber.engine.common.model.WorkflowContext
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.amber.error.ErrorUtils.getStackTraceWithAllCauses
import edu.uci.ics.amber.engine.common.Utils.objectMapper
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.websocket.event.{
  CacheStatusUpdateEvent,
  WorkflowErrorEvent,
  WorkflowStateEvent
}
import edu.uci.ics.texera.web.model.websocket.request._
import edu.uci.ics.texera.web.model.websocket.response._
import edu.uci.ics.texera.web.service.{WorkflowCacheChecker, WorkflowService}
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.amber.engine.common.workflowruntimestate.FatalErrorType.COMPILATION_ERROR
import edu.uci.ics.amber.engine.common.workflowruntimestate.WorkflowFatalError
import edu.uci.ics.texera.web.{ServletAwareConfigurator, SessionState}
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler

import java.time.Instant
import javax.websocket._
import javax.websocket.server.ServerEndpoint
import scala.jdk.CollectionConverters.MapHasAsScala

@ServerEndpoint(
  value = "/wsapi/workflow-websocket",
  configurator = classOf[ServletAwareConfigurator]
)
class WorkflowWebsocketResource extends LazyLogging {

  @OnOpen
  def myOnOpen(session: Session, config: EndpointConfig): Unit = {
    val sessionState = new SessionState(session)
    SessionState.setState(session.getId, sessionState)
    val wid = session.getRequestParameterMap.get("wid").get(0).toLong
    // hack to refresh frontend run button state
    sessionState.send(WorkflowStateEvent("Uninitialized"))
    val workflowState =
      WorkflowService.getOrCreate(WorkflowIdentity(wid))
    sessionState.subscribe(workflowState)
    sessionState.send(ClusterStatusUpdateEvent(ClusterListener.numWorkerNodesInCluster))
    logger.info("connection open")
  }

  @OnClose
  def myOnClose(session: Session, cr: CloseReason): Unit = {
    SessionState.removeState(session.getId)
  }

  @OnMessage
  def myOnMsg(session: Session, message: String): Unit = {
    val request = objectMapper.readValue(message, classOf[TexeraWebSocketRequest])
    val userOpt = session.getUserProperties.asScala
      .get(classOf[User].getName)
      .map(_.asInstanceOf[User])
    val uidOpt = userOpt.map(_.getUid)

    val sessionState = SessionState.getState(session.getId)
    val workflowStateOpt = sessionState.getCurrentWorkflowState
    val executionStateOpt = workflowStateOpt.flatMap(x => Option(x.executionService.getValue))
    try {
      request match {
        case heartbeat: HeartBeatRequest =>
          sessionState.send(HeartBeatResponse())
        case paginationRequest: ResultPaginationRequest =>
          workflowStateOpt.foreach(state =>
            sessionState.send(state.resultService.handleResultPagination(paginationRequest))
          )
        case resultExportRequest: ResultExportRequest =>
          workflowStateOpt.foreach(state =>
            sessionState.send(state.exportService.exportResult(userOpt.get, resultExportRequest))
          )
        case modifyLogicRequest: ModifyLogicRequest =>
          if (workflowStateOpt.isDefined) {
            val executionService = workflowStateOpt.get.executionService.getValue
            val modifyLogicResponse =
              executionService.executionReconfigurationService.modifyOperatorLogic(
                modifyLogicRequest
              )
            sessionState.send(modifyLogicResponse)
          }
        case editingTimeCompilationRequest: EditingTimeCompilationRequest =>
          // TODO: remove this after separating the workflow compiler as a standalone service
          val stateStore = if (executionStateOpt.isDefined) {
            val currentState =
              executionStateOpt.get.executionStateStore.metadataStore.getState.state
            if (currentState == RUNNING || currentState == PAUSED) {
              // disable check if the workflow execution is active.
              return
            }
            executionStateOpt.get.executionStateStore
          } else {
            new ExecutionStateStore()
          }
          val workflowContext = new WorkflowContext(
            sessionState.getCurrentWorkflowState.get.workflowId
          )
          try {
            val workflowCompiler =
              new WorkflowCompiler(workflowContext)
            val newPlan = workflowCompiler.compileLogicalPlan(
              editingTimeCompilationRequest.toLogicalPlanPojo,
              stateStore
            )
            val validateResult = WorkflowCacheChecker.handleCacheStatusUpdate(
              workflowStateOpt.get.lastCompletedLogicalPlan,
              newPlan,
              editingTimeCompilationRequest
            )
            sessionState.send(CacheStatusUpdateEvent(validateResult))
          } catch {
            case t: Throwable => // skip, rethrow this exception will overwrite the compilation errors reported below.
          } finally {
            if (stateStore.metadataStore.getState.fatalErrors.nonEmpty) {
              sessionState.send(WorkflowErrorEvent(stateStore.metadataStore.getState.fatalErrors))
            }
          }
        case workflowExecuteRequest: WorkflowExecuteRequest =>
          workflowStateOpt match {
            case Some(workflow) =>
              workflow.initExecutionService(workflowExecuteRequest, userOpt, session.getRequestURI)
            case None => throw new IllegalStateException("workflow is not initialized")
          }
        case other =>
          workflowStateOpt.map(_.executionService.getValue) match {
            case Some(value) => value.wsInput.onNext(other, uidOpt)
            case None        => throw new IllegalStateException("workflow execution is not initialized")
          }
      }
    } catch {
      case err: Exception =>
        logger.error("error occurred in websocket", err)
        val errEvt = WorkflowFatalError(
          COMPILATION_ERROR,
          Timestamp(Instant.now),
          err.toString,
          getStackTraceWithAllCauses(err),
          "unknown operator"
        )
        if (executionStateOpt.isDefined) {
          executionStateOpt.get.executionStateStore.metadataStore.updateState { metadataStore =>
            metadataStore
              .withFatalErrors(metadataStore.fatalErrors.filter(e => e.`type` != COMPILATION_ERROR))
              .addFatalErrors(errEvt)
          }
        } else {
          sessionState.send(
            WorkflowErrorEvent(
              Seq(errEvt)
            )
          )
        }
        throw err
    }

  }
}
