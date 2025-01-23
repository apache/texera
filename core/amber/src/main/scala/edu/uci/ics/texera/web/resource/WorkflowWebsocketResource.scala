package edu.uci.ics.texera.web.resource

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.clustering.ClusterListener
import edu.uci.ics.amber.engine.common.Utils.objectMapper
import edu.uci.ics.amber.error.ErrorUtils.getStackTraceWithAllCauses
import edu.uci.ics.amber.core.virtualidentity.WorkflowIdentity
import edu.uci.ics.amber.core.workflowruntimestate.FatalErrorType.COMPILATION_ERROR
import edu.uci.ics.amber.core.workflowruntimestate.WorkflowFatalError
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.websocket.event.{WorkflowErrorEvent, WorkflowStateEvent}
import edu.uci.ics.texera.web.model.websocket.request._
import edu.uci.ics.texera.web.model.websocket.response._
import edu.uci.ics.texera.web.service.WorkflowService
import edu.uci.ics.texera.web.{ServletAwareConfigurator, SessionState}

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
