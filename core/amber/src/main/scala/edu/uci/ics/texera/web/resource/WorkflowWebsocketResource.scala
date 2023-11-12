package edu.uci.ics.texera.web.resource

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.clustering.ClusterListener
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.websocket.event.{WorkflowErrorEvent, WorkflowStateEvent}
import edu.uci.ics.texera.web.model.websocket.request._
import edu.uci.ics.texera.web.model.websocket.response._
import edu.uci.ics.texera.web.service.{WorkflowCacheChecker, WorkflowService}
import edu.uci.ics.texera.web.storage.JobStateStore
import edu.uci.ics.texera.web.workflowruntimestate.FatalErrorType.COMPILATION_ERROR
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{FAILED, PAUSED, RUNNING}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowFatalError
import edu.uci.ics.texera.web.{ServletAwareConfigurator, SessionState}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.LogicalPlan

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import javax.websocket._
import javax.websocket.server.ServerEndpoint
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

object WorkflowWebsocketResource {
  val nextExecutionID = new AtomicInteger(0)
}

@ServerEndpoint(
  value = "/wsapi/workflow-websocket",
  configurator = classOf[ServletAwareConfigurator]
)
class WorkflowWebsocketResource extends LazyLogging {

  @OnOpen
  def myOnOpen(session: Session, config: EndpointConfig): Unit = {
    SessionState.setState(session.getId, new SessionState(session))
    logger.info("connection open")
  }

  @OnClose
  def myOnClose(session: Session, cr: CloseReason): Unit = {
    SessionState.removeState(session.getId)
  }

  @OnMessage
  def myOnMsg(session: Session, message: String): Unit = {
    val request = objectMapper.readValue(message, classOf[TexeraWebSocketRequest])
    val uidOpt = session.getUserProperties.asScala
      .get(classOf[User].getName)
      .map(_.asInstanceOf[User].getUid)
    val sessionState = SessionState.getState(session.getId)
    val workflowStateOpt = sessionState.getCurrentWorkflowState
    try {
      request match {
        case wIdRequest: RegisterWIdRequest =>
          // hack to refresh frontend run button state
          sessionState.send(WorkflowStateEvent("Uninitialized"))
          val workflowState = WorkflowService.getOrCreate(wIdRequest.wId)
          sessionState.subscribe(workflowState)
          sessionState.send(ClusterStatusUpdateEvent(ClusterListener.numWorkerNodesInCluster))
          sessionState.send(RegisterWIdResponse("wid registered"))
        case heartbeat: HeartBeatRequest =>
          sessionState.send(HeartBeatResponse())
        case paginationRequest: ResultPaginationRequest =>
          workflowStateOpt.foreach(state =>
            sessionState.send(state.resultService.handleResultPagination(paginationRequest))
          )
        case resultExportRequest: ResultExportRequest =>
          workflowStateOpt.foreach(state =>
            sessionState.send(state.exportService.exportResult(uidOpt.get, resultExportRequest))
          )
        case modifyLogicRequest: ModifyLogicRequest =>
          if (workflowStateOpt.isDefined) {
            val jobService = workflowStateOpt.get.jobService.getValue
            val modifyLogicResponse =
              jobService.jobReconfigurationService.modifyOperatorLogic(modifyLogicRequest)
            sessionState.send(modifyLogicResponse)
          }
        case editingTimeCompilationRequest: EditingTimeCompilationRequest =>
          if (workflowStateOpt.isDefined) {
            var stateStore = new JobStateStore()
            if (workflowStateOpt.get.jobService.hasValue) {
              val currentState =
                workflowStateOpt.get.jobService.getValue.stateStore.jobMetadataStore.getState.state
              if (currentState == RUNNING || currentState == PAUSED) {
                // disable check if the workflow execution is active.
                return
              }
              stateStore = workflowStateOpt.get.jobService.getValue.stateStore
            }
            val newPlan = {
              LogicalPlan.apply(
                editingTimeCompilationRequest.toLogicalPlanPojo(),
                new WorkflowContext()
              )
            }
            newPlan.initializeLogicalPlan(stateStore)
            if (stateStore.jobMetadataStore.getState.state == FAILED) {
              sessionState.send(WorkflowStateEvent("Failed"))
              sessionState.send(
                WorkflowErrorEvent(stateStore.jobMetadataStore.getState.fatalErrors)
              )
            } else {
              WorkflowCacheChecker.handleCacheStatusUpdate(
                workflowStateOpt.get.lastCompletedLogicalPlan,
                newPlan,
                sessionState,
                editingTimeCompilationRequest
              )
            }
          }
        case workflowExecuteRequest: WorkflowExecuteRequest =>
          workflowStateOpt match {
            case Some(workflow) => workflow.initJobService(workflowExecuteRequest, uidOpt)
            case None           => throw new IllegalStateException("workflow is not initialized")
          }
        case other =>
          workflowStateOpt.map(_.jobService.getValue) match {
            case Some(value) => value.wsInput.onNext(other, uidOpt)
            case None        => throw new IllegalStateException("workflow job is not initialized")
          }
      }
    } catch {
      case err: Exception =>
        logger.error("error occurred in websocket", err)
        sessionState.send(WorkflowStateEvent("Failed"))
        sessionState.send(
          WorkflowErrorEvent(
            Seq(
              WorkflowFatalError(
                COMPILATION_ERROR,
                Timestamp(Instant.now),
                err.toString,
                err.getStackTrace.mkString("\n"),
                "unknown operator"
              )
            )
          )
        )
        throw err
    }

  }

}
