package edu.uci.ics.texera.web.resource

import java.util.concurrent.atomic.AtomicInteger
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.{ServletAwareConfigurator, SessionState, SessionStateManager}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.websocket.event.{
  TexeraWebSocketEvent,
  WorkflowErrorEvent,
  WorkflowStateEvent
}
import edu.uci.ics.texera.web.model.websocket.request._
import edu.uci.ics.texera.web.model.websocket.response._
import edu.uci.ics.texera.web.service.{WorkflowCacheService, WorkflowService}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler.ConstraintViolationException

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

  final val objectMapper = Utils.objectMapper

  private def send(session: Session, msg: TexeraWebSocketEvent): Unit = {
    session.getAsyncRemote.sendText(objectMapper.writeValueAsString(msg))
  }

  @OnOpen
  def myOnOpen(session: Session, config: EndpointConfig): Unit = {
    SessionStateManager.setState(session.getId, new SessionState(session))
    logger.info("connection open")
  }

  @OnClose
  def myOnClose(session: Session, cr: CloseReason): Unit = {
    SessionStateManager.removeState(session.getId)
  }

  @OnMessage
  def myOnMsg(session: Session, message: String): Unit = {
    val request = objectMapper.readValue(message, classOf[TexeraWebSocketRequest])
    val uidOpt = session.getUserProperties.asScala
      .get(classOf[User].getName)
      .map(_.asInstanceOf[User].getUid)
    val sessionState = SessionStateManager.getState(session.getId)
    val workflowStateOpt = sessionState.getCurrentWorkflowState
    try {
      request match {
        case wIdRequest: RegisterWIdRequest =>
          // hack to refresh frontend run button state
          send(session, WorkflowStateEvent("Uninitialized"))
          val workflowState = uidOpt match {
            case Some(user) =>
              val workflowStateId = user + "-" + wIdRequest.wId
              WorkflowService.getOrCreate(workflowStateId)
            case None =>
              // use a fixed wid for reconnection
              val workflowStateId = "dummy wid"
              WorkflowService.getOrCreate(workflowStateId)
            // Alternative:
            // anonymous session: set immediately cleanup
            // WorkflowService.getOrCreate("anonymous session " + session.getId, 0)
          }
          sessionState.subscribe(workflowState)
          send(session, RegisterWIdResponse("wid registered"))
        case heartbeat: HeartBeatRequest =>
          send(session, HeartBeatResponse())
        case paginationRequest: ResultPaginationRequest =>
          workflowStateOpt.foreach(state =>
            send(session, state.resultService.handleResultPagination(paginationRequest))
          )
        case resultExportRequest: ResultExportRequest =>
          workflowStateOpt.foreach(state =>
            send(session, state.exportService.exportResult(uidOpt.get, resultExportRequest))
          )
        case other =>
          workflowStateOpt match {
            case Some(workflow) => workflow.wsInput.onNext(other, uidOpt)
            case None           => throw new IllegalStateException("workflow is not initialized")
          }

//        case req: AddBreakpointRequest =>
//          workflowStateOpt.foreach(
//            _.jobService.foreach(
//              _.jobRuntimeService.addBreakpoint(req.operatorID, req.breakpoint)
//            )
//          )
//
//        case cacheStatusUpdateRequest: CacheStatusUpdateRequest =>
//          if (WorkflowCacheService.isAvailable) {
//            workflowStateOpt.foreach(_.operatorCache.updateCacheStatus(cacheStatusUpdateRequest))
//          }
      }
    } catch {
      case x: ConstraintViolationException =>
        send(session, WorkflowErrorEvent(operatorErrors = x.violations))
      case err: Exception =>
        send(
          session,
          WorkflowErrorEvent(generalErrors =
            Map("exception" -> (err.getMessage + "\n" + err.getStackTrace.mkString("\n")))
          )
        )
        throw err
    }

  }

}
