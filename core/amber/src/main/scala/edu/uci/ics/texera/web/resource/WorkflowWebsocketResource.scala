package edu.uci.ics.texera.web.resource

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.ServletAwareConfigurator
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.websocket.event.{TexeraWebSocketEvent, WorkflowErrorEvent}
import edu.uci.ics.texera.web.model.websocket.request._
import edu.uci.ics.texera.web.model.websocket.request.python.PythonExpressionEvaluateRequest
import edu.uci.ics.texera.web.model.websocket.response._
import edu.uci.ics.texera.web.resource.execution.{
  OperatorCache,
  OperatorResultService,
  SessionState,
  SnapshotMulticast,
  WorkflowJobState,
  WorkflowState
}
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler.ConstraintViolationException
import javax.websocket._
import javax.websocket.server.ServerEndpoint

import scala.collection.mutable
import scala.jdk.CollectionConverters.mapAsScalaMapConverter
import scala.util.{Failure, Success}

object WorkflowWebsocketResource {
  val nextExecutionID = new AtomicInteger(0)
  val sessionIdToSessionState = new mutable.HashMap[String, SessionState]()
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
    SessionState.registerState(session.getId, new SessionState(session))
    logger.info("connection open")
  }

  @OnClose
  def myOnClose(session: Session, cr: CloseReason): Unit = {
    SessionState.unregisterState(session.getId)
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
          val wId = uidOpt.toString + "-" + wIdRequest.wId
          val workflowState = WorkflowState.getOrCreate(wId)
          sessionState.bind(workflowState)
          logger.info("start working on " + wId)
          send(session, RegisterWIdResponse("wid registered"))
        case heartbeat: HeartBeatRequest =>
          send(session, HeartBeatResponse())
        case execute: WorkflowExecuteRequest =>
          println(execute)
          try {
            workflowStateOpt.get.initExecutionState(execute, uidOpt)
          } catch {
            case x: ConstraintViolationException =>
              send(session, WorkflowErrorEvent(operatorErrors = x.violations))
            case other: Exception => throw other
          }
        case newLogic: ModifyLogicRequest =>
          workflowStateOpt.foreach(_.jobState.foreach(_.modifyLogic(newLogic)))
        case pause: WorkflowPauseRequest =>
          workflowStateOpt.foreach(
            _.jobState.foreach(_.workflowRuntimeService.pauseWorkflow())
          )
        case resume: WorkflowResumeRequest =>
          workflowStateOpt.foreach(
            _.jobState.foreach(_.workflowRuntimeService.resumeWorkflow())
          )
        case kill: WorkflowKillRequest =>
          workflowStateOpt.foreach(
            _.jobState.foreach(_.workflowRuntimeService.killWorkflow())
          )
        case skipTupleMsg: SkipTupleRequest =>
          workflowStateOpt.foreach(
            _.jobState.foreach(_.workflowRuntimeService.skipTuple(skipTupleMsg))
          )
        case retryRequest: RetryRequest =>
          workflowStateOpt.foreach(
            _.jobState.foreach(_.workflowRuntimeService.retryWorkflow())
          )
        case req: AddBreakpointRequest =>
          workflowStateOpt.foreach(
            _.jobState.foreach(
              _.workflowRuntimeService.addBreakpoint(req.operatorID, req.breakpoint)
            )
          )
        case paginationRequest: ResultPaginationRequest =>
          workflowStateOpt.foreach(
            _.jobState.foreach(
              _.workflowResultService.handleResultPagination(paginationRequest)
            )
          )
        case resultExportRequest: ResultExportRequest =>
          workflowStateOpt.foreach(_.jobState.foreach { state =>
            send(session, state.exportResult(uidOpt.get, resultExportRequest))
          })
        case cacheStatusUpdateRequest: CacheStatusUpdateRequest =>
          if (OperatorCache.isAvailable) {
            workflowStateOpt.foreach(_.operatorCache.updateCacheStatus(cacheStatusUpdateRequest))
          }
        case pythonExpressionEvaluateRequest: PythonExpressionEvaluateRequest =>
          workflowStateOpt.foreach(
            _.jobState.foreach(
              _.workflowRuntimeService.evaluatePythonExpression(pythonExpressionEvaluateRequest)
            )
          )
      }
    } catch {
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
