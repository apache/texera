package edu.uci.ics.texera.web.resource

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.ServletAwareConfigurator
import edu.uci.ics.texera.web.model.event._
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.request._
import edu.uci.ics.texera.web.model.request.python.PythonExpressionEvaluateRequest
import edu.uci.ics.texera.web.resource.execution.{
  OperatorCache,
  OperatorResultService,
  WorkflowExecutionState
}
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler.ConstraintViolationException
import javax.websocket._
import javax.websocket.server.ServerEndpoint
import rx.lang.scala.{Observer, Subscription}

import scala.collection.mutable
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

object WorkflowWebsocketResource {
  // TODO should reorganize this resource.
  val nextExecutionID = new AtomicInteger(0)
  val sessionIdToWId = new mutable.HashMap[String, String]()
  val sessionIdToObserver = new mutable.HashMap[String, Observer[TexeraWebSocketEvent]]()
  val sessionIdToSubscription = new mutable.HashMap[String, Subscription]()
  val wIdToExecutionState = new mutable.HashMap[String, WorkflowExecutionState]()
  val sessionIdToOperatorCache = new mutable.HashMap[String, OperatorCache]()
}

@ServerEndpoint(
  value = "/wsapi/workflow-websocket",
  configurator = classOf[ServletAwareConfigurator]
)
class WorkflowWebsocketResource extends LazyLogging {

  final val objectMapper = Utils.objectMapper
  import WorkflowWebsocketResource._

  private def sendInternal(session: Session, msg: TexeraWebSocketEvent): Unit = {
    session.getAsyncRemote.sendText(objectMapper.writeValueAsString(msg))
  }

  private def getExecutionState(session: Session): Option[WorkflowExecutionState] = {
    sessionIdToWId.get(session.getId) match {
      case Some(value) => wIdToExecutionState.get(value)
      case None        => None
    }
  }

  @OnOpen
  def myOnOpen(session: Session, config: EndpointConfig): Unit = {
    val subscriber = new WebsocketSubscriber(session)
    sessionIdToObserver(session.getId) = subscriber
    val operatorCache = new OperatorCache()
    operatorCache.subscribe(subscriber)
    sessionIdToOperatorCache(session.getId) = operatorCache
    logger.info("connection open")
  }

  @OnClose
  def myOnClose(session: Session, cr: CloseReason): Unit = {
    tryUnbindExecution(session.getId)
    sessionIdToOperatorCache.remove(session.getId)
    sessionIdToObserver.remove(session.getId)
    sessionIdToWId.remove(session.getId)
  }

  def tryUnbindExecution(sId: String): Unit = {
    if (sessionIdToSubscription.contains(sId)) {
      sessionIdToSubscription(sId).unsubscribe()
      sessionIdToSubscription.remove(sId)
    }
  }

  def BindSessionToExecution(sId: String, wId: String): Unit = {
    tryUnbindExecution(sId)
    if (wIdToExecutionState.contains(wId)) {
      val subscription = wIdToExecutionState(wId).subscribeAll(sessionIdToObserver(sId))
      sessionIdToSubscription(sId) = subscription
    }
  }

  @OnMessage
  def myOnMsg(session: Session, message: String): Unit = {
    val request = objectMapper.readValue(message, classOf[TexeraWebSocketRequest])
    val uidOpt = session.getUserProperties.asScala
      .get(classOf[User].getName)
      .map(_.asInstanceOf[User].getUid)
    try {
      request match {
        case wIdRequest: RegisterWIdRequest =>
          val wId = uidOpt.toString + "-" + wIdRequest.wId
          logger.info("start working on " + wId)
          BindSessionToExecution(session.getId, wId)
          sessionIdToWId(session.getId) = wId
          sendInternal(session, RegisterWIdResponse("wid registered"))
        case heartbeat: HeartBeatRequest =>
          sendInternal(session, HeartBeatResponse())
        case execute: ExecuteWorkflowRequest =>
          println(execute)
          val wId = sessionIdToWId(session.getId)
          var executionState: WorkflowExecutionState = null
          val prevResults: mutable.HashMap[String, OperatorResultService] =
            if (wIdToExecutionState.contains(wId)) {
              wIdToExecutionState(wId).workflowResultService.operatorResults
            } else {
              mutable.HashMap.empty
            }
          try {
            executionState = new WorkflowExecutionState(
              sessionIdToOperatorCache(session.getId),
              uidOpt,
              execute,
              prevResults
            )
          } catch {
            case x: ConstraintViolationException =>
              sendInternal(session, WorkflowErrorEvent(operatorErrors = x.violations))
            case other: Exception => throw other
          }
          wIdToExecutionState(wId) = executionState
          // bind existing sessions to new execution
          sessionIdToWId.filter(_._2 == wId).foreach {
            case (sId, _) => BindSessionToExecution(sId, wId)
          }
        case newLogic: ModifyLogicRequest =>
          getExecutionState(session).foreach(_.modifyLogic(newLogic))
        case pause: PauseWorkflowRequest =>
          getExecutionState(session).foreach(_.workflowRuntimeService.pauseWorkflow())
        case resume: ResumeWorkflowRequest =>
          getExecutionState(session).foreach(_.workflowRuntimeService.resumeWorkflow())
        case kill: KillWorkflowRequest =>
          getExecutionState(session).foreach(_.workflowRuntimeService.killWorkflow())
        case skipTupleMsg: SkipTupleRequest =>
          getExecutionState(session).foreach(_.workflowRuntimeService.skipTuple(skipTupleMsg))
        case retryRequest: RetryRequest =>
          getExecutionState(session).foreach(_.workflowRuntimeService.retryWorkflow())
        case req: AddBreakpointRequest =>
          getExecutionState(session).foreach(
            _.workflowRuntimeService.addBreakpoint(req.operatorID, req.breakpoint)
          )
        case paginationRequest: ResultPaginationRequest =>
          getExecutionState(session).foreach(
            _.workflowResultService.handleResultPagination(paginationRequest)
          )
        case resultExportRequest: ResultExportRequest =>
          getExecutionState(session).foreach { state =>
            sendInternal(session, state.exportResult(uidOpt.get, resultExportRequest))
          }
        case cacheStatusUpdateRequest: CacheStatusUpdateRequest =>
          if (OperatorCache.isAvailable) {
            getExecutionState(session).foreach(
              _.operatorCache.updateCacheStatus(cacheStatusUpdateRequest)
            )
          }
        case pythonExpressionEvaluateRequest: PythonExpressionEvaluateRequest =>
          getExecutionState(session).foreach(
            _.workflowRuntimeService.evaluatePythonExpression(pythonExpressionEvaluateRequest)
          )
      }
    } catch {
      case err: Exception =>
        sendInternal(
          session,
          WorkflowErrorEvent(generalErrors =
            Map("exception" -> (err.getMessage + "\n" + err.getStackTrace.mkString("\n")))
          )
        )
        throw err
    }

  }

}
