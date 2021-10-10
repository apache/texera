package edu.uci.ics.texera.web.resource

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.EvaluatePythonExpressionHandler.EvaluatePythonExpression
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ModifyLogicHandler.ModifyLogic
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RetryWorkflowHandler.RetryWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{Controller, ControllerConfig}
import edu.uci.ics.amber.engine.common.{AmberUtils, AmberClient}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.model.event._
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.request._
import edu.uci.ics.texera.web.resource.auth.UserResource
import edu.uci.ics.texera.web.model.request.python.PythonExpressionEvaluateRequest
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource._
import edu.uci.ics.texera.web.{ServletAwareConfigurator, TexeraWebApplication}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.storage.memory.{JCSOpResultStorage, MemoryOpResultStorage}
import edu.uci.ics.texera.workflow.common.storage.mongo.MongoOpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo.toJgraphtDAG
import edu.uci.ics.texera.workflow.common.workflow.{
  WorkflowCompiler,
  WorkflowInfo,
  WorkflowRewriter,
  WorkflowVertex
}
import edu.uci.ics.texera.workflow.operators.sink.CacheSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import org.jose4j.jwt.consumer.JwtContext
import java.util.concurrent.atomic.AtomicInteger

import edu.uci.ics.texera.web.resource.execution.{OperatorCache, OperatorResultService, WorkflowExecutionState, WorkflowResultService}
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler.ConstraintViolationException
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  BreakpointTriggered,
  ErrorOccurred,
  PythonPrintTriggered,
  ReportCurrentProcessingTuple,
  WorkflowCompleted,
  WorkflowPaused,
  WorkflowResultUpdate,
  WorkflowStatusUpdate
}
import javax.websocket._
import javax.websocket.server.ServerEndpoint

import scala.collection.{breakOut, mutable}
import scala.jdk.CollectionConverters.mapAsScalaMapConverter
import scala.util.{Failure, Success}

object WorkflowWebsocketResource {
  // TODO should reorganize this resource.
  val nextExecutionID = new AtomicInteger(0)
  val sessionIdToHttpSession = new mutable.HashMap[String, HttpSession]()
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
class WorkflowWebsocketResource extends LazyLogging{

  final val objectMapper = Utils.objectMapper
  import WorkflowWebsocketResource._

  private def sendInternal(session: Session, msg:TexeraWebSocketEvent): Unit ={
    session.getAsyncRemote.sendText(objectMapper.writeValueAsString(msg))
  }

  private def getExecutionState(session:Session): Option[WorkflowExecutionState] ={
    sessionIdToWId.get(session.getId) match {
      case Some(value) => wIdToExecutionState.get(value)
      case None => None
    }
  }

  @OnOpen
  def myOnOpen(session: Session, config: EndpointConfig): Unit = {
    sessionIdToHttpSession(session.getId) = config.getUserProperties.get("httpSession").asInstanceOf[HttpSession]
    val subscriber = new WebsocketSubscriber(session)
    sessionIdToObserver(session.getId) = subscriber
    val operatorCache = new OperatorCache()
    operatorCache.subscribe(subscriber)
    sessionIdToOperatorCache(session.getId) = operatorCache
    logger.info("connection open")
  }

  @OnClose
  def myOnClose(session: Session, cr: CloseReason): Unit = {
    tryUnbindExecution(session)
    sessionIdToOperatorCache.remove(session.getId)
    sessionIdToHttpSession.remove(session.getId)
    sessionIdToObserver.remove(session.getId)
  }

  def tryUnbindExecution(session: Session): Unit ={
    if(sessionIdToSubscription.contains(session.getId)){
      sessionIdToSubscription(session.getId).unsubscribe()
      sessionIdToSubscription.remove(session.getId)
    }
  }

  def tryBindExecution(session:Session, wId:String):Unit ={
    if(wIdToExecutionState.contains(wId)){
      val subscription = wIdToExecutionState(wId).subscribeAll(sessionIdToObserver(session.getId))
      sessionIdToSubscription(session.getId) = subscription
    }
  }

  @OnMessage
  def myOnMsg(session: Session, message: String): Unit = {
    val request = objectMapper.readValue(message, classOf[TexeraWebSocketRequest])

    try {
      request match {
        case wIdRequest: RegisterWIdRequest =>
          val uId = UserResource
            .getUser(sessionIdToHttpSession(session.getId))
            .map(u => u.getUid)
          val wId = uId.toString + "-" + wIdRequest.wId
          logger.info("start working on "+wId)
          tryUnbindExecution(session)
          tryBindExecution(session, wId)
          sessionIdToWId(session.getId) = wId
          sendInternal(session, RegisterWIdResponse("wid registered"))
        case heartbeat: HeartBeatRequest =>
          sendInternal(session, HeartBeatResponse())
        case execute: ExecuteWorkflowRequest =>
          println(execute)
          val wId = sessionIdToWId(session.getId)
          var executionState:WorkflowExecutionState = null
          val prevResults:mutable.HashMap[String, OperatorResultService] =
            if(wIdToExecutionState.contains(wId)){
              wIdToExecutionState(wId).workflowResultService.operatorResults
            }else{
              mutable.HashMap.empty
            }
          try{
            executionState = new WorkflowExecutionState(sessionIdToOperatorCache(session.getId), sessionIdToHttpSession(session.getId), execute, prevResults)
          }catch{
            case x:ConstraintViolationException => sendInternal(session, WorkflowErrorEvent(operatorErrors = x.violations))
            case other:Exception => throw other
          }
          wIdToExecutionState(wId) = executionState
          tryUnbindExecution(session)
          tryBindExecution(session, wId)
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
        case breakpoint: AddBreakpointRequest =>
          getExecutionState(session).foreach(_.workflowRuntimeService.addBreakpoint(breakpoint))
        case paginationRequest: ResultPaginationRequest =>
          getExecutionState(session).foreach(_.workflowResultService.handleResultPagination(paginationRequest))
        case resultExportRequest: ResultExportRequest =>
          getExecutionState(session).foreach{
            state => sendInternal(session, state.exportResult(sessionIdToHttpSession(session.getId), resultExportRequest))
          }
        case cacheStatusUpdateRequest: CacheStatusUpdateRequest =>
          if (OperatorCache.isAvailable) {
            getExecutionState(session).foreach(_.operatorCache.updateCacheStatus(cacheStatusUpdateRequest))
          }
        case pythonExpressionEvaluateRequest: PythonExpressionEvaluateRequest =>
          evaluatePythonExpression(session, pythonExpressionEvaluateRequest)
      }
    } catch {
      case err: Exception =>
        sendInternal(session,
          WorkflowErrorEvent(generalErrors =
            Map("exception" -> (err.getMessage + "\n" + err.getStackTrace.mkString("\n")))
          )
        )
        throw err
    }

    def evaluatePythonExpression(session: Session, request: PythonExpressionEvaluateRequest): Unit = {
      val client = WorkflowWebsocketResource.sessionJobs(session.getId)._2
      client
        .sendAsTwitterFuture(EvaluatePythonExpression(request.expression, request.operatorId))
        .onSuccess { ret =>
          send(session, ret)
        }
    }

  }


}
