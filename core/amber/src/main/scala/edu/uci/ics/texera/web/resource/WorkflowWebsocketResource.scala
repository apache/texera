package edu.uci.ics.texera.web.resource

import akka.actor.{ActorRef, PoisonPill}
import edu.uci.ics.amber.engine.architecture.controller.{
  Controller,
  ControllerConfig,
  ControllerEventListener
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.{ServletAwareConfigurator, TexeraWebApplication}
import edu.uci.ics.texera.web.model.event._
import edu.uci.ics.texera.web.model.request._
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource.{
  send,
  sessionDownloadCache,
  sessionJobs,
  sessionMap,
  sessionResults
}
import edu.uci.ics.texera.web.resource.auth.UserResource
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.{
  WorkflowCompiler,
  WorkflowInfo,
  WorkflowRewriter,
  WorkflowVertex
}
import edu.uci.ics.texera.workflow.common.{Utils, WorkflowContext}

import java.util.concurrent.atomic.AtomicInteger
import edu.uci.ics.texera.workflow.common.Utils.objectMapper

import javax.servlet.http.HttpSession
import javax.websocket.{EndpointConfig, _}
import javax.websocket.server.ServerEndpoint
import scala.collection.{breakOut, mutable}
import com.typesafe.scalalogging.Logger
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo.toJgraphtDAG
import edu.uci.ics.texera.workflow.operators.sink.CacheSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc

object WorkflowWebsocketResource {
  // TODO should reorganize this resource.

  val nextJobID = new AtomicInteger(0)

  // Map[sessionId, (Session, HttpSession)]
  val sessionMap = new mutable.HashMap[String, (Session, HttpSession)]

  // Map[sessionId, (WorkflowCompiler, ActorRef)]
  val sessionJobs = new mutable.HashMap[String, (WorkflowCompiler, ActorRef)]

  // Map[sessionId, Map[operatorId, List[ITuple]]]
  val sessionResults = new mutable.HashMap[String, WorkflowResultService]

  // Map[sessionId, Map[downloadType, googleSheetLink]
  val sessionDownloadCache = new mutable.HashMap[String, mutable.HashMap[String, String]]

  def send(session: Session, event: TexeraWebSocketEvent): Unit = {
    session.getAsyncRemote.sendText(objectMapper.writeValueAsString(event))
  }
}

@ServerEndpoint(
  value = "/wsapi/workflow-websocket",
  configurator = classOf[ServletAwareConfigurator]
)
class WorkflowWebsocketResource {

  private val logger = Logger(this.getClass.getName)

  final val objectMapper = Utils.objectMapper

  @OnOpen
  def myOnOpen(session: Session, config: EndpointConfig): Unit = {
    WorkflowWebsocketResource.sessionMap.update(
      session.getId,
      (session, config.getUserProperties.get("httpSession").asInstanceOf[HttpSession])
    )
    println("connection open")
  }

  @OnMessage
  def myOnMsg(session: Session, message: String): Unit = {
    val request = objectMapper.readValue(message, classOf[TexeraWebSocketRequest])
    try {
      request match {
        case helloWorld: HelloWorldRequest =>
          send(session, HelloWorldResponse("hello from texera web server"))
        case heartbeat: HeartBeatRequest =>
          send(session, HeartBeatResponse())
        case execute: ExecuteWorkflowRequest =>
          println(execute)
          executeWorkflow(session, execute)
        case newLogic: ModifyLogicRequest =>
          println(newLogic)
          modifyLogic(session, newLogic)
        case pause: PauseWorkflowRequest =>
          pauseWorkflow(session)
        case resume: ResumeWorkflowRequest =>
          resumeWorkflow(session)
        case kill: KillWorkflowRequest =>
          killWorkflow(session)
        case skipTupleMsg: SkipTupleRequest =>
          skipTuple(session, skipTupleMsg)
        case breakpoint: AddBreakpointRequest =>
          addBreakpoint(session, breakpoint)
        case paginationRequest: ResultPaginationRequest =>
          resultPagination(session, paginationRequest)
        case resultDownloadRequest: ResultDownloadRequest =>
          downloadResult(session, resultDownloadRequest)
      }
    } catch {
      case e: Throwable =>
        send(
          session,
          WorkflowErrorEvent(generalErrors =
            Map("exception" -> (e.getMessage + "\n" + e.getStackTrace.mkString("\n")))
          )
        )
        throw e
    }

  }

  def resultPagination(session: Session, request: ResultPaginationRequest): Unit = {
    var operatorID = request.operatorID
    if (!sessionResults(session.getId).operatorResults.contains(operatorID)) {
      val downstreamIDs = sessionResults(session.getId).workflowCompiler.workflow
        .getDownstream(operatorID)
      for (elem <- downstreamIDs) {
        if (elem.isInstanceOf[CacheSinkOpDesc]) {
          operatorID = elem.operatorID
          breakOut
        }
      }
    }
    val opResultService = sessionResults(session.getId).operatorResults(operatorID)
    // calculate from index (pageIndex starts from 1 instead of 0)
    val from = request.pageSize * (request.pageIndex - 1)
    val paginationResults = opResultService.getResult
      .slice(from, from + request.pageSize)
      .map(tuple => tuple.asInstanceOf[Tuple].asKeyValuePairJson())

    send(session, PaginatedResultEvent.apply(request, paginationResults))
  }

  def addBreakpoint(session: Session, addBreakpoint: AddBreakpointRequest): Unit = {
    val compiler = WorkflowWebsocketResource.sessionJobs(session.getId)._1
    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    compiler.addBreakpoint(controller, addBreakpoint.operatorID, addBreakpoint.breakpoint)
  }

  def skipTuple(session: Session, tupleReq: SkipTupleRequest): Unit = {
//    val actorPath = tupleReq.actorPath
//    val faultedTuple = tupleReq.faultedTuple
//    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
//    controller ! SkipTupleGivenWorkerRef(actorPath, faultedTuple.toFaultedTuple())
    throw new RuntimeException("skipping tuple is temporarily disabled")
  }

  def modifyLogic(session: Session, newLogic: ModifyLogicRequest): Unit = {
//    val texeraOperator = newLogic.operator
//    val (compiler, controller) = WorkflowWebsocketResource.sessionJobs(session.getId)
//    compiler.initOperator(texeraOperator)
//    controller ! ModifyLogic(texeraOperator.operatorExecutor)
    throw new RuntimeException("modify logic is temporarily disabled")
  }

  def pauseWorkflow(session: Session): Unit = {
    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    controller ! ControlInvocation(AsyncRPCClient.IgnoreReply, PauseWorkflow())
    // workflow paused event will be send after workflow is actually paused
    // the callback function will handle sending the paused event to frontend
  }

  def resumeWorkflow(session: Session): Unit = {
    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    controller ! ControlInvocation(AsyncRPCClient.IgnoreReply, ResumeWorkflow())
    send(session, WorkflowResumedEvent())
  }

  var operatorOutputCache: mutable.HashMap[String, mutable.MutableList[Tuple]] =
    mutable.HashMap[String, mutable.MutableList[Tuple]]()
  var cachedOperators: mutable.HashMap[String, OperatorDescriptor] =
    mutable.HashMap[String, OperatorDescriptor]()
  var cacheSourceOperators: mutable.HashMap[String, CacheSourceOpDesc] =
    mutable.HashMap[String, CacheSourceOpDesc]()
  var cacheSinkOperators: mutable.HashMap[String, CacheSinkOpDesc] =
    mutable.HashMap[String, CacheSinkOpDesc]()
  var operatorRecord: mutable.HashMap[String, WorkflowVertex] =
    mutable.HashMap[String, WorkflowVertex]()

  def executeWorkflow(session: Session, request: ExecuteWorkflowRequest): Unit = {
    logger.info("Session id: {}", session.getId)
    val context = new WorkflowContext
    val jobID = Integer.toString(WorkflowWebsocketResource.nextJobID.incrementAndGet)
    context.jobID = jobID
    context.userID = UserResource
      .getUser(sessionMap(session.getId)._2)
      .map(u => u.getUid)

    var workflowInfo = WorkflowInfo(request.operators, request.links, request.breakpoints)
    workflowInfo.cachedOperatorIDs = request.cachedOperatorIDs
    logger.info("Cached operators: {}.", cachedOperators.toString())
    logger.info("request.cachedOperatorIDs: {}.", request.cachedOperatorIDs)
    val workflowRewriter = new WorkflowRewriter(
      workflowInfo,
      operatorOutputCache,
      cachedOperators,
      cacheSourceOperators,
      cacheSinkOperators,
      operatorRecord
    )
    val newWorkflowInfo = workflowRewriter.rewrite_v2
    logger.info("Original workflow: {}.", toJgraphtDAG(workflowInfo).toString)
    workflowInfo = newWorkflowInfo
    logger.info("Rewritten workflow: {}.", toJgraphtDAG(workflowInfo).toString)
    val texeraWorkflowCompiler = new WorkflowCompiler(workflowInfo, context)
    logger.info("TexeraWorkflowCompiler constructed: {}.", texeraWorkflowCompiler)
    val violations = texeraWorkflowCompiler.validate
    if (violations.nonEmpty) {
      send(session, WorkflowErrorEvent(violations))
      return
    }

    val workflow = texeraWorkflowCompiler.amberWorkflow
    val workflowTag = WorkflowIdentity(jobID)

    val workflowResultService = new WorkflowResultService(texeraWorkflowCompiler)
    if (!sessionResults.contains(session.getId)) {
      sessionResults(session.getId) = workflowResultService
    } else {
      val previousWorkflowResultService = sessionResults(session.getId)
      val previousResults = previousWorkflowResultService.operatorResults
      val results = workflowResultService.operatorResults
      results.foreach(e => {
        if (previousResults.contains(e._2.operatorID)) {
          previousResults(e._2.operatorID) = e._2
        }
      })
//      previousResults.foreach(e => {
      //        if (results.contains(e._1)) {
      //          previousResults(e._1) = results(e._1)
      //        }
      //      })
      previousResults.foreach(e => {
        if (cachedOperators.contains(e._2.operatorID) && !results.contains(e._2.operatorID)) {
          results += ((e._2.operatorID, e._2))
        }
//        if (previousWorkflowResultService.workflowCompiler.workflow.operators.contains(e._1)) {
//          val upID =
//            previousWorkflowResultService.workflowCompiler.workflow
//              .getUpstream(e._1)
//              .head
//              .operatorID
//          if (cachedOperators.contains(upID) && !results.contains(upID)) {
//            results += ((upID, e._2))
//          }
//        } else {
//          results += (e)
//        }
      })
      sessionResults(session.getId) = workflowResultService
    }
    val availableResultEvent = WorkflowAvailableResultEvent(
      sessionResults(session.getId).operatorResults
        .map(e =>
          (e._2.operatorID, OperatorAvailableResult(operatorOutputCache.contains(e._1), e._2.webOutputMode))
        )
        .toMap
    )
    send(session, availableResultEvent)

    val eventListener = ControllerEventListener(
      workflowCompletedListener = completed => {
        sessionDownloadCache.remove(session.getId)
        send(session, WorkflowCompletedEvent())
        WorkflowWebsocketResource.sessionJobs.remove(session.getId)
      },
      workflowStatusUpdateListener = statusUpdate => {
        send(session, WebWorkflowStatusUpdateEvent.apply(statusUpdate))
      },
      workflowResultUpdateListener = resultUpdate => {
        workflowResultService.onResultUpdate(resultUpdate, session)
      },
      modifyLogicCompletedListener = _ => {
        send(session, ModifyLogicCompletedEvent())
      },
      breakpointTriggeredListener = breakpointTriggered => {
        send(session, BreakpointTriggeredEvent.apply(breakpointTriggered))
      },
      workflowPausedListener = _ => {
        send(session, WorkflowPausedEvent())
      },
      skipTupleResponseListener = _ => {
        send(session, SkipTupleResponseEvent())
      },
      reportCurrentTuplesListener = report => {
//        send(session, OperatorCurrentTuplesUpdateEvent.apply(report))
      },
      recoveryStartedListener = _ => {
        send(session, RecoveryStartedEvent())
      },
      workflowExecutionErrorListener = errorOccurred => {
        logger.error("Workflow execution has error: {}.", errorOccurred.error)
        send(session, WorkflowExecutionErrorEvent(errorOccurred.error.convertToMap()))
      }
    )

    val controllerActorRef = TexeraWebApplication.actorSystem.actorOf(
      Controller.props(workflowTag, workflow, eventListener, ControllerConfig.default)
    )
    texeraWorkflowCompiler.initializeBreakpoint(controllerActorRef)
    controllerActorRef ! ControlInvocation(AsyncRPCClient.IgnoreReply, StartWorkflow())

    WorkflowWebsocketResource.sessionJobs(session.getId) =
      (texeraWorkflowCompiler, controllerActorRef)

    send(session, WorkflowStartedEvent())

  }

  def downloadResult(session: Session, request: ResultDownloadRequest): Unit = {
    val resultDownloadResponse = ResultDownloadResource.apply(session.getId, request)
    send(session, resultDownloadResponse)
  }

  def killWorkflow(session: Session): Unit = {
    WorkflowWebsocketResource.sessionJobs(session.getId)._2 ! PoisonPill
    println("workflow killed")
  }

  @OnClose
  def myOnClose(session: Session, cr: CloseReason): Unit = {
    if (WorkflowWebsocketResource.sessionJobs.contains(session.getId)) {
      println(s"session ${session.getId} disconnected, kill its controller actor")
      this.killWorkflow(session)
    }

    sessionResults.remove(session.getId)
    sessionJobs.remove(session.getId)
    sessionMap.remove(session.getId)
    sessionDownloadCache.remove(session.getId)
  }

  def removeBreakpoint(session: Session, removeBreakpoint: RemoveBreakpointRequest): Unit = {
    throw new UnsupportedOperationException()
  }

}
