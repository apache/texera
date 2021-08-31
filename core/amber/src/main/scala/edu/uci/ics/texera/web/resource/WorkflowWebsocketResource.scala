package edu.uci.ics.texera.web.resource

import akka.actor.{ActorRef, PoisonPill}
import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{
  Controller,
  ControllerConfig,
  ControllerEventListener
}
import edu.uci.ics.amber.engine.architecture.storage.OpResultStorage
import edu.uci.ics.amber.engine.architecture.storage.memory.JCSOpResultStorage
import edu.uci.ics.amber.engine.architecture.storage.mongo.MongoOpResultStorage
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.web.model.event._
import edu.uci.ics.texera.web.model.request._
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource._
import edu.uci.ics.texera.web.resource.auth.UserResource
import edu.uci.ics.texera.web.{ServletAwareConfigurator, TexeraWebApplication}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo.toJgraphtDAG
import edu.uci.ics.texera.workflow.common.workflow.{
  WorkflowCompiler,
  WorkflowInfo,
  WorkflowRewriter,
  WorkflowVertex
}
import edu.uci.ics.texera.workflow.operators.sink.CacheSinkOpDescV2
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDescV2

import java.util.concurrent.atomic.AtomicInteger
import javax.servlet.http.HttpSession
import javax.websocket._
import javax.websocket.server.ServerEndpoint
import scala.collection.{breakOut, mutable}

object WorkflowWebsocketResource {
  // TODO should reorganize this resource.

  val nextJobID = new AtomicInteger(0)

  // Map[sessionId, (Session, HttpSession)]
  val sessionMap = new mutable.HashMap[String, (Session, HttpSession)]

  // Map[sessionId, (WorkflowCompiler, ActorRef)]
  val sessionJobs = new mutable.HashMap[String, (WorkflowCompiler, ActorRef)]

  // Map[sessionId, Map[operatorId, List[ITuple]]]
  val sessionResults = new mutable.HashMap[String, WorkflowResultServiceV2]

  // Map[sessionId, Map[exportType, googleSheetLink]
  val sessionExportCache = new mutable.HashMap[String, mutable.HashMap[String, String]]

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
        case resultExportRequest: ResultExportRequest =>
          exportResult(session, resultExportRequest)
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

  def resultPagination(session: Session, request: ResultPaginationRequest): Unit = {
    var operatorID = request.operatorID
    if (!sessionResults(session.getId).operatorResults.contains(operatorID)) {
      val downstreamIDs = sessionResults(session.getId).workflowCompiler.workflow
        .getDownstream(operatorID)
      for (elem <- downstreamIDs) {
        if (elem.isInstanceOf[CacheSinkOpDescV2]) {
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

  def clearMaterialization(session: Session): Unit = {
    if (!sessionCacheSourceOperators.contains(session.getId)) {
      return
    }
    sessionCacheSinkOperators(session.getId).values.foreach(op => opResultStorage.remove(op.uuid))
    sessionCachedOperators.remove(session.getId)
    sessionCacheSourceOperators.remove(session.getId)
    sessionCacheSinkOperators.remove(session.getId)
    sessionOperatorRecord.remove(session.getId)
  }

  val sessionCachedOperators: mutable.HashMap[String, mutable.HashMap[String, OperatorDescriptor]] =
    mutable.HashMap[String, mutable.HashMap[String, OperatorDescriptor]]()
  val sessionCacheSourceOperators
      : mutable.HashMap[String, mutable.HashMap[String, CacheSourceOpDescV2]] =
    mutable.HashMap[String, mutable.HashMap[String, CacheSourceOpDescV2]]()
  val sessionCacheSinkOperators
      : mutable.HashMap[String, mutable.HashMap[String, CacheSinkOpDescV2]] =
    mutable.HashMap[String, mutable.HashMap[String, CacheSinkOpDescV2]]()
  val sessionOperatorRecord: mutable.HashMap[String, mutable.HashMap[String, WorkflowVertex]] =
    mutable.HashMap[String, mutable.HashMap[String, WorkflowVertex]]()

  var opResultStorage: OpResultStorage = _
  if (Constants.storage.equals("memory")) {
    opResultStorage = new JCSOpResultStorage()
    logger.info("use memory storage for materialization")
  } else if (Constants.storage.equals("mongodb")) {
    logger.info("use mongodb for materialization")
    opResultStorage = new MongoOpResultStorage()
  } else {
    logger.info("invalid storage config")
    System.exit(-1)
  }

  def executeWorkflow(session: Session, request: ExecuteWorkflowRequest): Unit = {
    var cachedOperators: mutable.HashMap[String, OperatorDescriptor] = null
    if (!sessionCachedOperators.contains(session.getId)) {
      cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
      sessionCachedOperators += ((session.getId, cachedOperators))
    } else {
      cachedOperators = sessionCachedOperators(session.getId)
    }
    var cacheSourceOperators: mutable.HashMap[String, CacheSourceOpDescV2] = null
    if (!sessionCacheSourceOperators.contains(session.getId)) {
      cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDescV2]()
      sessionCacheSourceOperators += ((session.getId, cacheSourceOperators))
    } else {
      cacheSourceOperators = sessionCacheSourceOperators(session.getId)
    }
    var cacheSinkOperators: mutable.HashMap[String, CacheSinkOpDescV2] = null
    if (!sessionCacheSinkOperators.contains(session.getId)) {
      cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDescV2]()
      sessionCacheSinkOperators += ((session.getId, cacheSinkOperators))
    } else {
      cacheSinkOperators = sessionCacheSinkOperators(session.getId)
    }
    var operatorRecord: mutable.HashMap[String, WorkflowVertex] = null
    if (!sessionOperatorRecord.contains(session.getId)) {
      operatorRecord = mutable.HashMap[String, WorkflowVertex]()
      sessionOperatorRecord += ((session.getId, operatorRecord))
    } else {
      operatorRecord = sessionOperatorRecord(session.getId)
    }

    logger.info("Session id: {}", session.getId)
    val context = new WorkflowContext
    val jobID = Integer.toString(WorkflowWebsocketResource.nextJobID.incrementAndGet)
    context.jobID = jobID
    context.userID = UserResource
      .getUser(sessionMap(session.getId)._2)
      .map(u => u.getUid)

    val cacheStatusMap = request.cachedOperatorIDs
      .map(id => {
        if (cachedOperators.contains(id)) {
          (id, CacheStatus.CACHE_VALID)
        } else {
          (id, CacheStatus.CACHE_INVALID)
        }
      })
      .toMap
    val cacheStatusUpdateEvent = CacheStatusUpdateEvent(cacheStatusMap)
    send(session, cacheStatusUpdateEvent)

    var workflowInfo = WorkflowInfo(request.operators, request.links, request.breakpoints)
    workflowInfo.cachedOperatorIDs = request.cachedOperatorIDs
    logger.info("Cached operators: {}.", cachedOperators.toString())
    logger.info("request.cachedOperatorIDs: {}.", request.cachedOperatorIDs)
    val workflowRewriter = new WorkflowRewriter(
      workflowInfo,
      cachedOperators,
      cacheSourceOperators,
      cacheSinkOperators,
      operatorRecord,
      opResultStorage
    )
    val newWorkflowInfo = workflowRewriter.rewrite
    val oldWorkflowInfo = workflowInfo
    workflowInfo = newWorkflowInfo
    logger.info("Original workflow: {}.", toJgraphtDAG(oldWorkflowInfo).toString)
    logger.info("Rewritten workflow: {}.", toJgraphtDAG(workflowInfo).toString)
    val texeraWorkflowCompiler = new WorkflowCompiler(workflowInfo, context)
    logger.info("TexeraWorkflowCompiler constructed: {}.", texeraWorkflowCompiler)
    val violations = texeraWorkflowCompiler.validate
    if (violations.nonEmpty) {
      send(session, WorkflowErrorEvent(violations))
      return
    }

    val workflow = texeraWorkflowCompiler.amberWorkflow(WorkflowIdentity(jobID))

    val workflowResultService = new WorkflowResultServiceV2(texeraWorkflowCompiler, opResultStorage)
    if (!sessionResults.contains(session.getId)) {
      sessionResults(session.getId) = workflowResultService
    } else {
      val previousWorkflowResultServiceV2 = sessionResults(session.getId)
      val previousResults = previousWorkflowResultServiceV2.operatorResults
      val results = workflowResultService.operatorResults
      results.foreach(e => {
        if (previousResults.contains(e._2.operatorID)) {
          previousResults(e._2.operatorID) = e._2
        }
      })
      previousResults.foreach(e => {
        if (cachedOperators.contains(e._2.operatorID) && !results.contains(e._2.operatorID)) {
          results += ((e._2.operatorID, e._2))
        }
      })
      sessionResults(session.getId) = workflowResultService
    }

    val cachedIDs = mutable.HashSet[String]()
    val cachedIDMap = mutable.HashMap[String, String]()
    sessionResults(session.getId).operatorResults.foreach(e =>
      cachedIDMap += ((e._2.operatorID, e._1))
    )

    val availableResultEvent = WorkflowAvailableResultEvent(
      request.operators
        .filter(op => cachedIDMap.contains(op.operatorID))
        .map(op => op.operatorID)
        .map(id => {
          (
            id,
            OperatorAvailableResult(
              cachedIDs.contains(id),
              sessionResults(session.getId).operatorResults(cachedIDMap(id)).webOutputMode
            )
          )
        })
        .toMap
    )

    send(session, availableResultEvent)

    val eventListener = ControllerEventListener(
      workflowCompletedListener = completed => {
        sessionExportCache.remove(session.getId)
        send(session, WorkflowCompletedEvent())
        WorkflowWebsocketResource.sessionJobs.remove(session.getId)
        val cacheStatusMap = request.cachedOperatorIDs
          .map(id => {
            if (cachedOperators.contains(id)) {
              (id, CacheStatus.CACHE_VALID)
            } else {
              (id, CacheStatus.CACHE_INVALID)
            }
          })
          .toMap
        val cacheStatusUpdateEvent = CacheStatusUpdateEvent(cacheStatusMap)
        send(session, cacheStatusUpdateEvent)
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
      pythonPrintTriggeredListener = pythonPrintTriggered => {
        send(session, PythonPrintTriggeredEvent.apply(pythonPrintTriggered))
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
        send(session, WorkflowExecutionErrorEvent(errorOccurred.error.getLocalizedMessage))
      }
    )

    val controllerActorRef = TexeraWebApplication.actorSystem.actorOf(
      Controller.props(workflow, eventListener, ControllerConfig.default)
    )
    texeraWorkflowCompiler.initializeBreakpoint(controllerActorRef)
    controllerActorRef ! ControlInvocation(AsyncRPCClient.IgnoreReply, StartWorkflow())

    WorkflowWebsocketResource.sessionJobs(session.getId) =
      (texeraWorkflowCompiler, controllerActorRef)

    send(session, WorkflowStartedEvent())

  }

  def exportResult(session: Session, request: ResultExportRequest): Unit = {
    val resultExportResponse = ResultExportResource.apply(session.getId, request)
    send(session, resultExportResponse)
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
    clearMaterialization(session)
    sessionExportCache.remove(session.getId)
  }

  def removeBreakpoint(session: Session, removeBreakpoint: RemoveBreakpointRequest): Unit = {
    throw new UnsupportedOperationException()
  }

}
