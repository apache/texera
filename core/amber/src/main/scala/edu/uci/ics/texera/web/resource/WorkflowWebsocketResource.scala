package edu.uci.ics.texera.web.resource

import akka.actor.{ActorRef, PoisonPill}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.EvaluatePythonExpressionHandler.EvaluatePythonExpression
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ModifyLogicHandler.ModifyLogic
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RetryWorkflowHandler.RetryWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{Controller, ControllerConfig}
import edu.uci.ics.amber.engine.common.{AmberClient, AmberUtils}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.web.model.common.CacheStatus
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.websocket.event.WorkflowAvailableResultEvent.OperatorAvailableResult
import edu.uci.ics.texera.web.model.websocket.event._
import edu.uci.ics.texera.web.model.websocket.event.python.PythonPrintTriggeredEvent
import edu.uci.ics.texera.web.model.websocket.request._
import edu.uci.ics.texera.web.model.websocket.request.python.PythonExpressionEvaluateRequest
import edu.uci.ics.texera.web.model.websocket.response.{HeartBeatResponse, HelloWorldResponse}
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource._
import edu.uci.ics.texera.web.service.WorkflowResultService
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
import edu.uci.ics.texera.web.model.websocket.response.python.PythonExpressionEvaluateResponse
import javax.websocket._
import javax.websocket.server.ServerEndpoint

import scala.collection.mutable
import scala.jdk.CollectionConverters.mapAsScalaMapConverter
import scala.util.{Failure, Success}

object WorkflowWebsocketResource {
  // TODO should reorganize this resource.

  val nextJobId = new AtomicInteger(0)

  // Map[sessionId, (Session, HttpSession)]
  val sessionMap = new mutable.HashMap[String, (Session, JwtContext)]

  // Map[sessionId, (WorkflowCompiler, ActorRef)]
  val sessionJobs = new mutable.HashMap[String, (WorkflowCompiler, AmberClient)]

  // Map[sessionId, Map[operatorId, List[ITuple]]]
  val sessionResults = new mutable.HashMap[String, WorkflowResultService]

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
class WorkflowWebsocketResource extends LazyLogging {

  final val objectMapper = Utils.objectMapper
  val sessionCachedOperators: mutable.HashMap[String, mutable.HashMap[String, OperatorDescriptor]] =
    mutable.HashMap[String, mutable.HashMap[String, OperatorDescriptor]]()
  val sessionCacheSourceOperators
      : mutable.HashMap[String, mutable.HashMap[String, CacheSourceOpDesc]] =
    mutable.HashMap[String, mutable.HashMap[String, CacheSourceOpDesc]]()
  val sessionCacheSinkOperators: mutable.HashMap[String, mutable.HashMap[String, CacheSinkOpDesc]] =
    mutable.HashMap[String, mutable.HashMap[String, CacheSinkOpDesc]]()
  val sessionOperatorRecord: mutable.HashMap[String, mutable.HashMap[String, WorkflowVertex]] =
    mutable.HashMap[String, mutable.HashMap[String, WorkflowVertex]]()
  val opResultStorageConfig: Config = ConfigFactory.load("application")
  val storageType: String = AmberUtils.amberConfig.getString("cache.storage").toLowerCase
  var opResultSwitch: Boolean = storageType != "off"
  var opResultStorage: OpResultStorage = storageType match {
    case "off" =>
      null
    case "memory" =>
      new MemoryOpResultStorage()
    case "jcs" =>
      new JCSOpResultStorage()
    case "mongodb" =>
      new MongoOpResultStorage()
    case _ =>
      throw new RuntimeException(s"invalid storage config $storageType")
  }
  if (opResultSwitch) {
    logger.info(s"Use $storageType for materialization")
  }

  @OnOpen
  def myOnOpen(session: Session): Unit = {
    logger.info("connection open")
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
        case execute: WorkflowExecuteRequest =>
          println(execute)
          executeWorkflow(session, execute)
        case newLogic: ModifyLogicRequest =>
          modifyLogic(session, newLogic)
        case pause: WorkflowPauseRequest =>
          pauseWorkflow(session)
        case resume: WorkflowResumeRequest =>
          resumeWorkflow(session)
        case kill: WorkflowKillRequest =>
          killWorkflow(session)
        case skipTupleMsg: SkipTupleRequest =>
          skipTuple(session, skipTupleMsg)
        case retryRequest: RetryRequest =>
          retryWorkflow(session)
        case breakpoint: AddBreakpointRequest =>
          addBreakpoint(session, breakpoint)
        case paginationRequest: ResultPaginationRequest =>
          resultPagination(session, paginationRequest)
        case resultExportRequest: ResultExportRequest =>
          exportResult(session, resultExportRequest)
        case cacheStatusUpdateRequest: CacheStatusUpdateRequest =>
          if (opResultSwitch) {
            updateCacheStatus(session, cacheStatusUpdateRequest)
          }
        case pythonExpressionEvaluateRequest: PythonExpressionEvaluateRequest =>
          evaluatePythonExpression(session, pythonExpressionEvaluateRequest)
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

  def evaluatePythonExpression(session: Session, request: PythonExpressionEvaluateRequest): Unit = {
    val client = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    client
      .sendAsync(EvaluatePythonExpression(request.expression, request.operatorId))
      .onSuccess { ret: PythonExpressionEvaluateResponse => send(session, ret) }
  }

  def resultPagination(session: Session, request: ResultPaginationRequest): Unit = {
    var operatorID = request.operatorID
    if (!sessionResults(session.getId).operatorResults.contains(operatorID)) {
      val downstreamIDs = sessionResults(session.getId).workflowCompiler.workflow
        .getDownstream(operatorID)
      // Get the first CacheSinkOpDesc, if exists
      downstreamIDs.find(_.isInstanceOf[CacheSinkOpDesc]).foreach { op =>
        operatorID = op.operatorID
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
    val client = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    compiler.addBreakpoint(client, addBreakpoint.operatorID, addBreakpoint.breakpoint)
  }

  def skipTuple(session: Session, tupleReq: SkipTupleRequest): Unit = {
//    val actorPath = tupleReq.actorPath
//    val faultedTuple = tupleReq.faultedTuple
//    val client = WorkflowWebsocketResource.sessionJobs(session.getId)._2
//    client ! SkipTupleGivenWorkerRef(actorPath, faultedTuple.toFaultedTuple())
    throw new RuntimeException("skipping tuple is temporarily disabled")
  }

  def modifyLogic(session: Session, newLogic: ModifyLogicRequest): Unit = {
    val texeraOperator = newLogic.operator
    val (compiler, client) = WorkflowWebsocketResource.sessionJobs(session.getId)
    compiler.initOperator(texeraOperator)
    client.fireAndForget(ModifyLogic(texeraOperator))
  }

  def retryWorkflow(session: Session): Unit = {
    val (_, client) = WorkflowWebsocketResource.sessionJobs(session.getId)
    client
      .sendAsync(RetryWorkflow())
      .onSuccess(_ => send(session, WorkflowResumedEvent()))
  }

  def pauseWorkflow(session: Session): Unit = {
    val client = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    client.sendAsync(PauseWorkflow()).onSuccess(x => send(session, WorkflowPausedEvent()))
  }

  def resumeWorkflow(session: Session): Unit = {
    val client = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    client
      .sendAsync(ResumeWorkflow())
      .onSuccess(_ => send(session, WorkflowResumedEvent()))
  }

  def executeWorkflow(session: Session, request: WorkflowExecuteRequest): Unit = {
    var cachedOperators: mutable.HashMap[String, OperatorDescriptor] = null
    var cacheSourceOperators: mutable.HashMap[String, CacheSourceOpDesc] = null
    var cacheSinkOperators: mutable.HashMap[String, CacheSinkOpDesc] = null
    var operatorRecord: mutable.HashMap[String, WorkflowVertex] = null
    if (opResultSwitch) {
      if (!sessionCachedOperators.contains(session.getId)) {
        cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
        sessionCachedOperators += ((session.getId, cachedOperators))
      } else {
        cachedOperators = sessionCachedOperators(session.getId)
      }
      if (!sessionCacheSourceOperators.contains(session.getId)) {
        cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
        sessionCacheSourceOperators += ((session.getId, cacheSourceOperators))
      } else {
        cacheSourceOperators = sessionCacheSourceOperators(session.getId)
      }
      if (!sessionCacheSinkOperators.contains(session.getId)) {
        cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDesc]()
        sessionCacheSinkOperators += ((session.getId, cacheSinkOperators))
      } else {
        cacheSinkOperators = sessionCacheSinkOperators(session.getId)
      }
      if (!sessionOperatorRecord.contains(session.getId)) {
        operatorRecord = mutable.HashMap[String, WorkflowVertex]()
        sessionOperatorRecord += ((session.getId, operatorRecord))
      } else {
        operatorRecord = sessionOperatorRecord(session.getId)
      }
    }

    logger.info(s"Session id: ${session.getId}")
    val context = new WorkflowContext
    val jobId = Integer.toString(WorkflowWebsocketResource.nextJobId.incrementAndGet)
    context.jobId = jobId
    context.userId = session.getUserProperties.asScala
      .get(classOf[User].getName)
      .map(_.asInstanceOf[User].getUid)

    if (opResultSwitch) {
      updateCacheStatus(
        session,
        CacheStatusUpdateRequest(
          request.operators,
          request.links,
          request.breakpoints,
          request.cachedOperatorIds
        )
      )
    }

    var workflowInfo = WorkflowInfo(request.operators, request.links, request.breakpoints)
    if (opResultSwitch) {
      workflowInfo.cachedOperatorIds = request.cachedOperatorIds
      logger.debug(s"Cached operators: $cachedOperators with ${request.cachedOperatorIds}")
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
      workflowInfo.cachedOperatorIds = oldWorkflowInfo.cachedOperatorIds
      logger.info(
        s"Rewrite the original workflow: ${toJgraphtDAG(oldWorkflowInfo)} to be: ${toJgraphtDAG(workflowInfo)}"
      )
    }
    val texeraWorkflowCompiler = new WorkflowCompiler(workflowInfo, context)
    val violations = texeraWorkflowCompiler.validate
    if (violations.nonEmpty) {
      send(session, WorkflowErrorEvent(violations))
      return
    }

    val workflow = texeraWorkflowCompiler.amberWorkflow(WorkflowIdentity(jobId))

    val workflowResultService = new WorkflowResultService(texeraWorkflowCompiler, opResultStorage)
    if (!sessionResults.contains(session.getId)) {
      sessionResults(session.getId) = workflowResultService
    } else if (opResultSwitch) {
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

    val client = TexeraWebApplication.createAmberRuntime(workflow, ControllerConfig.default)
    texeraWorkflowCompiler.initializeBreakpoint(client)

    client
      .getObservable[WorkflowCompleted]
      .subscribe(completed => {
        sessionExportCache.remove(session.getId)
        send(session, WorkflowCompletedEvent())
        if (opResultSwitch) {
          updateCacheStatus(
            session,
            CacheStatusUpdateRequest(
              request.operators,
              request.links,
              request.breakpoints,
              request.cachedOperatorIds
            )
          )
        }
      })

    client
      .getObservable[WorkflowStatusUpdate]
      .subscribe(statusUpdate => {
        send(session, WebWorkflowStatusUpdateEvent.apply(statusUpdate))
      })
    client
      .getObservable[WorkflowResultUpdate]
      .subscribe(resultUpdate => {
        workflowResultService.onResultUpdate(resultUpdate, session)
      })
    client
      .getObservable[BreakpointTriggered]
      .subscribe(breakpointTriggered => {
        send(session, BreakpointTriggeredEvent.apply(breakpointTriggered))
      })
    client
      .getObservable[PythonPrintTriggered]
      .subscribe(pythonPrintTriggered => {
        send(session, PythonPrintTriggeredEvent.apply(pythonPrintTriggered))
      })
    client
      .getObservable[ReportCurrentProcessingTuple]
      .subscribe(report => {
        //        send(session, OperatorCurrentTuplesUpdateEvent.apply(report))
      })
    client
      .getObservable[ErrorOccurred]
      .subscribe(errorOccurred => {
        logger.error("Workflow execution has error: {}.", errorOccurred.error)
        send(session, WorkflowExecutionErrorEvent(errorOccurred.error.getLocalizedMessage))
      })

    client.fireAndForget(StartWorkflow())

    WorkflowWebsocketResource.sessionJobs(session.getId) = (texeraWorkflowCompiler, client)

    send(session, WorkflowStartedEvent())

  }

  def updateCacheStatus(session: Session, request: CacheStatusUpdateRequest): Unit = {
    var cachedOperators: mutable.HashMap[String, OperatorDescriptor] = null
    if (!sessionCachedOperators.contains(session.getId)) {
      cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
    } else {
      cachedOperators = sessionCachedOperators(session.getId)
    }
    var cacheSourceOperators: mutable.HashMap[String, CacheSourceOpDesc] = null
    if (!sessionCacheSourceOperators.contains(session.getId)) {
      cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
    } else {
      cacheSourceOperators = sessionCacheSourceOperators(session.getId)
    }
    var cacheSinkOperators: mutable.HashMap[String, CacheSinkOpDesc] = null
    if (!sessionCacheSinkOperators.contains(session.getId)) {
      cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDesc]()
    } else {
      cacheSinkOperators = sessionCacheSinkOperators(session.getId)
    }
    var operatorRecord: mutable.HashMap[String, WorkflowVertex] = null
    if (!sessionOperatorRecord.contains(session.getId)) {
      operatorRecord = mutable.HashMap[String, WorkflowVertex]()
    } else {
      operatorRecord = sessionOperatorRecord(session.getId)
    }

    val workflowInfo = WorkflowInfo(request.operators, request.links, request.breakpoints)
    workflowInfo.cachedOperatorIds = request.cachedOperatorIds
    logger.debug(s"Cached operators: $cachedOperators with ${request.cachedOperatorIds}")
    val workflowRewriter = new WorkflowRewriter(
      workflowInfo,
      cachedOperators.clone(),
      cacheSourceOperators.clone(),
      cacheSinkOperators.clone(),
      operatorRecord.clone(),
      opResultStorage
    )

    val invalidSet = workflowRewriter.cacheStatusUpdate()

    val cacheStatusMap = request.cachedOperatorIds
      .filter(cachedOperators.contains)
      .map(id => {
        if (cachedOperators.contains(id)) {
          if (!invalidSet.contains(id)) {
            (id, CacheStatus.CACHE_VALID)
          } else {
            (id, CacheStatus.CACHE_INVALID)
          }
        } else {
          (id, CacheStatus.CACHE_INVALID)
        }
      })
      .toMap

    val cacheStatusUpdateEvent = CacheStatusUpdateEvent(cacheStatusMap)
    send(session, cacheStatusUpdateEvent)
  }

  def exportResult(session: Session, request: ResultExportRequest): Unit = {
    val resultExportResponse = ResultExportResource.apply(session, request)
    send(session, resultExportResponse)
  }

  def killWorkflow(session: Session): Unit = {
    WorkflowWebsocketResource.sessionJobs(session.getId)._2.shutdown()
    logger.info("workflow killed")
  }

  @OnClose
  def myOnClose(session: Session, cr: CloseReason): Unit = {
    if (WorkflowWebsocketResource.sessionJobs.contains(session.getId)) {
      logger.info(s"session ${session.getId} disconnected, kill its client actor")
      this.killWorkflow(session)
    }

    sessionResults.remove(session.getId)
    sessionJobs.remove(session.getId)
    sessionMap.remove(session.getId)
    sessionExportCache.remove(session.getId)
    if (opResultSwitch) {
      clearMaterialization(session)
    }
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

  def removeBreakpoint(session: Session, removeBreakpoint: RemoveBreakpointRequest): Unit = {
    throw new UnsupportedOperationException()
  }

}
