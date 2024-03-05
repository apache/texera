package edu.uci.ics.texera.web.service

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.ControllerConfig
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  FaultToleranceConfig,
  StateRestoreConfig
}
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ChannelMarkerIdentity,
  ExecutionIdentity,
  WorkflowIdentity
}
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.model.websocket.request.WorkflowExecuteRequest
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource
import edu.uci.ics.texera.web.service.WorkflowService.mkWorkflowStateId
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState
import edu.uci.ics.texera.web.storage.{ExecutionStateStore, WorkflowStateStore}
import edu.uci.ics.texera.web.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{COMPLETED, FAILED}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowFatalError
import edu.uci.ics.texera.web.{SubscriptionManager, WorkflowLifecycleManager}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.LogicalPlan
import io.reactivex.rxjava3.disposables.{CompositeDisposable, Disposable}
import io.reactivex.rxjava3.subjects.BehaviorSubject
import org.jooq.types.UInteger
import play.api.libs.json.Json

import java.util.concurrent.ConcurrentHashMap
import java.net.URI
import java.time.Instant
import scala.jdk.CollectionConverters.IterableHasAsScala

object WorkflowService {
  private val workflowServiceMapping = new ConcurrentHashMap[String, WorkflowService]()
  val cleanUpDeadlineInSeconds: Int = AmberConfig.executionStateCleanUpInSecs

  def getAllWorkflowServices: Iterable[WorkflowService] = workflowServiceMapping.values().asScala

  def mkWorkflowStateId(workflowId: WorkflowIdentity): String = {
    workflowId.toString
  }
  def getOrCreate(
      workflowId: WorkflowIdentity,
      cleanupTimeout: Int = cleanUpDeadlineInSeconds
  ): WorkflowService = {
    workflowServiceMapping.compute(
      mkWorkflowStateId(workflowId),
      (_, v) => {
        if (v == null) {
          new WorkflowService(workflowId, cleanupTimeout)
        } else {
          v
        }
      }
    )
  }
}

class WorkflowService(
    val workflowId: WorkflowIdentity,
    cleanUpTimeout: Int
) extends SubscriptionManager
    with LazyLogging {
  // state across execution:
  var opResultStorage: OpResultStorage = new OpResultStorage()
  private val errorSubject = BehaviorSubject.create[TexeraWebSocketEvent]().toSerialized
  val stateStore = new WorkflowStateStore()
  var executionService: BehaviorSubject[WorkflowExecutionService] = BehaviorSubject.create()

  val resultService: ExecutionResultService =
    new ExecutionResultService(opResultStorage, stateStore)
  val exportService: ResultExportService =
    new ResultExportService(opResultStorage, UInteger.valueOf(workflowId.id))
  val lifeCycleManager: WorkflowLifecycleManager = new WorkflowLifecycleManager(
    s"workflowId=$workflowId",
    cleanUpTimeout,
    () => {
      opResultStorage.close()
      WorkflowService.workflowServiceMapping.remove(mkWorkflowStateId(workflowId))
      if (executionService.getValue != null) {
        // shutdown client
        executionService.getValue.client.shutdown()
      }
      unsubscribeAll()
    }
  )

  var lastCompletedLogicalPlan: Option[LogicalPlan] = Option.empty

  executionService.subscribe { executionService: WorkflowExecutionService =>
    {
      executionService.executionStateStore.metadataStore.registerDiffHandler {
        (oldState, newState) =>
          {
            if (oldState.state != COMPLETED && newState.state == COMPLETED) {
              lastCompletedLogicalPlan = Option.apply(executionService.workflow.originalLogicalPlan)
            }
            Iterable.empty
          }
      }
    }
  }

  def connect(onNext: TexeraWebSocketEvent => Unit): Disposable = {
    lifeCycleManager.increaseUserCount()
    val subscriptions = stateStore.getAllStores
      .map(_.getWebsocketEventObservable)
      .map(evtPub =>
        evtPub.subscribe { evts: Iterable[TexeraWebSocketEvent] => evts.foreach(onNext) }
      )
      .toSeq
    val errorSubscription = errorSubject.subscribe { evt: TexeraWebSocketEvent => onNext(evt) }
    new CompositeDisposable(subscriptions :+ errorSubscription: _*)
  }

  def connectToExecution(onNext: TexeraWebSocketEvent => Unit): Disposable = {
    var localDisposable = Disposable.empty()
    executionService.subscribe { executionService: WorkflowExecutionService =>
      localDisposable.dispose()
      val subscriptions = executionService.executionStateStore.getAllStores
        .map(_.getWebsocketEventObservable)
        .map(evtPub =>
          evtPub.subscribe { events: Iterable[TexeraWebSocketEvent] => events.foreach(onNext) }
        )
        .toSeq
      localDisposable = new CompositeDisposable(subscriptions: _*)
    }
  }

  def disconnect(): Unit = {
    lifeCycleManager.decreaseUserCount(
      Option(executionService.getValue).map(_.executionStateStore.metadataStore.getState.state)
    )
  }

  private[this] def createWorkflowContext(
      uidOpt: Option[UInteger]
  ): WorkflowContext = {
    new WorkflowContext(uidOpt, workflowId)
  }

  def initExecutionService(req: WorkflowExecuteRequest, uidOpt: Option[UInteger]): Unit = {
    if (executionService.getValue != null) {
      //unsubscribe all
      executionService.getValue.unsubscribeAll()
    }
    val workflowContext: WorkflowContext = createWorkflowContext(uidOpt)
    var controllerConf = ControllerConfig.default

    // fetch the workflow's environment eid
    val environmentEid =
      WorkflowResource.getEnvironmentEidOfWorkflow(UInteger.valueOf(workflowContext.workflowId.id))
    workflowContext.executionId = ExecutionsMetadataPersistService.insertNewExecution(
      workflowContext.workflowId,
      workflowContext.userId,
      req.executionName,
      convertToJson(req.engineVersion),
      environmentEid
    )

    if (AmberConfig.isUserSystemEnabled) {
      // enable only if we have mysql
      if (AmberConfig.faultToleranceLogRootFolder.isDefined) {
        val writeLocation = AmberConfig.faultToleranceLogRootFolder.get.resolve(
          s"${workflowContext.workflowId}/${workflowContext.executionId}"
        )
        ExecutionsMetadataPersistService.tryUpdateExistingExecution(workflowContext.executionId) {
          execution => execution.setLogLocation(writeLocation.toString)
        }
        controllerConf = controllerConf.copy(faultToleranceConfOpt =
          Some(FaultToleranceConfig(writeTo = writeLocation))
        )
      }
      if (req.replayFromExecution.isDefined) {
        val replayInfo = req.replayFromExecution.get
        ExecutionsMetadataPersistService
          .tryGetExistingExecution(ExecutionIdentity(replayInfo.eid))
          .foreach { execution =>
            val readLocation = new URI(execution.getLogLocation)
            controllerConf = controllerConf.copy(stateRestoreConfOpt =
              Some(
                StateRestoreConfig(
                  readFrom = readLocation,
                  replayDestination = ChannelMarkerIdentity(replayInfo.interaction)
                )
              )
            )
          }
      }
    }

    val executionStateStore = new ExecutionStateStore()
    val errorHandler: Throwable => Unit = { t =>
      {
        logger.error("error during execution", t)
        executionStateStore.statsStore.updateState(stats =>
          stats.withEndTimeStamp(System.currentTimeMillis())
        )
        executionStateStore.metadataStore.updateState { metadataStore =>
          updateWorkflowState(FAILED, metadataStore).addFatalErrors(
            WorkflowFatalError(
              EXECUTION_FAILURE,
              Timestamp(Instant.now),
              t.toString,
              t.getStackTrace.mkString("\n"),
              "unknown operator"
            )
          )
        }
      }
    }

    try {
      val execution = new WorkflowExecutionService(
        controllerConf,
        workflowContext,
        resultService,
        req,
        executionStateStore,
        errorHandler,
        lastCompletedLogicalPlan
      )
      lifeCycleManager.registerCleanUpOnStateChange(executionStateStore)
      executionService.onNext(execution)
      execution.startWorkflow()
    } catch {
      case e: Throwable => errorHandler(e)
    }

  }

  def convertToJson(frontendVersion: String): String = {
    val environmentVersionMap = Map(
      "engine_version" -> Json.toJson(frontendVersion)
    )
    Json.stringify(Json.toJson(environmentVersionMap))
  }

  override def unsubscribeAll(): Unit = {
    super.unsubscribeAll()
    Option(executionService.getValue).foreach(_.unsubscribeAll())
    resultService.unsubscribeAll()
  }

}
