package edu.uci.ics.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.model.websocket.request.WorkflowExecuteRequest
import edu.uci.ics.texera.web.service.WorkflowService.mkWorkflowStateId
import edu.uci.ics.texera.web.storage.WorkflowStateStore
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.COMPLETED
import edu.uci.ics.texera.web.{SubscriptionManager, WorkflowLifecycleManager}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.LogicalPlan
import io.reactivex.rxjava3.disposables.{CompositeDisposable, Disposable}
import io.reactivex.rxjava3.subjects.BehaviorSubject
import org.jooq.types.UInteger
import play.api.libs.json.Json

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

object WorkflowService {
  private val workflowServiceMapping = new ConcurrentHashMap[String, WorkflowService]()
  val cleanUpDeadlineInSeconds: Int = AmberConfig.executionStateCleanUpInSecs

  def getAllWorkflowServices: Iterable[WorkflowService] = workflowServiceMapping.values().asScala

  def mkWorkflowStateId(workflowId: Long): String = {
    workflowId.toString
  }
  def getOrCreate(
      workflowId: Long,
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
                       val workflowId: Long,
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
    new ResultExportService(opResultStorage, UInteger.valueOf(workflowId))
  val lifeCycleManager: WorkflowLifecycleManager = new WorkflowLifecycleManager(
    s"wid=$workflowId",
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

  executionService.subscribe { job: WorkflowExecutionService =>
    {
      job.executionStateStore.metadataStore.registerDiffHandler { (oldState, newState) =>
        {
          if (oldState.state != COMPLETED && newState.state == COMPLETED) {
            lastCompletedLogicalPlan = Option.apply(job.workflow.originalLogicalPlan)
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

  def connectToJob(onNext: TexeraWebSocketEvent => Unit): Disposable = {
    var localDisposable = Disposable.empty()
    executionService.subscribe { job: WorkflowExecutionService =>
      localDisposable.dispose()
      val subscriptions = job.executionStateStore.getAllStores
        .map(_.getWebsocketEventObservable)
        .map(evtPub =>
          evtPub.subscribe { evts: Iterable[TexeraWebSocketEvent] => evts.foreach(onNext) }
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
    new WorkflowContext(uidOpt, UInteger.valueOf(workflowId))
  }

  def initJobService(req: WorkflowExecuteRequest, uidOpt: Option[UInteger]): Unit = {
    if (executionService.getValue != null) {
      //unsubscribe all
      executionService.getValue.unsubscribeAll()
    }
    val workflowContext: WorkflowContext = createWorkflowContext(uidOpt)

    workflowContext.executionId = ExecutionsMetadataPersistService.insertNewExecution(
      workflowContext.wid,
      workflowContext.userId,
      req.executionName,
      convertToJson(req.engineVersion)
    )

    val job = new WorkflowExecutionService(
      workflowContext,
      resultService,
      req,
      lastCompletedLogicalPlan
    )

    lifeCycleManager.registerCleanUpOnStateChange(job.executionStateStore)
    executionService.onNext(job)
    if (job.executionStateStore.metadataStore.getState.fatalErrors.isEmpty) {
      job.startWorkflow()
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
