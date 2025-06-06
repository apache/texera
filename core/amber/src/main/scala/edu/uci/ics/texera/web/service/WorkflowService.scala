/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.texera.web.service

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.WorkflowRuntimeException
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.amber.engine.architecture.controller.ControllerConfig
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  FAILED
}
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  FaultToleranceConfig,
  StateRestoreConfig
}
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.error.ErrorUtils.{getOperatorFromActorIdOpt, getStackTraceWithAllCauses}
import edu.uci.ics.amber.core.virtualidentity.{
  ChannelMarkerIdentity,
  ExecutionIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.core.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import edu.uci.ics.amber.core.workflowruntimestate.WorkflowFatalError
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.model.websocket.request.WorkflowExecuteRequest
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource
import edu.uci.ics.texera.web.service.WorkflowService.mkWorkflowStateId
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState
import edu.uci.ics.texera.web.storage.{ExecutionStateStore, WorkflowStateStore}
import edu.uci.ics.texera.web.{SubscriptionManager, WorkflowLifecycleManager}
import edu.uci.ics.texera.workflow.LogicalPlan
import io.reactivex.rxjava3.disposables.{CompositeDisposable, Disposable}
import io.reactivex.rxjava3.subjects.BehaviorSubject
import play.api.libs.json.Json

import java.net.URI
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.IterableHasAsScala

import edu.uci.ics.amber.core.storage.result.iceberg.OnIceberg

object WorkflowService {
  private val workflowServiceMapping = new ConcurrentHashMap[String, WorkflowService]()
  val cleanUpDeadlineInSeconds: Int = AmberConfig.executionStateCleanUpInSecs

  def getAllWorkflowServices: Iterable[WorkflowService] = workflowServiceMapping.values().asScala

  def mkWorkflowStateId(workflowId: WorkflowIdentity): String = {
    workflowId.toString
  }

  def getOrCreate(
      workflowId: WorkflowIdentity,
      computingUnitId: Int,
      cleanupTimeout: Int = cleanUpDeadlineInSeconds
  ): WorkflowService = {
    workflowServiceMapping.compute(
      mkWorkflowStateId(workflowId),
      (_, v) => {
        if (v == null) {
          new WorkflowService(workflowId, computingUnitId, cleanupTimeout)
        } else {
          v
        }
      }
    )
  }
}

class WorkflowService(
    val workflowId: WorkflowIdentity,
    val computingUnitId: Int,
    cleanUpTimeout: Int
) extends SubscriptionManager
    with LazyLogging {

  // state across execution:
  private val errorSubject = BehaviorSubject.create[TexeraWebSocketEvent]().toSerialized
  val stateStore = new WorkflowStateStore()
  var executionService: BehaviorSubject[WorkflowExecutionService] = BehaviorSubject.create()

  val resultService: ExecutionResultService =
    new ExecutionResultService(workflowId, computingUnitId, stateStore)
  val lifeCycleManager: WorkflowLifecycleManager = new WorkflowLifecycleManager(
    s"workflowId=$workflowId",
    cleanUpTimeout,
    () => {
      // clear the storage resources associated with the latest execution
      WorkflowExecutionService
        .getLatestExecutionId(workflowId, computingUnitId)
        .foreach(eid => {
          clearExecutionResources(eid)
        })
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
              lastCompletedLogicalPlan = Option.apply(executionService.workflow.logicalPlan)
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
    val localDisposable = new CompositeDisposable()
    val disposable = executionService.subscribe { execService: WorkflowExecutionService =>
      localDisposable.clear() // Clears previous subscriptions safely
      val subscriptions = execService.executionStateStore.getAllStores
        .map(_.getWebsocketEventObservable)
        .map(evtPub =>
          evtPub.subscribe { events: Iterable[TexeraWebSocketEvent] => events.foreach(onNext) }
        )
        .toSeq
      localDisposable.addAll(subscriptions: _*)
    }
    // Note: this new CompositeDisposable is necessary. DO NOT OPTIMIZE.
    new CompositeDisposable(localDisposable, disposable)
  }

  def disconnect(): Unit = {
    lifeCycleManager.decreaseUserCount(
      Option(executionService.getValue).map(_.executionStateStore.metadataStore.getState.state)
    )
  }

  private[this] def createWorkflowContext(): WorkflowContext = {
    new WorkflowContext(workflowId)
  }

  def initExecutionService(
      req: WorkflowExecuteRequest,
      userOpt: Option[User],
      sessionUri: URI
  ): Unit = {

    if (executionService.hasValue) {
      executionService.getValue.unsubscribeAll()
    }

    val (uidOpt, userEmailOpt) = userOpt.map(user => (user.getUid, user.getEmail)).unzip

    val workflowContext: WorkflowContext = createWorkflowContext()
    var controllerConf = ControllerConfig.default

    // clean up results from previous run
    val previousExecutionId =
      WorkflowExecutionService.getLatestExecutionId(workflowId, req.computingUnitId)
    previousExecutionId.foreach(eid => {
      clearExecutionResources(eid)
    }) // TODO: change this behavior after enabling cache.

    workflowContext.executionId = ExecutionsMetadataPersistService.insertNewExecution(
      workflowContext.workflowId,
      uidOpt,
      req.executionName,
      convertToJson(req.engineVersion),
      req.computingUnitId
    )

    if (AmberConfig.isUserSystemEnabled) {
      // enable only if we have mysql
      if (AmberConfig.faultToleranceLogRootFolder.isDefined) {
        val writeLocation = AmberConfig.faultToleranceLogRootFolder.get.resolve(
          s"${workflowContext.workflowId}/${workflowContext.executionId}/"
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
    // assign execution id to find the execution from DB in case the constructor fails.
    executionStateStore.metadataStore.updateState(state =>
      state.withExecutionId(workflowContext.executionId)
    )
    val errorHandler: Throwable => Unit = { t =>
      {
        val fromActorOpt = t match {
          case ex: WorkflowRuntimeException =>
            ex.relatedWorkerId
          case other =>
            None
        }
        val (operatorId, workerId) = getOperatorFromActorIdOpt(fromActorOpt)
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
              getStackTraceWithAllCauses(t),
              operatorId,
              workerId
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
        userEmailOpt,
        sessionUri
      )
      lifeCycleManager.registerCleanUpOnStateChange(executionStateStore)
      executionService.onNext(execution)
      execution.executeWorkflow()
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

  /**
    * Cleans up all resources associated with a workflow execution.
    *
    * This method performs resource cleanup in the following sequence:
    *  1. Retrieves all document URIs associated with the execution
    *  2. Clears URI references from the execution registry
    *  3. Safely clears all result and console message documents
    *  4. Expires Iceberg snapshots for runtime statistics
    *
    * @param eid The execution identity to clean up resources for
    */
  private def clearExecutionResources(eid: ExecutionIdentity): Unit = {
    // Retrieve URIs for all resources associated with this execution
    val resultUris = WorkflowExecutionsResource.getResultUrisByExecutionId(eid)
    val consoleMessagesUris = WorkflowExecutionsResource.getConsoleMessagesUriByExecutionId(eid)

    // Remove references from registry first
    WorkflowExecutionsResource.deleteConsoleMessageAndExecutionResultUris(eid)

    // Clean up all result and console message documents
    (resultUris ++ consoleMessagesUris).foreach { uri =>
      try DocumentFactory.openDocument(uri)._1.clear()
      catch {
        case error: Throwable =>
          logger.debug(s"Error processing document at $uri: ${error.getMessage}")
      }
    }

    // Expire any Iceberg snapshots for runtime statistics
    WorkflowExecutionsResource.getRuntimeStatsUriByExecutionId(eid).foreach { uri =>
      try {
        DocumentFactory.openDocument(uri)._1 match {
          case iceberg: OnIceberg => iceberg.expireSnapshots()
          case other =>
            logger.error(
              s"Cannot expire snapshots: document from URI [$uri] is of type ${other.getClass.getName}. " +
                s"Expected an instance of ${classOf[OnIceberg].getName}."
            )
        }
      } catch {
        case error: Throwable =>
          logger.debug(s"Error processing document at $uri: ${error.getMessage}")
      }
    }
  }

}
