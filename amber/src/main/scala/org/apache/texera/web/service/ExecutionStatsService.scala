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

package org.apache.texera.web.service

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.storage.model.BufferedItemWriter
import org.apache.texera.amber.core.storage.result.ResultSchema
import org.apache.texera.amber.core.storage.{DocumentFactory, VFSURIFactory}
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.workflow.WorkflowContext
import org.apache.texera.amber.core.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import org.apache.texera.amber.core.workflowruntimestate.WorkflowFatalError
import org.apache.texera.amber.engine.architecture.coordinator.{
  ExecutionStateUpdate,
  ExecutionStatsUpdate,
  FatalError,
  RuntimeStatisticsPersist,
  WorkerAssignmentUpdate,
  WorkflowRecoveryStatus
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  FAILED,
  KILLED
}
import org.apache.texera.amber.engine.common.Utils
import org.apache.texera.amber.engine.architecture.worker.statistics.PortTupleMetricsMapping
import org.apache.texera.amber.engine.common.Utils.maptoStatusCode
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.amber.engine.common.executionruntimestate.{
  OperatorMetrics,
  OperatorWorkerMapping
}
import org.apache.texera.amber.error.ErrorUtils.{
  getOperatorFromActorIdOpt,
  getStackTraceWithAllCauses
}
import org.apache.texera.web.SubscriptionManager
import org.apache.texera.web.model.websocket.event.{
  ExecutionDurationUpdateEvent,
  OperatorAggregatedMetrics,
  OperatorStatisticsUpdateEvent,
  WorkerAssignmentUpdateEvent
}
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource
import org.apache.texera.web.storage.ExecutionStateStore
import org.apache.texera.web.storage.ExecutionStateStore.updateWorkflowState

import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, RejectedExecutionException, TimeUnit, TimeoutException}

class ExecutionStatsService(
    client: AmberClient,
    stateStore: ExecutionStateStore,
    workflowContext: WorkflowContext
) extends SubscriptionManager
    with LazyLogging {
  private val (metricsPersistThread, runtimeStatsWriter) = {
    val thread = Executors.newSingleThreadExecutor()
    val uri = VFSURIFactory.createRuntimeStatisticsURI(
      workflowContext.workflowId,
      workflowContext.executionId,
      warehouse = workflowContext.warehouse
    )
    val writer = DocumentFactory
      .createDocument(uri, ResultSchema.runtimeStatisticsSchema)
      .writer("runtime_statistics")
      .asInstanceOf[BufferedItemWriter[Tuple]]
    WorkflowExecutionsResource.updateRuntimeStatsUri(
      workflowContext.workflowId.id,
      workflowContext.executionId.id,
      uri
    )
    writer.open()
    (thread, writer)
  }

  private var lastPersistedMetrics: Map[String, OperatorMetrics] =
    Map.empty[String, OperatorMetrics]

  private val committed = new AtomicBoolean(false)
  private val CommitTimeoutSeconds = 60L

  registerCallbacks()

  addSubscription(
    stateStore.statsStore.registerDiffHandler((oldState, newState) => {
      // Update operator stats if any operator updates its stat
      if (newState.operatorInfo.toSet != oldState.operatorInfo.toSet) {
        Iterable(
          OperatorStatisticsUpdateEvent(newState.operatorInfo.map {
            case (opId, metrics) =>
              val (inCount, inSize, inMap) =
                summarizePortMetrics(metrics.operatorStatistics.inputMetrics)
              val (outCount, outSize, outMap) =
                summarizePortMetrics(metrics.operatorStatistics.outputMetrics)

              val res = OperatorAggregatedMetrics(
                Utils.aggregatedStateToString(metrics.operatorState),
                inCount,
                inSize,
                inMap,
                outCount,
                outSize,
                outMap,
                metrics.operatorStatistics.numWorkers,
                metrics.operatorStatistics.dataProcessingTime,
                metrics.operatorStatistics.controlProcessingTime,
                metrics.operatorStatistics.idleTime,
                reusedFromCache = metrics.reusedFromCache
              )
              (opId, res)
          })
        )
      } else {
        Iterable.empty
      }
    })
  )

  addSubscription(
    stateStore.statsStore.registerDiffHandler((oldState, newState) => {
      // update operators' workers.
      if (newState.operatorWorkerMapping != oldState.operatorWorkerMapping) {
        newState.operatorWorkerMapping
          .map { opToWorkers =>
            WorkerAssignmentUpdateEvent(opToWorkers.operatorId, opToWorkers.workerIds)
          }
      } else {
        Iterable()
      }
    })
  )

  addSubscription(
    stateStore.statsStore.registerDiffHandler((oldState, newState) => {
      // update execution duration.
      if (
        newState.startTimeStamp != oldState.startTimeStamp || newState.endTimeStamp != oldState.endTimeStamp
      ) {
        if (newState.endTimeStamp != 0) {
          Iterable(
            ExecutionDurationUpdateEvent(
              newState.endTimeStamp - newState.startTimeStamp,
              isRunning = false
            )
          )
        } else {
          val currentTime = System.currentTimeMillis()
          Iterable(
            ExecutionDurationUpdateEvent(currentTime - newState.startTimeStamp, isRunning = true)
          )
        }
      } else {
        Iterable()
      }
    })
  )

  private[this] def registerCallbacks(): Unit = {
    registerCallbackOnWorkflowStatsUpdate()
    registerCallbackOnWorkerAssignedUpdate()
    registerCallbackOnWorkflowRecoveryUpdate()
    registerCallbackOnFatalError()
  }

  private[this] def registerCallbackOnWorkflowStatsUpdate(): Unit = {
    // Register callback for UI updates (UI state store update only, no persistence)
    addSubscription(
      client
        .registerCallback[ExecutionStatsUpdate]((evt: ExecutionStatsUpdate) => {
          stateStore.statsStore.updateState { statsStore =>
            statsStore.withOperatorInfo(evt.operatorMetrics)
          }
        })
    )

    // Register callback for statistics persistence (persistence only, no UI update)
    addSubscription(
      client
        .registerCallback[RuntimeStatisticsPersist]((evt: RuntimeStatisticsPersist) => {
          try {
            metricsPersistThread.execute(() => {
              storeRuntimeStatistics(computeStatsDiff(evt.operatorMetrics))
              lastPersistedMetrics = evt.operatorMetrics
            })
          } catch {
            // The statistics are already committed and the writer is closed.
            case _: RejectedExecutionException =>
          }
        })
    )
  }

  addSubscription(
    client.registerCallback[ExecutionStateUpdate] {
      case ExecutionStateUpdate(state: WorkflowAggregatedState.Recognized)
          if state == COMPLETED || state == FAILED || state == KILLED =>
        commitAndStop()
      case _ =>
    }
  )

  /** A user kill or a fatal error tears the service down without a terminal state; commit here. */
  override def unsubscribeAll(): Unit = {
    super.unsubscribeAll()
    commitAndStop()
  }

  /**
    * Commits the runtime statistics and stops the persist thread, once. The engine reports the
    * final statistics right before the terminal state, and that report may still be queued on
    * the persist thread. Closing the writer there, behind it, commits it; closing on the calling
    * thread would flush before it and strand it in the buffer.
    */
  private def commitAndStop(): Unit = {
    if (committed.compareAndSet(false, true)) {
      logger.info("Workflow execution terminated. Commit runtime statistics.")
      try {
        // Bounded so a stuck write cannot hang the caller; shutdown still lets the close finish.
        metricsPersistThread
          .submit((() => runtimeStatsWriter.close()): Runnable)
          .get(CommitTimeoutSeconds, TimeUnit.SECONDS)
      } catch {
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          logger.error("Interrupted while closing runtime statistics writer", e)
        case _: TimeoutException =>
          logger.warn(
            s"Runtime statistics commit exceeded ${CommitTimeoutSeconds}s; continuing in background"
          )
        case e: Exception =>
          logger.error("Failed to close runtime statistics writer", e)
      } finally {
        metricsPersistThread.shutdown()
      }
    }
  }

  private def computeStatsDiff(
      newMetrics: Map[String, OperatorMetrics]
  ): Map[String, OperatorMetrics] = {
    val previousMetrics = lastPersistedMetrics
    val missingKeys = previousMetrics.keySet.diff(newMetrics.keySet)
    newMetrics ++ missingKeys.map(key => key -> previousMetrics(key))
  }

  private def summarizePortMetrics(
      portMetrics: Seq[PortTupleMetricsMapping]
  ): (Long, Long, Map[String, Long]) = {
    val perPortCounts = Map.newBuilder[String, Long]
    val (totalCount, totalSize) = aggregatePortMetrics(
      portMetrics,
      (portId, count) => perPortCounts += (portId -> count)
    )
    (totalCount, totalSize, perPortCounts.result())
  }

  private def aggregatePortMetrics(
      portMetrics: Seq[PortTupleMetricsMapping],
      onPort: (String, Long) => Unit = (_, _) => ()
  ): (Long, Long) = {
    var totalCount = 0L
    var totalSize = 0L
    portMetrics.foreach { pm =>
      val count = pm.tupleMetrics.count
      totalCount += count
      totalSize += pm.tupleMetrics.size
      onPort(pm.portId.id.toString, count)
    }
    (totalCount, totalSize)
  }

  private def storeRuntimeStatistics(
      operatorStatistics: scala.collection.immutable.Map[String, OperatorMetrics]
  ): Unit = {
    if (operatorStatistics.isEmpty) {
      return
    }
    try {
      val schema = ResultSchema.runtimeStatisticsSchema
      val timestamp = new java.sql.Timestamp(System.currentTimeMillis())
      operatorStatistics.foreach {
        case (operatorId, stat) =>
          val (inCount, inSize) = aggregatePortMetrics(stat.operatorStatistics.inputMetrics)
          val (outCount, outSize) = aggregatePortMetrics(stat.operatorStatistics.outputMetrics)
          val runtimeStats = new Tuple(
            schema,
            Array(
              operatorId,
              timestamp,
              inCount,
              inSize,
              outCount,
              outSize,
              stat.operatorStatistics.dataProcessingTime,
              stat.operatorStatistics.controlProcessingTime,
              stat.operatorStatistics.idleTime,
              stat.operatorStatistics.numWorkers,
              maptoStatusCode(stat.operatorState).toInt
            )
          )
          runtimeStatsWriter.putOne(runtimeStats)
      }
    } catch {
      case err: Throwable => logger.error("error occurred when storing runtime statistics", err)
    }
  }

  private[this] def registerCallbackOnWorkerAssignedUpdate(): Unit = {
    addSubscription(
      client
        .registerCallback[WorkerAssignmentUpdate]((evt: WorkerAssignmentUpdate) => {
          stateStore.statsStore.updateState { statsStore =>
            statsStore.withOperatorWorkerMapping(
              evt.workerMapping
                .map({
                  case (opId, workerIds) => OperatorWorkerMapping(opId, workerIds.toSeq)
                })
                .toSeq
            )
          }
        })
    )
  }

  private[this] def registerCallbackOnWorkflowRecoveryUpdate(): Unit = {
    addSubscription(
      client
        .registerCallback[WorkflowRecoveryStatus]((evt: WorkflowRecoveryStatus) => {
          stateStore.metadataStore.updateState { metadataStore =>
            metadataStore.withIsRecovering(evt.isRecovering)
          }
        })
    )
  }

  private[this] def registerCallbackOnFatalError(): Unit = {
    addSubscription(
      client
        .registerCallback[FatalError]((evt: FatalError) => {
          client.shutdown()
          val (operatorId, workerId) = getOperatorFromActorIdOpt(evt.fromActor)
          stateStore.statsStore.updateState(stats =>
            stats.withEndTimeStamp(System.currentTimeMillis())
          )
          stateStore.metadataStore.updateState { metadataStore =>
            logger.error("error occurred in execution", evt.e)
            updateWorkflowState(FAILED, metadataStore).addFatalErrors(
              WorkflowFatalError(
                EXECUTION_FAILURE,
                Timestamp(Instant.now),
                evt.e.toString,
                getStackTraceWithAllCauses(evt.e),
                operatorId,
                workerId
              )
            )
          }
        })
    )
  }
}
