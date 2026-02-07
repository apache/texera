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
import org.apache.texera.amber.engine.architecture.controller._
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  FAILED,
  KILLED
}
import org.apache.texera.amber.engine.common.Utils
import org.apache.texera.amber.engine.common.Utils.maptoStatusCode
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.amber.engine.common.executionruntimestate.{
  OperatorMetrics,
  OperatorStatistics,
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
import java.util.concurrent.Executors

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
      workflowContext.executionId
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

  registerCallbacks()

  addSubscription(
    stateStore.statsStore.registerDiffHandler((oldState, newState) => {
      // Update operator stats if any operator updates its stat
      if (newState.operatorInfo.toSet != oldState.operatorInfo.toSet) {
        Iterable(
          OperatorStatisticsUpdateEvent(newState.operatorInfo.collect {
            case x =>
              val metrics = x._2
              val inMap = metrics.operatorStatistics.inputMetrics
                .map(pm => pm.portId.id.toString -> pm.tupleMetrics.count)
                .toMap
              val outMap = metrics.operatorStatistics.outputMetrics
                .map(pm => pm.portId.id.toString -> pm.tupleMetrics.count)
                .toMap

              val res = OperatorAggregatedMetrics(
                Utils.aggregatedStateToString(metrics.operatorState),
                metrics.operatorStatistics.inputMetrics.map(_.tupleMetrics.count).sum,
                metrics.operatorStatistics.inputMetrics.map(_.tupleMetrics.size).sum,
                inMap,
                metrics.operatorStatistics.outputMetrics.map(_.tupleMetrics.count).sum,
                metrics.operatorStatistics.outputMetrics.map(_.tupleMetrics.size).sum,
                outMap,
                metrics.operatorStatistics.numWorkers,
                metrics.operatorStatistics.dataProcessingTime,
                metrics.operatorStatistics.controlProcessingTime,
                metrics.operatorStatistics.idleTime
              )
              (x._1, res)
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
    addSubscription(
      client
        .registerCallback[ExecutionStatsUpdate]((evt: ExecutionStatsUpdate) => {
          stateStore.statsStore.updateState { statsStore =>
            statsStore.withOperatorInfo(evt.operatorMetrics)
          }
          metricsPersistThread.execute(() => {
            storeRuntimeStatistics(computeStatsDiff(evt.operatorMetrics))
            lastPersistedMetrics = evt.operatorMetrics
          })
        })
    )
  }

  addSubscription(
    client.registerCallback[ExecutionStateUpdate] {
      case ExecutionStateUpdate(state: WorkflowAggregatedState.Recognized)
          if Set(COMPLETED, FAILED, KILLED).contains(state) =>
        logger.info("Workflow execution terminated. Commit runtime statistics.")
        try {
          runtimeStatsWriter.close()
        } catch {
          case e: Exception =>
            logger.error("Failed to close runtime statistics writer", e)
        }
      case _ =>
    }
  )

  private def computeStatsDiff(
      newMetrics: Map[String, OperatorMetrics]
  ): Map[String, OperatorMetrics] = {
    // Default metrics for new operators
    val defaultMetrics = OperatorMetrics(
      WorkflowAggregatedState.UNINITIALIZED,
      OperatorStatistics(Seq.empty, Seq.empty, 0, 0, 0, 0)
    )

    // Determine new and old keys
    val newKeys = newMetrics.keySet.diff(lastPersistedMetrics.keySet)
    val oldKeys = lastPersistedMetrics.keySet.diff(newMetrics.keySet)

    // Update last metrics with default metrics for new keys
    val updatedLastMetrics = lastPersistedMetrics ++ newKeys.map(_ -> defaultMetrics)

    // Combine new metrics with old metrics for keys that are no longer present
    val completeMetricsMap = newMetrics ++ oldKeys.map(key => key -> updatedLastMetrics(key))

    // Transform the complete metrics map to ensure consistent structure
    completeMetricsMap.map {
      case (key, metrics) =>
        key -> OperatorMetrics(
          metrics.operatorState,
          OperatorStatistics(
            metrics.operatorStatistics.inputMetrics,
            metrics.operatorStatistics.outputMetrics,
            metrics.operatorStatistics.numWorkers,
            metrics.operatorStatistics.dataProcessingTime,
            metrics.operatorStatistics.controlProcessingTime,
            metrics.operatorStatistics.idleTime
          )
        )
    }
  }

  private def storeRuntimeStatistics(
      operatorStatistics: scala.collection.immutable.Map[String, OperatorMetrics]
  ): Unit = {
    try {
      operatorStatistics.foreach {
        case (operatorId, stat) =>
          val runtimeStats = new Tuple(
            ResultSchema.runtimeStatisticsSchema,
            Array(
              operatorId,
              new java.sql.Timestamp(System.currentTimeMillis()),
              stat.operatorStatistics.inputMetrics.map(_.tupleMetrics.count).sum,
              stat.operatorStatistics.inputMetrics.map(_.tupleMetrics.size).sum,
              stat.operatorStatistics.outputMetrics.map(_.tupleMetrics.count).sum,
              stat.operatorStatistics.outputMetrics.map(_.tupleMetrics.size).sum,
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
