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
import org.apache.texera.amber.core.tuple.{AttributeType, Tuple}
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
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
import org.apache.texera.amber.engine.architecture.coordinator.execution.ExecutionUtils.aggregateStatistics
import org.apache.texera.amber.engine.architecture.worker.statistics.{
  PortTupleMetricsMapping,
  TupleMetrics
}
import org.apache.texera.common.compiler.{MacroExpansion, MacroResultTarget}
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
import java.util.concurrent.Executors

object ExecutionStatsService {
  val metricsColumn = "operatorMetrics"

  private[service] def canvasStatistics(
      expansion: Option[MacroExpansion],
      native: Map[String, OperatorMetrics]
  ) = {
    val boundaries = expansion.iterator.flatMap(_.boundaries).flatMap {
      case (id, boundary) =>
        val owned = boundary.members
        val members =
          if (owned.nonEmpty) owned
          else
            (boundary.outputs.flatten ++ (if (boundary.terminal) boundary.inputs.flatten else Nil))
              .map(_.operatorID)
              .distinct
        val updates = members.flatMap(native.get)
        Option.when(updates.nonEmpty) {
          def ports(bindings: List[List[MacroResultTarget]], input: Boolean) =
            bindings.zipWithIndex.map {
              case (targets, ordinal) =>
                // A broadcast sends the same stream to several consumers; use the furthest progress.
                val tuples = targets
                  .flatMap { target =>
                    native.get(target.operatorID).flatMap { m =>
                      val stats = m.operatorStatistics
                      (if (input) stats.inputMetrics else stats.outputMetrics)
                        .find(_.portId == PortIdentity(target.portIndex))
                        .map(_.tupleMetrics)
                    }
                  }
                  .maxByOption(m => (m.count, m.size))
                  .getOrElse(TupleMetrics.defaultInstance)
                PortTupleMetricsMapping(PortIdentity(ordinal), tuples)
            }
          val states = updates.map(_.operatorState)
          import WorkflowAggregatedState._
          val complete = updates.size == members.size
          val state =
            if (complete && states.forall(s => s == COMPLETED || s == TERMINATED)) COMPLETED
            else
              List(FAILED, KILLED, PAUSING, PAUSED, RESUMING, RUNNING, READY)
                .find(states.contains)
                .getOrElse(UNINITIALIZED)
          id -> OperatorMetrics(
            state,
            aggregateStatistics(
              if (owned.nonEmpty) updates else Nil,
              ports(boundary.inputs, !boundary.terminal),
              ports(boundary.outputs, input = false)
            ),
            complete && updates.forall(_.reusedFromCache)
          )
        }
    }
    (native ++ boundaries).view.mapValues(OperatorAggregatedMetrics.from).toMap
  }

  def restore(
      expansion: MacroExpansion,
      rows: Iterator[Tuple]
  ): Map[String, OperatorAggregatedMetrics] = {
    val latest = scala.collection.mutable.Map.empty[String, Tuple]
    rows.foreach { row =>
      val id = row.getField[String]("operatorId")
      if (
        latest
          .get(id)
          .forall(r =>
            !r.getField[java.sql.Timestamp]("time").after(row.getField[java.sql.Timestamp]("time"))
          )
      )
        latest(id) = row
    }
    canvasStatistics(
      Some(expansion),
      latest.view
        .mapValues(row => OperatorMetrics.parseFrom(row.getField[Array[Byte]](metricsColumn)))
        .toMap
    )
  }
}

class ExecutionStatsService(
    client: AmberClient,
    stateStore: ExecutionStateStore,
    workflowContext: WorkflowContext,
    expansion: Option[MacroExpansion] = None
) extends SubscriptionManager
    with LazyLogging {
  private val schema =
    if (expansion.nonEmpty)
      ResultSchema.runtimeStatisticsSchema.add(
        ExecutionStatsService.metricsColumn,
        AttributeType.BINARY
      )
    else ResultSchema.runtimeStatisticsSchema
  private val (metricsPersistThread, runtimeStatsWriter) = {
    val thread = Executors.newSingleThreadExecutor()
    val uri = VFSURIFactory.createRuntimeStatisticsURI(
      workflowContext.workflowId,
      workflowContext.executionId,
      warehouse = workflowContext.warehouse
    )
    val writer = DocumentFactory
      .createDocument(uri, schema)
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

  private var lastSampleTime = 0L
  private var lastPersistedMetrics: Map[String, OperatorMetrics] =
    Map.empty[String, OperatorMetrics]

  registerCallbacks()

  addSubscription(
    stateStore.statsStore.registerDiffHandler((oldState, newState) => {
      // Update operator stats if any operator updates its stat
      if (newState.operatorInfo.toSet != oldState.operatorInfo.toSet) {
        Iterable(
          OperatorStatisticsUpdateEvent(
            ExecutionStatsService.canvasStatistics(expansion, newState.operatorInfo)
          )
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
            statsStore.withOperatorInfo(statsStore.operatorInfo ++ evt.operatorMetrics)
          }
        })
    )

    // Register callback for statistics persistence (persistence only, no UI update)
    addSubscription(
      client
        .registerCallback[RuntimeStatisticsPersist]((evt: RuntimeStatisticsPersist) => {
          metricsPersistThread.execute(() => {
            lastPersistedMetrics ++= evt.operatorMetrics
            storeRuntimeStatistics(lastPersistedMetrics)
          })
        })
    )
  }

  addSubscription(
    client.registerCallback[ExecutionStateUpdate] {
      case ExecutionStateUpdate(state: WorkflowAggregatedState.Recognized)
          if state == COMPLETED || state == FAILED || state == KILLED =>
        logger.info("Workflow execution terminated. Commit runtime statistics.")
        try {
          metricsPersistThread.submit((() => runtimeStatsWriter.close()): Runnable).get()
        } catch {
          case e: Exception =>
            logger.error("Failed to close runtime statistics writer", e)
        }
      case _ =>
    }
  )

  private def aggregatePortMetrics(
      portMetrics: Seq[PortTupleMetricsMapping]
  ): (Long, Long) = {
    var totalCount = 0L
    var totalSize = 0L
    portMetrics.foreach { pm =>
      val count = pm.tupleMetrics.count
      totalCount += count
      totalSize += pm.tupleMetrics.size
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
      lastSampleTime = math.max(System.currentTimeMillis(), lastSampleTime + 1)
      val timestamp = new java.sql.Timestamp(lastSampleTime)
      operatorStatistics.foreach {
        case (operatorId, stat) =>
          val (inCount, inSize) = aggregatePortMetrics(stat.operatorStatistics.inputMetrics)
          val (outCount, outSize) = aggregatePortMetrics(stat.operatorStatistics.outputMetrics)
          val runtimeStats = new Tuple(
            schema,
            Array[Any](
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
            ) ++ expansion.map(_ => stat.toByteArray)
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
