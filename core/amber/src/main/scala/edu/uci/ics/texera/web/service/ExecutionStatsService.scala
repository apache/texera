package edu.uci.ics.texera.web.service

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.model.BufferedItemWriter
import edu.uci.ics.amber.core.storage.result.ResultSchema
import edu.uci.ics.amber.core.storage.{DocumentFactory, VFSURIFactory}
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.engine.architecture.controller.{
  ExecutionStatsUpdate,
  FatalError,
  WorkerAssignmentUpdate,
  WorkflowRecoveryStatus
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.FAILED
import edu.uci.ics.amber.engine.common.Utils.maptoStatusCode
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.executionruntimestate.{
  OperatorMetrics,
  OperatorStatistics,
  OperatorWorkerMapping
}
import edu.uci.ics.amber.engine.common.{AmberConfig, Utils}
import edu.uci.ics.amber.error.ErrorUtils.{getOperatorFromActorIdOpt, getStackTraceWithAllCauses}
import edu.uci.ics.amber.core.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import edu.uci.ics.amber.core.workflowruntimestate.WorkflowFatalError
import edu.uci.ics.texera.web.SubscriptionManager
import edu.uci.ics.texera.web.model.websocket.event.{
  ExecutionDurationUpdateEvent,
  OperatorAggregatedMetrics,
  OperatorStatisticsUpdateEvent,
  WorkerAssignmentUpdateEvent
}
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState
import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource

import java.time.Instant
import java.util.concurrent.Executors

class ExecutionStatsService(
    client: AmberClient,
    stateStore: ExecutionStateStore,
    workflowContext: WorkflowContext
) extends SubscriptionManager
    with LazyLogging {
  private val (metricsPersistThread, runtimeStatsWriter) = {
    if (AmberConfig.isUserSystemEnabled) {
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
      (Some(thread), Some(writer))
    } else {
      (None, None)
    }
  }

  private var lastPersistedMetrics: Option[Map[String, OperatorMetrics]] =
    Option.when(AmberConfig.isUserSystemEnabled)(Map.empty[String, OperatorMetrics])

  registerCallbacks()

  addSubscription(
    stateStore.statsStore.registerDiffHandler((oldState, newState) => {
      // Update operator stats if any operator updates its stat
      if (newState.operatorInfo.toSet != oldState.operatorInfo.toSet) {
        Iterable(
          OperatorStatisticsUpdateEvent(newState.operatorInfo.collect {
            case x =>
              val metrics = x._2
              val res = OperatorAggregatedMetrics(
                Utils.aggregatedStateToString(metrics.operatorState),
                metrics.operatorStatistics.inputMetrics.map(_.tupleMetrics.count).sum,
                metrics.operatorStatistics.inputMetrics.map(_.tupleMetrics.size).sum,
                metrics.operatorStatistics.outputMetrics.map(_.tupleMetrics.count).sum,
                metrics.operatorStatistics.outputMetrics.map(_.tupleMetrics.size).sum,
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
          metricsPersistThread.foreach { thread =>
            thread.execute(() => {
              storeRuntimeStatistics(computeStatsDiff(evt.operatorMetrics))
              lastPersistedMetrics = Some(evt.operatorMetrics)
            })
          }
        })
    )
  }

  private def computeStatsDiff(
      newMetrics: Map[String, OperatorMetrics]
  ): Map[String, OperatorMetrics] = {
    // Default metrics for new operators
    val defaultMetrics = OperatorMetrics(
      WorkflowAggregatedState.UNINITIALIZED,
      OperatorStatistics(Seq.empty, Seq.empty, 0, 0, 0, 0)
    )

    // Retrieve the last persisted metrics or default to an empty map
    val lastMetrics = lastPersistedMetrics.getOrElse(Map.empty)

    // Determine new and old keys
    val newKeys = newMetrics.keySet.diff(lastMetrics.keySet)
    val oldKeys = lastMetrics.keySet.diff(newMetrics.keySet)

    // Update last metrics with default metrics for new keys
    val updatedLastMetrics = lastMetrics ++ newKeys.map(_ -> defaultMetrics)

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
    runtimeStatsWriter match {
      case Some(writer) =>
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
              writer.putOne(runtimeStats)
          }
          writer.close()
        } catch {
          case err: Throwable => logger.error("error occurred when storing runtime statistics", err)
        }
      case None =>
        logger.warn("Runtime statistics writer is not available.")
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
