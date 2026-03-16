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

package org.apache.texera.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import org.apache.texera.amber.config.ApplicationConfig
import org.apache.texera.amber.core.virtualidentity.PhysicalOpIdentity
import org.apache.texera.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  ExecutionStatsUpdate,
  RuntimeStatisticsPersist
}
import org.apache.texera.amber.engine.architecture.deploysemantics.layer.WorkerExecution
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest,
  QueryStatisticsRequest,
  StatisticsUpdateTarget
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.COMPLETED
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  WorkerMetricsResponse
}
import org.apache.texera.amber.util.VirtualIdentityUtils

/** Get statistics from all the workers
  *
  * possible sender: controller(by statusUpdateAskHandle)
  */
trait QueryWorkerStatisticsHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  private var globalQueryStatsOngoing = false

  // Minimum of the two timer intervals converted to nanoseconds.
  // A full-graph worker query is skipped and served from cache when the last completed
  // query falls within this window, avoiding redundant worker RPCs.
  private val minQueryIntervalNs: Long =
    Math.min(
      ApplicationConfig.getStatusUpdateIntervalInMs,
      ApplicationConfig.getRuntimeStatisticsPersistenceIntervalInMs
    ) * 1_000_000L

  // Nanosecond timestamp of the last completed full-graph worker stats query.
  @volatile private var lastWorkerQueryTimestampNs: Long = 0L

  // Reads the current cached stats and forwards them to the appropriate client sink(s).
  private def forwardStats(updateTarget: StatisticsUpdateTarget): Unit = {
    val stats = cp.workflowExecution.getAllRegionExecutionsStats
    updateTarget match {
      case StatisticsUpdateTarget.UI_ONLY =>
        sendToClient(ExecutionStatsUpdate(stats))
      case StatisticsUpdateTarget.PERSISTENCE_ONLY =>
        sendToClient(RuntimeStatisticsPersist(stats))
      case StatisticsUpdateTarget.BOTH_UI_AND_PERSISTENCE |
          StatisticsUpdateTarget.Unrecognized(_) =>
        sendToClient(ExecutionStatsUpdate(stats))
        sendToClient(RuntimeStatisticsPersist(stats))
    }
  }

  override def controllerInitiateQueryStatistics(
      msg: QueryStatisticsRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    // Avoid issuing concurrent full-graph statistics queries.
    // If a global query is already in progress, skip this request.
    if (globalQueryStatsOngoing && msg.filterByWorkers.isEmpty) {
      // A query is already in-flight: serve the last completed query's cached data,
      // or drop silently if no prior query has finished yet.
      if (lastWorkerQueryTimestampNs > 0) forwardStats(msg.updateTarget)
      return EmptyReturn()
    }

    var opFilter: Set[PhysicalOpIdentity] = Set.empty
    // Only enforce the single-query restriction for full-graph queries.
    if (msg.filterByWorkers.isEmpty) {
      if (System.nanoTime() - lastWorkerQueryTimestampNs < minQueryIntervalNs) {
        // Cache is still fresh: the faster timer already queried workers recently.
        forwardStats(msg.updateTarget)
        return EmptyReturn()
      }
      globalQueryStatsOngoing = true
    } else {
      // Map the filtered worker IDs (if any) to their corresponding physical operator IDs
      val initialOps: Set[PhysicalOpIdentity] =
        msg.filterByWorkers.map(VirtualIdentityUtils.getPhysicalOpId).toSet

      // Include all transitive upstream operators in the filter set
      opFilter = {
        val visited = scala.collection.mutable.Set.empty[PhysicalOpIdentity]
        val toVisit = scala.collection.mutable.Queue.from(initialOps)

        while (toVisit.nonEmpty) {
          val current = toVisit.dequeue()
          if (visited.add(current)) {
            val upstreamOps = cp.workflowScheduler.physicalPlan.getUpstreamPhysicalOpIds(current)
            toVisit.enqueueAll(upstreamOps)
          }
        }

        visited.toSet
      }
    }

    // Traverse the physical plan in reverse topological order (sink to source),
    // grouped by layers of parallel operators.
    val layers = cp.workflowScheduler.physicalPlan.layeredReversedTopologicalOrder

    // Accumulator to collect all (exec, wid, state, stats) results
    val collectedResults =
      scala.collection.mutable.ArrayBuffer.empty[(WorkerExecution, WorkerMetricsResponse, Long)]

    // Recursively process each operator layer sequentially (top-down in reverse topo order)
    def processLayers(layers: Seq[Set[PhysicalOpIdentity]]): Future[Unit] =
      layers match {
        case Nil =>
          // All layers have been processed
          Future.Done

        case layer +: rest =>
          // Issue statistics queries to all eligible workers in the current layer
          val futures = layer.toSeq.flatMap { opId =>
            // Skip operators not included in the filtered subset (if any)
            if (opFilter.nonEmpty && !opFilter.contains(opId)) {
              Seq.empty
            } else {
              cp.workflowExecution.getLatestOperatorExecutionOption(opId) match {
                // Operator region has not been initialized yet; skip in this polling round.
                case None       => Seq.empty
                case Some(exec) =>
                  // Skip completed operators
                  if (exec.getState == COMPLETED) {
                    Seq.empty
                  } else {
                    // Select all workers for this operator
                    val workerIds = exec.getWorkerIds

                    // Send queryStatistics to each worker and update internal state on reply
                    workerIds.map { wid =>
                      workerInterface.queryStatistics(EmptyRequest(), wid).map { resp =>
                        collectedResults.addOne(
                          (exec.getWorkerExecution(wid), resp, System.nanoTime())
                        )
                      }
                    }
                  }
              }
            }
          }

          // After all worker queries in this layer complete, process the next layer
          Future.collect(futures).flatMap(_ => processLayers(rest))
      }

    // Start processing all layers and forward stats to the appropriate sink(s) on completion.
    processLayers(layers).map { _ =>
      collectedResults.foreach {
        case (wExec, resp, timestamp) =>
          wExec.update(timestamp, resp.metrics.workerState, resp.metrics.workerStatistics)
      }
      forwardStats(msg.updateTarget)
      // Record the completion timestamp before releasing the lock so that any timer
      // firing in between sees a valid cache entry rather than triggering a redundant query.
      if (globalQueryStatsOngoing) {
        lastWorkerQueryTimestampNs = System.nanoTime()
        globalQueryStatsOngoing = false
      }
      EmptyReturn()
    }
  }

}
