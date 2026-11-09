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

package org.apache.texera.amber.engine.architecture.coordinator.promisehandlers

import com.twitter.util.Future
import org.apache.texera.common.config.ApplicationConfig
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, PhysicalOpIdentity}
import org.apache.texera.amber.core.workflow.PhysicalLink
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorAsyncRPCHandlerInitializer,
  ExecutionStatsUpdate,
  LeastLoadedRoutingService,
  RuntimeStatisticsPersist
}
import org.apache.texera.amber.engine.architecture.deploysemantics.layer.WorkerExecution
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest,
  QueryStatisticsRequest,
  StatisticsUpdateTarget,
  UpdateRoutingPreferenceRequest
}
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.LeastLoadedPartitioning
import org.apache.texera.amber.engine.architecture.worker.statistics.WorkerStatistics
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.COMPLETED
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  WorkerMetricsResponse
}
import org.apache.texera.amber.util.VirtualIdentityUtils

/** Get statistics from all the workers
  *
  * possible sender: coordinator(by statusUpdateAskHandle)
  */
trait QueryWorkerStatisticsHandler {
  this: CoordinatorAsyncRPCHandlerInitializer =>

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

  /** Least-loaded links in the executing regions, paired with the partitioning
    * their senders were actually configured with -- read from the same resource
    * config that wired them, so this never disagrees with what the workers hold.
    */
  private def liveLeastLoadedLinks: Map[PhysicalLink, LeastLoadedPartitioning] = {
    cp.workflowExecutionManager.getExecutingRegions.toSeq
      .flatMap(_.resourceConfig.toSeq)
      .flatMap(_.linkConfigs.toSeq)
      .collect {
        case (link, linkConfig) =>
          linkConfig.partitioning match {
            case p: LeastLoadedPartitioning => Some(link -> p)
            case _                          => None
          }
      }
      .flatten
      .toMap
  }

  /** Tell each sender on a least-loaded link which receiver to fill its next
    * batch for, ranked by backlog measured this poll.
    *
    * Backlog is per worker, never summed across an operator's workers: telling
    * individual receivers apart is the entire point, and an operator-level sum
    * erases exactly that.
    */
  private def runLeastLoadedRouting(
      samples: Map[ActorVirtualIdentity, WorkerStatistics]
  ): Unit = {
    val links = liveLeastLoadedLinks
    if (links.isEmpty) return

    links.foreach {
      case (link, partitioning) =>
        val senders = partitioning.channels.map(_.fromWorkerId).distinct
        val receivers = partitioning.channels.map(_.toWorkerId).distinct
        val backlogByReceiver: Map[ActorVirtualIdentity, Long] = receivers.map { receiver =>
          val stats = samples.get(receiver)
          val delivered = stats.map(_.receivedTupleCount).getOrElse(0L)
          val processed = stats.map(_.inputTupleMetrics.map(_.tupleMetrics.count).sum).getOrElse(0L)
          // A receiver reporting more processed than received would mean the two
          // counters were read mid-update; clamp rather than rank on a negative.
          receiver -> math.max(0L, delivered - processed)
        }.toMap

        LeastLoadedRoutingService
          .computeAssignments(senders, receivers, backlogByReceiver)
          .foreach { assignment =>
            val index = receivers.indexOf(assignment.preferredReceiver)
            if (index >= 0) {
              workerInterface.updateRoutingPreference(
                UpdateRoutingPreferenceRequest(link, index),
                mkContext(assignment.sender)
              )
              if (ApplicationConfig.leastLoadedRoutingVerbose) {
                logger.info(
                  s"[least-loaded-routing] ${assignment.sender.name} -> " +
                    s"${assignment.preferredReceiver.name} " +
                    s"(backlog=${backlogByReceiver.getOrElse(assignment.preferredReceiver, 0L)})"
                )
              }
            }
          }
    }
  }

  override def coordinatorInitiateQueryStatistics(
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

    // Per-worker statistics for least-loaded routing. Full-graph rounds only:
    // a filtered round covers a subset of workers, and ranking on a partial
    // view would read an un-polled receiver's absence as "no backlog" rather
    // than "no reading this round".
    val routingSamples =
      scala.collection.mutable.HashMap.empty[ActorVirtualIdentity, WorkerStatistics]
    val collectRoutingSamples =
      ApplicationConfig.enableLeastLoadedRouting && msg.filterByWorkers.isEmpty

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
                        if (collectRoutingSamples) {
                          routingSamples.update(wid, resp.metrics.workerStatistics)
                        }
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
          // State is ordered by the worker's logical version; stats by receipt time.
          wExec.updateState(resp.metrics.stateVersion, resp.metrics.workerState)
          wExec.updateStats(timestamp, resp.metrics.workerStatistics)
      }
      forwardStats(msg.updateTarget)
      if (collectRoutingSamples) {
        // Runs after stats have been forwarded, and never escapes: statistics
        // reporting must not depend on routing, and a throw here would leave
        // globalQueryStatsOngoing stuck true, stalling every later poll.
        try runLeastLoadedRouting(routingSamples.toMap)
        catch {
          case t: Throwable => logger.warn(s"[least-loaded-routing] round failed: $t")
        }
      }
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
