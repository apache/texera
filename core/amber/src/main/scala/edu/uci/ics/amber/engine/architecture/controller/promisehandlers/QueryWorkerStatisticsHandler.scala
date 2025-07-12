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

package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.core.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  ExecutionStatsUpdate
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerExecution
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest,
  QueryStatisticsRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.{EmptyReturn, WorkerMetricsResponse}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.COMPLETED
import edu.uci.ics.amber.util.VirtualIdentityUtils

/** Get statistics from all the workers
  *
  * possible sender: controller(by statusUpdateAskHandle)
  */
trait QueryWorkerStatisticsHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  private var globalQueryStatsOngoing = false

  override def controllerInitiateQueryStatistics(
      msg: QueryStatisticsRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    // Avoid issuing concurrent full-graph statistics queries.
    // If a global query is already in progress, skip this request.
    if (globalQueryStatsOngoing && msg.filterByWorkers.isEmpty) {
      return EmptyReturn()
    }

    var opFilter: Set[PhysicalOpIdentity] = Set.empty
    // Only enforce the single-query restriction for full-graph queries.
    if (msg.filterByWorkers.isEmpty) {
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
              val exec = cp.workflowExecution.getLatestOperatorExecution(opId)
              // Skip completed operators
              if (exec.getState == COMPLETED) {
                Seq.empty
              } else {
                // Select all workers for this operator
                val workerIds = exec.getWorkerIds

                // Send queryStatistics to each worker and update internal state on reply
                workerIds.map { wid =>
                  workerInterface.queryStatistics(EmptyRequest(), wid).map { resp =>
                    collectedResults.addOne((exec.getWorkerExecution(wid), resp, System.nanoTime()))
                  }
                }
              }
            }
          }

          // After all worker queries in this layer complete, process the next layer
          Future.collect(futures).flatMap(_ => processLayers(rest))
      }

    // Start processing all layers and update the frontend after completion
    processLayers(layers).map { _ =>
      collectedResults.foreach {
        case (wExec, resp, timestamp) =>
          wExec.update(timestamp, resp.metrics.workerState, resp.metrics.workerStatistics)
      }
      sendToClient(
        ExecutionStatsUpdate(cp.workflowExecution.getAllRegionExecutionsStats)
      )
      // Release the global query lock if it was set
      if (globalQueryStatsOngoing) {
        globalQueryStatsOngoing = false
      }
      EmptyReturn()
    }
  }

}
