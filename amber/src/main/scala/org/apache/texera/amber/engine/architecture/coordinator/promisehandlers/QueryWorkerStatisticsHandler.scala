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

import com.twitter.util.{Future, Promise}
import org.apache.pekko.actor.Cancellable
import org.apache.texera.common.config.ApplicationConfig
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, PhysicalOpIdentity}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorAsyncRPCHandlerInitializer,
  ExecutionStatsUpdate,
  RuntimeStatisticsPersist
}
import org.apache.texera.amber.engine.architecture.deploysemantics.layer.WorkerExecution
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest,
  QueryStatisticsRequest,
  StatisticsQueryDeadlineRequest,
  StatisticsUpdateTarget
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.COMPLETED
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  WorkerMetricsResponse
}
import org.apache.texera.amber.engine.architecture.rpc.coordinatorservice.CoordinatorServiceGrpc.METHOD_COORDINATOR_STATISTICS_QUERY_DEADLINE
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_QUERY_STATISTICS
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.apache.texera.amber.engine.common.virtualidentity.util.SELF
import org.apache.texera.amber.util.VirtualIdentityUtils

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

/** Get statistics from the workers and forward them to the client.
  *
  * possible sender: coordinator (its status and persistence timers, and the final refreshes
  * requested by `portCompleted` and `workerExecutionCompleted`)
  */
trait QueryWorkerStatisticsHandler {
  this: CoordinatorAsyncRPCHandlerInitializer =>

  // All state below is only touched on the coordinator's actor thread, including from the
  // continuations: every reply, and every query deadline, arrives as an input message rather
  // than being completed from a timer thread.
  private var globalQueryStatsOngoing = false

  /**
    * How long a query waits for the workers' replies. Far above the normal reply latency (a
    * worker normally answers within milliseconds), and long enough not to give up on a briefly
    * busy worker.
    */
  private val statisticsReplyTimeout: FiniteDuration =
    FiniteDuration(ApplicationConfig.getStatisticsQueryReplyTimeoutInMs, MILLISECONDS)

  // Deadlines of the queries in flight, by query id; see `newQueryDeadline`.
  private val pendingDeadlines = scala.collection.mutable.HashMap.empty[Long, Promise[Unit]]
  private var nextQueryId = 0L

  private class QueryDeadline(val queryId: Long, val passed: Future[Unit], val timer: Cancellable)

  /**
    * A deadline that passes [[statisticsReplyTimeout]] from now. The coordinator schedules a
    * `coordinatorStatisticsQueryDeadline` request to itself, the same way its status timer
    * schedules statistics polls, so the deadline is handled on the actor thread like any other
    * request. Without a deadline, one lost reply would stall the query, and with it the port
    * completion or workflow completion that awaits it (#6924).
    */
  private def newQueryDeadline(): QueryDeadline = {
    val queryId = nextQueryId
    nextQueryId += 1
    val passed = Promise[Unit]()
    pendingDeadlines(queryId) = passed
    val timer = cp.actorService.sendToSelfOnce(
      statisticsReplyTimeout,
      ControlInvocation(
        METHOD_COORDINATOR_STATISTICS_QUERY_DEADLINE,
        StatisticsQueryDeadlineRequest(queryId),
        AsyncRPCContext(SELF, SELF),
        AsyncRPCClient.IgnoreReplyAndDoNotLog
      )
    )
    new QueryDeadline(queryId, passed, timer)
  }

  override def coordinatorStatisticsQueryDeadline(
      msg: StatisticsQueryDeadlineRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    // A deadline whose query already ended, or one delivered twice (e.g., replayed from the log
    // while its timer is also re-scheduled during replay), finds nothing here and does nothing.
    pendingDeadlines.remove(msg.queryId).foreach(_.setDone())
    EmptyReturn()
  }

  /**
    * Send `queryStatistics` to a worker. The returned future completes once the reply has been
    * applied, or once the worker has answered with an error. The reply is applied whenever it
    * arrives, even after the query that asked for it stopped waiting, so a worker slower than
    * the deadline still gets its statistics updated, just later.
    */
  private def requestWorkerStatistics(
      workerId: ActorVirtualIdentity,
      workerExecution: WorkerExecution
  ): Future[Unit] = {
    val (invocation, reply) =
      createInvocation(METHOD_QUERY_STATISTICS.getBareMethodName, EmptyRequest(), workerId)
    cp.outputGateway.sendTo(workerId, invocation)
    reply
      .map { resp =>
        val metrics = resp.asInstanceOf[WorkerMetricsResponse].metrics
        // State is ordered by the worker's logical version; stats by receipt time.
        workerExecution.updateState(metrics.stateVersion, metrics.workerState)
        workerExecution.updateStats(System.nanoTime(), metrics.workerStatistics)
      }
      .handle {
        case err =>
          logger.warn(
            s"Statistics query to $workerId failed (${err.getMessage}); " +
              "keeping its last known statistics."
          )
      }
  }

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

    // The requests this query sent before its deadline passed, with the worker each went to, and
    // the query's deadline, created with the first request.
    val requested = scala.collection.mutable.ArrayBuffer.empty[(ActorVirtualIdentity, Future[Unit])]
    var deadlineOpt: Option[QueryDeadline] = None

    // Recursively process each operator layer sequentially (top-down in reverse topo order)
    def processLayers(layers: Seq[Set[PhysicalOpIdentity]]): Future[Unit] =
      layers match {
        case Nil =>
          // All layers have been processed
          Future.Done

        case layer +: rest =>
          // Issue statistics queries to all eligible workers in the current layer
          val requests = layer.toSeq.flatMap { opId =>
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
                    // Send queryStatistics to each worker; replies update its state on arrival
                    exec.getWorkerIds.toSeq.map { wid =>
                      wid -> requestWorkerStatistics(wid, exec.getWorkerExecution(wid))
                    }
                  }
              }
            }
          }
          if (requests.nonEmpty && deadlineOpt.isEmpty) deadlineOpt = Some(newQueryDeadline())
          // Requests sent after the deadline (to later layers) did not get the full timeout to
          // answer, so they are not reported as missing it.
          if (!deadlineOpt.exists(_.passed.isDefined)) requested ++= requests

          // After all worker queries in this layer complete, or once the deadline has passed,
          // process the next layer. Past the deadline, the remaining layers are still asked, but
          // not waited for; their replies are applied whenever they arrive.
          deadlineOpt match {
            case Some(deadline) if requests.nonEmpty && !deadline.passed.isDefined =>
              Future
                .collect(requests.map(_._2))
                .unit
                .or(deadline.passed)
                .flatMap(_ => processLayers(rest))
            case _ => processLayers(rest)
          }
      }

    // Traverse the physical plan in reverse topological order (sink to source), grouped by layers
    // of parallel operators, and forward stats to the appropriate sink(s) on completion. Started
    // inside `flatMap` so that an exception thrown while starting it fails the returned future
    // instead of escaping before `ensure` below is attached, which would leave the lock held.
    Future.Unit
      .flatMap(_ =>
        processLayers(cp.workflowScheduler.physicalPlan.layeredReversedTopologicalOrder)
      )
      .map { _ =>
        val unanswered = requested.collect { case (wid, replied) if !replied.isDefined => wid }
        if (unanswered.nonEmpty) {
          logger.warn(
            s"No statistics reply within $statisticsReplyTimeout from ${unanswered.mkString(", ")}; " +
              "keeping their last known statistics until they answer."
          )
        }
        forwardStats(msg.updateTarget)
        // Record the completion timestamp before releasing the lock so that any timer
        // firing in between sees a valid cache entry rather than triggering a redundant query.
        if (msg.filterByWorkers.isEmpty) lastWorkerQueryTimestampNs = System.nanoTime()
        EmptyReturn()
      }
      .ensure {
        // Release the lock however the query ends: clearing it only on success would leave every
        // later full-graph query serving the stale cache (#6924). A query cannot hang instead of
        // ending, since it has a deadline. A failed query records no timestamp, so the next poll
        // queries the workers again.
        if (msg.filterByWorkers.isEmpty) globalQueryStatsOngoing = false
        // The deadline is no longer needed: withdraw it, so a query that ends before it costs no
        // more messages than it did without one. (For a deadline that already passed, both are
        // no-ops.)
        deadlineOpt.foreach { deadline =>
          deadline.timer.cancel()
          pendingDeadlines.remove(deadline.queryId)
        }
      }
  }

}
