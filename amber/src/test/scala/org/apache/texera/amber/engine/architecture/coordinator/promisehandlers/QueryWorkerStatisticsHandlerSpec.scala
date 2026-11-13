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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalLink, PhysicalPlan, PortIdentity}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorProcessor,
  ExecutionStatsUpdate
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  QueryStatisticsRequest,
  StatisticsUpdateTarget,
  ControlInvocation => ControlInvocationPayload
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  ControlError,
  ReturnInvocation,
  WorkerMetricsResponse
}
import org.apache.texera.amber.engine.architecture.rpc.coordinatorservice.CoordinatorServiceGrpc.{
  METHOD_COORDINATOR_INITIATE_QUERY_STATISTICS,
  METHOD_COORDINATOR_STATISTICS_QUERY_DEADLINE
}
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_QUERY_STATISTICS
import org.apache.texera.amber.engine.architecture.scheduling.RegionExecutionManagerTestSupport.{
  createSourceOp,
  createWorkerId,
  seedReusableWorkerExecution
}
import org.apache.texera.amber.engine.architecture.scheduling.{
  CoordinatorWithRegionInFlight,
  RegionExecutionManagerTestSupport
}
import org.apache.texera.amber.engine.architecture.worker.statistics.{
  WorkerMetrics,
  WorkerStatistics
}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.apache.texera.amber.engine.common.virtualidentity.util.{CLIENT, COORDINATOR, SELF}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration.FiniteDuration

/**
  * Statistics queries must always end, so that nothing awaiting one can stall (#6924):
  *  - a query has a deadline, after which a worker that has not answered keeps its last known
  *    statistics until its reply arrives;
  *  - a full-graph query holds a lock (`globalQueryStatsOngoing`) so concurrent polls are served
  *    from the cache, and the lock must be released however the query ends.
  */
class QueryWorkerStatisticsHandlerSpec
    extends TestKit(ActorSystem("QueryWorkerStatisticsHandlerSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with RegionExecutionManagerTestSupport {

  private val physicalOp = createSourceOp("query-stats-op")
  private val workerId = createWorkerId(physicalOp)
  private var nextCommandId = 1L

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  /** A coordinator with one running (not completed) worker, which every query reaches. */
  private def fixture(): CoordinatorWithRegionInFlight =
    newCoordinatorInFlight(physicalOp, workerId)

  private val upstreamOp = createSourceOp("query-stats-upstream-op")
  private val upstreamWorkerId = createWorkerId(upstreamOp)

  /**
    * As [[fixture]], plus a running upstream operator linked into the first one. Queries walk the
    * plan from sink to source, so the first worker is asked first and the upstream one after it.
    */
  private def twoLayerFixture(): CoordinatorWithRegionInFlight = {
    val f = fixture()
    seedReusableWorkerExecution(
      f.cp.workflowExecution,
      seedRegionId = 202,
      upstreamOp,
      upstreamWorkerId
    )
    // The link only orders the two operators into layers: the traversal reads operator ids, not
    // ports, so linking into a source operator (which has no input port) is enough here.
    f.cp.workflowScheduler.physicalPlan = PhysicalPlan(
      Set(physicalOp, upstreamOp),
      Set(PhysicalLink(upstreamOp.id, PortIdentity(), physicalOp.id, PortIdentity()))
    )
    f
  }

  /** Deliver a statistics query: a full-graph poll by default, as the status timer sends. */
  private def receiveQuery(
      cp: CoordinatorProcessor,
      filterByWorkers: Seq[ActorVirtualIdentity] = Seq.empty
  ): Unit = {
    cp.processDCM(
      ChannelIdentity(COORDINATOR, COORDINATOR, isControl = true),
      ControlInvocation(
        METHOD_COORDINATOR_INITIATE_QUERY_STATISTICS,
        QueryStatisticsRequest(filterByWorkers, StatisticsUpdateTarget.UI_ONLY),
        AsyncRPCContext(COORDINATOR, COORDINATOR),
        nextCommandId
      )
    )
    nextCommandId += 1
  }

  /** The `queryStatistics` requests sent to a worker (the first one by default) so far. */
  private def workerQueries(
      f: CoordinatorWithRegionInFlight,
      worker: ActorVirtualIdentity = workerId
  ): Seq[ControlInvocationPayload] =
    f.captured.toSeq.collect {
      // Matched case-insensitively, as the RPC servers do.
      case WorkflowFIFOMessage(channelId, _, inv: ControlInvocationPayload)
          if channelId.toWorkerId == worker &&
            inv.methodName.equalsIgnoreCase(METHOD_QUERY_STATISTICS.getBareMethodName) =>
        inv
    }

  /** The query deadlines the coordinator scheduled to itself so far, oldest first. */
  private def scheduledDeadlines(
      f: CoordinatorWithRegionInFlight
  ): Seq[(FiniteDuration, ControlInvocationPayload)] =
    f.actorService.scheduledToSelf.toSeq.collect {
      case (delay, inv: ControlInvocationPayload)
          if inv.methodName.equalsIgnoreCase(
            METHOD_COORDINATOR_STATISTICS_QUERY_DEADLINE.getBareMethodName
          ) =>
        (delay, inv)
    }

  /** Fire a scheduled deadline, routed as `Coordinator` routes a request it sent to itself. */
  private def fireDeadline(
      f: CoordinatorWithRegionInFlight,
      deadline: ControlInvocationPayload
  ): Unit =
    f.cp.processDCM(ChannelIdentity(SELF, SELF, isControl = true), deadline)

  private def replyFromWorker(
      f: CoordinatorWithRegionInFlight,
      query: ControlInvocationPayload,
      dataProcessingTime: Long = 0,
      worker: ActorVirtualIdentity = workerId
  ): Unit =
    f.cp.processDCM(
      ChannelIdentity(worker, COORDINATOR, isControl = true),
      ReturnInvocation(
        query.commandId,
        WorkerMetricsResponse(
          WorkerMetrics.defaultInstance.copy(
            workerStatistics =
              WorkerStatistics.defaultInstance.copy(dataProcessingTime = dataProcessingTime),
            stateVersion = 1
          )
        )
      )
    )

  private def workerStats(f: CoordinatorWithRegionInFlight): WorkerStatistics =
    f.cp.workflowExecution
      .getLatestOperatorExecutionOption(physicalOp.id)
      .get
      .getWorkerExecution(workerId)
      .getStats

  /** Statistics forwarded to the UI so far: one per query that ran to completion or hit the cache. */
  private def statsUpdates(f: CoordinatorWithRegionInFlight): Int =
    f.captured.count(msg =>
      msg.channelId.toWorkerId == CLIENT && msg.payload.isInstanceOf[ExecutionStatsUpdate]
    )

  "QueryWorkerStatisticsHandler" should "complete the query at its deadline when a worker never replies" in {
    val f = fixture()
    receiveQuery(f.cp)

    // The lost reply of #6924: without a deadline, the query (and whatever awaits it) hangs.
    fireDeadline(f, scheduledDeadlines(f).head._2)

    assert(statsUpdates(f) == 1)
    // It completed rather than failed: the cache is fresh and the lock is released.
    receiveQuery(f.cp)
    assert(workerQueries(f).size == 1)
    assert(statsUpdates(f) == 2)
  }

  it should "apply a worker's reply that arrives after the deadline" in {
    val f = fixture()
    receiveQuery(f.cp)
    fireDeadline(f, scheduledDeadlines(f).head._2)

    // A worker slower than the deadline still gets its statistics updated, just later.
    replyFromWorker(f, workerQueries(f).head, dataProcessingTime = 42)

    assert(workerStats(f).dataProcessingTime == 42)
  }

  it should "release the lock when a full-graph query fails inside the coordinator" in {
    val f = fixture()
    val plan = f.cp.workflowScheduler.physicalPlan
    // Fails while starting the traversal, after the lock is taken. Relies on the traversal
    // reading the physical plan, which is what this test uses to make the coordinator fail.
    f.cp.workflowScheduler.physicalPlan = null
    receiveQuery(f.cp)
    assert(workerQueries(f).isEmpty)

    // The failed query left no fresh cache entry, so the next poll reaches the worker.
    f.cp.workflowScheduler.physicalPlan = plan
    receiveQuery(f.cp)
    assert(workerQueries(f).size == 1)
  }

  it should "not let a filtered query release a full-graph query's lock" in {
    val f = fixture()
    receiveQuery(f.cp) // full-graph, still in flight
    receiveQuery(f.cp, filterByWorkers = Seq(workerId))
    val Seq(_, (_, filteredDeadline)) = scheduledDeadlines(f)

    // The filtered query (as `portCompleted` issues) ends at its own deadline, while the
    // full-graph one is still waiting.
    fireDeadline(f, filteredDeadline)
    assert(statsUpdates(f) == 1)

    // The full-graph query still holds the lock, and the filtered one did not mark the cache as
    // a fresh full-graph result: the poll neither queries the workers nor forwards the cache.
    receiveQuery(f.cp)
    assert(workerQueries(f).size == 2)
    assert(statsUpdates(f) == 1)
  }

  it should "still ask the upstream layers once the deadline has passed" in {
    val f = twoLayerFixture()
    receiveQuery(f.cp)
    // The sink layer is asked first, and the query waits for it.
    assert(workerQueries(f).size == 1)
    assert(workerQueries(f, upstreamWorkerId).isEmpty)

    fireDeadline(f, scheduledDeadlines(f).head._2)

    // Out of time: the upstream layer is asked without waiting, and the query completes.
    assert(workerQueries(f, upstreamWorkerId).size == 1)
    assert(statsUpdates(f) == 1)
  }

  it should "withdraw the deadline once every worker has answered" in {
    val f = fixture()
    receiveQuery(f.cp)
    replyFromWorker(f, workerQueries(f).head)

    // Nothing is left for the deadline to expire, so it is cancelled rather than delivered.
    assert(f.actorService.cancelled == Seq(scheduledDeadlines(f).head._2))
  }

  it should "keep a worker's last statistics when it answers with an error" in {
    val f = fixture()
    receiveQuery(f.cp)
    replyFromWorker(f, workerQueries(f).head, dataProcessingTime = 42)

    // E.g., a worker whose statistics collection throws: the query still completes, and the
    // worker keeps the statistics from its last successful reply. (A filtered query, so the poll
    // cache does not answer it instead.)
    receiveQuery(f.cp, filterByWorkers = Seq(workerId))
    f.cp.processDCM(
      ChannelIdentity(workerId, COORDINATOR, isControl = true),
      ReturnInvocation(workerQueries(f).last.commandId, ControlError.defaultInstance)
    )

    assert(statsUpdates(f) == 2)
    assert(workerStats(f).dataProcessingTime == 42)
  }

  it should "not schedule a deadline for a query that asks no worker" in {
    val f = fixture()
    // A plan whose only operator has no execution yet, so there is nobody to ask.
    f.cp.workflowScheduler.physicalPlan = PhysicalPlan(Set(upstreamOp), Set.empty)

    receiveQuery(f.cp)

    assert(statsUpdates(f) == 1)
    assert(scheduledDeadlines(f).isEmpty)
  }
}
