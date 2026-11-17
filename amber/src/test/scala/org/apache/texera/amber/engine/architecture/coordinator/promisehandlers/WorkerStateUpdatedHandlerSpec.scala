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

import com.twitter.util.{Await, Duration}
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import org.apache.texera.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  PhysicalOp,
  PortIdentity,
  WorkflowContext
}
import org.apache.texera.amber.engine.architecture.coordinator.execution.RegionExecution
import org.apache.texera.amber.engine.architecture.coordinator.{
  ClientEvent,
  CoordinatorAsyncRPCHandlerInitializer,
  CoordinatorConfig,
  CoordinatorProcessor,
  ExecutionStatsUpdate,
  RuntimeStatisticsPersist
}
import org.apache.texera.amber.engine.architecture.deploysemantics.layer.WorkerExecution
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  WorkerStateUpdatedRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  WorkflowAggregatedState
}
import org.apache.texera.amber.engine.architecture.scheduling.{Region, RegionIdentity}
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import org.apache.texera.amber.engine.architecture.worker.statistics.WorkerState
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.executionruntimestate.OperatorMetrics
import org.apache.texera.amber.engine.common.virtualidentity.util.{CLIENT, COORDINATOR}
import org.apache.texera.amber.util.VirtualIdentityUtils
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ArrayBuffer

/**
  * `workerStateUpdated` is how a worker's own state transitions reach the coordinator's execution
  * bookkeeping: the reporting worker is named only by `ctx.sender`, from which the handler derives
  * the physical operator, looks it up among the *running* region executions, and applies the
  * report to that worker's `WorkerExecution` with the request's `stateVersion`. It then pushes the
  * whole workflow's statistics snapshot to the client twice — once as a live update, once for
  * persistence.
  *
  * What these tests pin:
  *   - the update is scoped to the sender alone (not the operator's other workers, not another
  *     operator), is version-ordered so a stale report cannot clobber a newer state, and stops
  *     entirely once the worker reaches a terminal state;
  *   - only running region executions are consulted, and only the first one owning the operator,
  *     while the broadcast statistics cover every region execution, running or not;
  *   - the silent-skip branch: when no running region execution owns the sender's operator the
  *     state report is dropped, yet both client events still fire and the reply is still an empty
  *     success. This is the branch the handler has no other coverage for.
  *
  * These are characterization tests: they record what the handler does today, including both kinds
  * of silent drop -- the handler skipping a report that no running region execution owns, and
  * `WorkerExecution.updateState` refusing one that is not strictly newer -- and the one place where
  * a missing worker execution is not silent at all (see the `NullPointerException` case).
  *
  * The harness mirrors `EmbeddedControlMessageHandlerSpec`: a real `CoordinatorProcessor` plus a
  * `CoordinatorAsyncRPCHandlerInitializer`, with the handler invoked directly and the dispatched
  * messages captured through the coordinator's output handler. No ActorSystem is needed.
  */
class WorkerStateUpdatedHandlerSpec extends AnyFlatSpec {

  private val awaitTimeout = Duration.fromSeconds(1)
  // The single output port each seeded region declares; completing it is what moves a region
  // execution out of the running set.
  private val outputPort = PortIdentity()
  private val clientChannel = ChannelIdentity(COORDINATOR, CLIENT, isControl = true)

  private val opA = mkPhysicalOp("op-a")
  private val opB = mkPhysicalOp("op-b")
  private val workerA0 = mkWorkerId(opA, 0)
  private val workerA1 = mkWorkerId(opA, 1)
  private val workerB0 = mkWorkerId(opB, 0)

  private def mkPhysicalOp(logicalOpId: String): PhysicalOp =
    PhysicalOp(
      PhysicalOpIdentity(OperatorIdentity(logicalOpId), "main"),
      DEFAULT_WORKFLOW_ID,
      DEFAULT_EXECUTION_ID,
      OpExecInitInfo.Empty
    )

  private def mkWorkerId(physicalOp: PhysicalOp, index: Int): ActorVirtualIdentity =
    VirtualIdentityUtils.createWorkerIdentity(DEFAULT_WORKFLOW_ID, physicalOp.id, index)

  private case class Fixture(
      init: CoordinatorAsyncRPCHandlerInitializer,
      sent: ArrayBuffer[WorkflowFIFOMessage]
  )

  private def newFixture(): Fixture = {
    val sent = ArrayBuffer[WorkflowFIFOMessage]()
    val outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit = {
      case Right(m) => sent += m
      case _        => ()
    }
    val cp = new CoordinatorProcessor(
      new WorkflowContext(),
      CoordinatorConfig(None, None, None, None),
      COORDINATOR,
      outputHandler
    )
    Fixture(new CoordinatorAsyncRPCHandlerInitializer(cp), sent)
  }

  /**
    * Seed one region execution owning the given operators and workers. A region execution is
    * completed exactly when all of its ports are completed, so declaring one output port per
    * operator gives each test a switch between the running set and the completed set.
    */
  private def seedRegion(
      fixture: Fixture,
      regionId: Long,
      workersByOp: Seq[(PhysicalOp, Seq[ActorVirtualIdentity])],
      completed: Boolean = false
  ): RegionExecution = {
    val region = Region(
      RegionIdentity(regionId),
      physicalOps = workersByOp.map(_._1).toSet,
      physicalLinks = Set.empty,
      ports = workersByOp.map {
        case (physicalOp, _) => GlobalPortIdentity(physicalOp.id, outputPort, input = false)
      }.toSet
    )
    val regionExecution = fixture.init.cp.workflowExecution.initRegionExecution(region)
    workersByOp.foreach {
      case (physicalOp, workerIds) =>
        val operatorExecution = regionExecution.initOperatorExecution(physicalOp.id)
        workerIds.foreach { workerId =>
          val workerExecution = operatorExecution.initWorkerExecution(workerId)
          if (completed) {
            workerExecution.getOutputPortExecution(outputPort).setCompleted()
          }
        }
    }
    assert(regionExecution.isCompleted == completed)
    regionExecution
  }

  /** Deliver a worker's state report exactly as the RPC layer would: sender carried by the context. */
  private def report(
      fixture: Fixture,
      sender: ActorVirtualIdentity,
      state: WorkerState,
      stateVersion: Long
  ): EmptyReturn =
    Await.result(
      fixture.init.workerStateUpdated(
        WorkerStateUpdatedRequest(state, stateVersion),
        AsyncRPCContext(sender, COORDINATOR)
      ),
      awaitTimeout
    )

  private def workerExecutionOf(
      regionExecution: RegionExecution,
      physicalOp: PhysicalOp,
      workerId: ActorVirtualIdentity
  ): WorkerExecution =
    regionExecution.getOperatorExecution(physicalOp.id).getWorkerExecution(workerId)

  private def stateOf(
      regionExecution: RegionExecution,
      physicalOp: PhysicalOp,
      workerId: ActorVirtualIdentity
  ): WorkerState =
    workerExecutionOf(regionExecution, physicalOp, workerId).getState

  private def clientEvents(fixture: Fixture): Seq[(ChannelIdentity, ClientEvent)] =
    fixture.sent.toSeq.collect {
      case WorkflowFIFOMessage(channelId, _, event: ClientEvent) => (channelId, event)
    }

  private def broadcastStats(fixture: Fixture): Seq[Map[String, OperatorMetrics]] =
    clientEvents(fixture).map(_._2).collect {
      case ExecutionStatsUpdate(metrics)     => metrics
      case RuntimeStatisticsPersist(metrics) => metrics
    }

  /** The kinds of client event the handler emitted, in emission order. */
  private def broadcastKinds(fixture: Fixture): Seq[String] =
    clientEvents(fixture).map(_._2).collect {
      case _: ExecutionStatsUpdate     => "ExecutionStatsUpdate"
      case _: RuntimeStatisticsPersist => "RuntimeStatisticsPersist"
    }

  private val oneBroadcastRound = Seq("ExecutionStatsUpdate", "RuntimeStatisticsPersist")

  behavior of "WorkerStateUpdatedHandler"

  it should "apply the reported state only to the sender's worker execution" in {
    val fixture = newFixture()
    val regionExecution =
      seedRegion(fixture, 1, Seq(opA -> Seq(workerA0, workerA1), opB -> Seq(workerB0)))

    assert(report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 2) == EmptyReturn())

    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.RUNNING)
    // A sibling worker of the same operator reports its own state separately, and another
    // operator's worker is not addressed by this report at all.
    assert(stateOf(regionExecution, opA, workerA1) == WorkerState.UNINITIALIZED)
    assert(stateOf(regionExecution, opB, workerB0) == WorkerState.UNINITIALIZED)
  }

  it should "resolve the owning operator from the sender of each report" in {
    val fixture = newFixture()
    val regionExecution =
      seedRegion(fixture, 1, Seq(opA -> Seq(workerA0), opB -> Seq(workerB0)))

    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 2)
    report(fixture, workerB0, WorkerState.PAUSED, stateVersion = 3)

    // Both operators live in the same region execution, so only the sender-derived physical
    // operator id can tell the two worker executions apart.
    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.RUNNING)
    assert(stateOf(regionExecution, opB, workerB0) == WorkerState.PAUSED)
  }

  it should "order reports by the request's state version, ignoring stale and repeated ones" in {
    val fixture = newFixture()
    val regionExecution = seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)))

    report(fixture, workerA0, WorkerState.PAUSED, stateVersion = 5)
    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.PAUSED)

    // A report that left the worker earlier but arrives later must not roll the state back.
    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 3)
    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.PAUSED)

    // Only a strictly newer version wins, so a re-delivered version is a no-op too.
    report(fixture, workerA0, WorkerState.READY, stateVersion = 5)
    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.PAUSED)

    // ... and the version the handler forwards is the request's own, not a fixed one.
    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 6)
    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.RUNNING)
  }

  it should "drop even a first report at the version sentinel, and apply the one above it" in {
    val fixture = newFixture()
    val regionExecution = seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)))

    // `WorkerExecution.lastStateVersion` starts at the -1 sentinel and the guard is
    // strictly-greater, so -1 is the one version that a *first* report cannot carry: it is
    // dropped as if it were stale. A real worker never emits it (`StateManager.stateVersion`
    // starts at 0 and only increases), which is precisely why the boundary is worth pinning --
    // it marks where the sentinel ends and real versions begin.
    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = -1)
    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.UNINITIALIZED)

    // 0, the first version a worker actually reports, clears the sentinel.
    report(fixture, workerA0, WorkerState.READY, stateVersion = 0)
    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.READY)

    // Both reports were broadcast regardless of whether they landed.
    assert(broadcastKinds(fixture) == oneBroadcastRound ++ oneBroadcastRound)
  }

  it should "leave a worker in its terminal state once it reported COMPLETED" in {
    val fixture = newFixture()
    val regionExecution = seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)))

    report(fixture, workerA0, WorkerState.COMPLETED, stateVersion = 4)
    // The handler forwards every report unconditionally; terminal absorption lives in
    // `WorkerExecution.updateState`, which refuses to move a COMPLETED/TERMINATED worker even
    // for a strictly newer version. Without that, a late RUNNING report would resurrect a
    // finished worker and drag the aggregated operator state back to RUNNING.
    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 9)

    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.COMPLETED)
    val stats = fixture.init.cp.workflowExecution.getAllRegionExecutionsStats
    assert(stats(opA.id.logicalOpId.id).operatorState == WorkflowAggregatedState.COMPLETED)
    // The refused report is still broadcast, like every other update the handler drops.
    assert(broadcastKinds(fixture) == oneBroadcastRound ++ oneBroadcastRound)
  }

  it should "leave a worker in its terminal state once it reported TERMINATED" in {
    val fixture = newFixture()
    val regionExecution = seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)))

    // The TERMINATED half of terminal absorption, reached purely through reports so that the
    // version guard cannot stand in for the terminal guard: version 9 is strictly newer than 3,
    // so only `isTerminal` can refuse it. Without this case, deleting TERMINATED from
    // `WorkerExecution.isTerminal` leaves the whole suite green -- the force-terminate test below
    // installs the Long.MaxValue ceiling at the same moment and is held up by the version guard.
    report(fixture, workerA0, WorkerState.TERMINATED, stateVersion = 3)
    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 9)

    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.TERMINATED)
  }

  it should "keep accepting and broadcasting reports after the region was force-terminated" in {
    val fixture = newFixture()
    val regionExecution = seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)))

    // The teardown path end to end: `RegionExecutionManager` force-terminates a region's workers,
    // and reports already in flight from those workers keep arriving at the handler afterwards.
    // The handler takes them normally -- reply and both broadcasts -- while the worker execution
    // stays TERMINATED.
    //
    // This case does NOT pin terminal absorption, and no single-guard mutation can fail it:
    // `forceTerminate` is `updateState(Long.MaxValue, TERMINATED)`, so it installs the terminal
    // state and the version ceiling at the same moment and either guard alone refuses everything
    // that follows. The guard itself is pinned by "leave a worker in its terminal state once it
    // reported TERMINATED" above, and the ceiling by "refuse every later report once one landed
    // at the maximum version" below.
    workerExecutionOf(regionExecution, opA, workerA0).forceTerminate()

    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 7)
    report(fixture, workerA0, WorkerState.COMPLETED, stateVersion = Long.MaxValue)

    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.TERMINATED)
    assert(broadcastKinds(fixture) == oneBroadcastRound ++ oneBroadcastRound)
  }

  it should "refuse every later report once one landed at the maximum version" in {
    val fixture = newFixture()
    val regionExecution = seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)))

    // The upper boundary, and the one that does not need a terminal state to bite: a
    // NON-terminal report at `Long.MaxValue` pins `lastStateVersion` at the ceiling, and since
    // the guard demands a strictly greater version, no later report can ever be accepted. The
    // worker is frozen in a non-terminal state for the rest of the execution.
    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = Long.MaxValue)
    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.RUNNING)

    report(fixture, workerA0, WorkerState.COMPLETED, stateVersion = 3)
    report(fixture, workerA0, WorkerState.COMPLETED, stateVersion = Long.MaxValue)

    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.RUNNING)
    // Every refused report is still broadcast, as on every other skip path.
    assert(broadcastKinds(fixture) == Seq.fill(3)(oneBroadcastRound).flatten)
  }

  it should "track state versions per worker, not per operator" in {
    val fixture = newFixture()
    val regionExecution = seedRegion(fixture, 1, Seq(opA -> Seq(workerA0, workerA1)))

    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 9)
    // Versions are per-worker logical clocks, so A0's high version must not shadow A1's low one.
    // Collapsing the version onto the operator (or sharing one WorkerExecution between workers)
    // would silently drop this second report.
    report(fixture, workerA1, WorkerState.READY, stateVersion = 1)

    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.RUNNING)
    assert(stateOf(regionExecution, opA, workerA1) == WorkerState.READY)
  }

  it should "advance the version even when the reported state is unchanged" in {
    val fixture = newFixture()
    val regionExecution = seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)))

    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 2)
    // A repeat of the same state at a newer version looks like a no-op but burns the version:
    // `updateState` reassigns the state and the version together, without comparing states.
    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 7)
    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.RUNNING)

    // The burn is only observable through what it rejects afterwards: a report at an
    // intermediate version is now stale, even though no state change ever became visible.
    report(fixture, workerA0, WorkerState.PAUSED, stateVersion = 5)
    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.RUNNING)

    // Above the burned version it applies again, so this is version bookkeeping, not a freeze.
    report(fixture, workerA0, WorkerState.PAUSED, stateVersion = 8)
    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.PAUSED)
  }

  it should "let an UNINITIALIZED report at version 0 consume the first real version" in {
    val fixture = newFixture()
    val regionExecution = seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)))

    // `UNINITIALIZED` is the proto default (statistics.proto: UNINITIALIZED = 0), so it is also
    // what an unset `state` field decodes to. Reported at version 0 it is applied like any other
    // state -- invisibly, since the worker execution starts UNINITIALIZED -- and moves
    // `lastStateVersion` from the -1 sentinel to 0.
    report(fixture, workerA0, WorkerState.UNINITIALIZED, stateVersion = 0)
    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.UNINITIALIZED)

    // A genuine version-0 report arriving after it is therefore dropped: the worker's true first
    // transition is lost, with nothing in the state to show a report was ever consumed.
    report(fixture, workerA0, WorkerState.READY, stateVersion = 0)
    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.UNINITIALIZED)

    // Version 1 still applies, so the loss is confined to the collision at version 0.
    report(fixture, workerA0, WorkerState.READY, stateVersion = 1)
    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.READY)
  }

  it should "apply the report to the running region execution, not a completed one" in {
    val fixture = newFixture()
    // Both region executions own the same operator and the same worker id; the completed one was
    // created first, so a handler consulting all region executions would pick it.
    val completedExecution =
      seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)), completed = true)
    val runningExecution = seedRegion(fixture, 2, Seq(opA -> Seq(workerA0)))

    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 2)

    assert(stateOf(runningExecution, opA, workerA0) == WorkerState.RUNNING)
    assert(stateOf(completedExecution, opA, workerA0) == WorkerState.UNINITIALIZED)
  }

  it should "apply the report to the first running region execution owning the operator" in {
    val fixture = newFixture()
    // Two *running* region executions own the same operator and the same worker id. The handler
    // updates only the first match in creation order -- note this is the opposite end from
    // `WorkflowExecution.getLatestOperatorExecution`, which searches in reverse creation order.
    val firstExecution = seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)))
    val secondExecution = seedRegion(fixture, 2, Seq(opA -> Seq(workerA0)))

    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 2)

    assert(stateOf(firstExecution, opA, workerA0) == WorkerState.RUNNING)
    assert(stateOf(secondExecution, opA, workerA0) == WorkerState.UNINITIALIZED)
  }

  it should "skip the state update when only a completed region owns the sender's operator" in {
    val fixture = newFixture()
    val completedExecution =
      seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)), completed = true)

    assert(report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 2) == EmptyReturn())

    // The report is dropped without a trace: no state change, no error, no log-visible signal.
    assert(stateOf(completedExecution, opA, workerA0) == WorkerState.UNINITIALIZED)
    // The broadcasts are not conditional on the update having landed, so both still fire.
    assert(broadcastKinds(fixture) == oneBroadcastRound)
  }

  it should "skip the state update when no region execution owns the sender's operator" in {
    val fixture = newFixture()
    val regionExecution = seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)))

    // `op-b` was never scheduled, so nothing in the workflow execution knows this worker.
    assert(report(fixture, workerB0, WorkerState.RUNNING, stateVersion = 2) == EmptyReturn())

    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.UNINITIALIZED)
    assert(broadcastKinds(fixture) == oneBroadcastRound)
  }

  it should "tolerate a report whose sender is not a worker identity" in {
    val fixture = newFixture()
    val regionExecution = seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)))

    // A non-worker identity maps to the `__DummyOperator` sentinel rather than failing, so the
    // handler takes the same skip branch instead of throwing.
    assert(report(fixture, COORDINATOR, WorkerState.RUNNING, stateVersion = 2) == EmptyReturn())

    assert(stateOf(regionExecution, opA, workerA0) == WorkerState.UNINITIALIZED)
    assert(broadcastKinds(fixture) == oneBroadcastRound)
  }

  it should "throw when the owning region execution has no execution for the sender" in {
    val fixture = newFixture()
    // The running region owns `op-a` but only worker 0 was ever initialized.
    seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)))

    // Characterization, not endorsement: the lookup returns `null` and the handler dereferences
    // it, so an unknown worker of a *known* operator fails loudly while an unknown operator is
    // skipped silently. Nothing is broadcast on this path.
    assertThrows[NullPointerException] {
      report(fixture, workerA1, WorkerState.RUNNING, stateVersion = 2)
    }
    assert(clientEvents(fixture).isEmpty)
  }

  it should "broadcast the post-update stats snapshot to the client, live update first" in {
    val fixture = newFixture()
    seedRegion(fixture, 1, Seq(opA -> Seq(workerA0)))

    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 2)

    val expectedStats = fixture.init.cp.workflowExecution.getAllRegionExecutionsStats
    // Both events carry the same snapshot and travel the client control channel; the live update
    // is emitted before the persistence copy.
    assert(
      clientEvents(fixture) == Seq(
        clientChannel -> ExecutionStatsUpdate(expectedStats),
        clientChannel -> RuntimeStatisticsPersist(expectedStats)
      )
    )
    // The snapshot is taken after the update is applied, so it already reports the new state.
    assert(expectedStats.keySet == Set(opA.id.logicalOpId.id))
    assert(expectedStats(opA.id.logicalOpId.id).operatorState == WorkflowAggregatedState.RUNNING)
  }

  it should "broadcast stats covering non-running region executions too" in {
    val fixture = newFixture()
    seedRegion(fixture, 1, Seq(opB -> Seq(workerB0)), completed = true)
    seedRegion(fixture, 2, Seq(opA -> Seq(workerA0)))

    report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 2)

    // Unlike the state update, the statistics snapshot spans every region execution, so the
    // client keeps seeing the operators of regions that already finished.
    assert(
      broadcastStats(fixture).forall(
        _.keySet == Set(opA.id.logicalOpId.id, opB.id.logicalOpId.id)
      )
    )
  }

  it should "broadcast an empty stats snapshot when there is no region execution at all" in {
    val fixture = newFixture()

    // Nothing has been scheduled yet -- the state a `WorkflowExecution` is in before the first
    // region starts, and the state a report from a straggler worker can still arrive in. The
    // aggregation over zero region executions yields an empty Map rather than failing, and the
    // broadcasts are unconditional, so the client receives both events with an empty payload.
    assert(report(fixture, workerA0, WorkerState.RUNNING, stateVersion = 2) == EmptyReturn())

    assert(fixture.init.cp.workflowExecution.getAllRegionExecutionsStats.isEmpty)
    assert(
      clientEvents(fixture) == Seq(
        clientChannel -> ExecutionStatsUpdate(Map.empty),
        clientChannel -> RuntimeStatisticsPersist(Map.empty)
      )
    )
  }
}
