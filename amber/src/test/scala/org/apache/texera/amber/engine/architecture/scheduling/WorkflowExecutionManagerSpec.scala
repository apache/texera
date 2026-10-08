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

package org.apache.texera.amber.engine.architecture.scheduling

import com.twitter.util.Future
import org.apache.pekko.actor.ActorSystem
import org.apache.texera.amber.core.WorkflowRuntimeException
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{InputPort, PhysicalOp, PortIdentity}
import org.apache.texera.amber.engine.architecture.coordinator.CoordinatorConfig
import org.apache.texera.amber.engine.architecture.coordinator.execution.WorkflowExecution
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  ControlError,
  EmptyReturn,
  ErrorLanguage
}
import org.apache.texera.amber.engine.architecture.scheduling.RegionExecutionManagerTestSupport._
import org.apache.texera.amber.engine.architecture.scheduling.config.{
  OperatorConfig,
  ResourceConfig,
  WorkerConfig
}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

class WorkflowExecutionManagerSpec
    extends TestKit(ActorSystem("WorkflowExecutionManagerSpec", AmberRuntime.pekkoConfig))
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with RegionExecutionManagerTestSupport {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  // -- Helpers used only by the jump-to-operator-region tests --

  private def jumpRegion(regionId: Long, opId: String): Region = {
    val physicalOp = PhysicalOp(
      PhysicalOpIdentity(OperatorIdentity(opId), "main"),
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OpExecInitInfo.Empty
    )
    Region(RegionIdentity(regionId), Set(physicalOp), Set.empty)
  }

  private def threeLevelSchedule(): (Region, Region, Region, Schedule) = {
    val first = jumpRegion(1, "first")
    val second = jumpRegion(2, "second")
    val third = jumpRegion(3, "third")
    val schedule = Schedule(
      Map(
        0 -> Set(first),
        1 -> Set(second),
        2 -> Set(third)
      )
    )
    (first, second, third, schedule)
  }

  private def newJumpManager(schedule: Schedule): WorkflowExecutionManager = {
    val manager = new WorkflowExecutionManager(WorkflowExecution(), null, null)
    manager.schedule = schedule
    manager
  }

  private def nextRegions(manager: WorkflowExecutionManager): Set[Region] = {
    val schedule = manager.schedule
    if (schedule.hasNext) schedule.next() else Set.empty
  }

  // Mirrors what JumpToOperatorRegionHandler does: read the current schedule, scan for the
  // level containing the target operator, and replace the schedule with a copy whose cursor is
  // at that level.
  private def jumpTo(manager: WorkflowExecutionManager, opName: String): Unit = {
    val opId = OperatorIdentity(opName)
    val schedule = manager.schedule
    schedule.levelSets
      .collectFirst {
        case (level, regions) if regions.exists(_.getOperators.exists(_.id.logicalOpId == opId)) =>
          level
      }
      .foreach { targetLevel =>
        manager.schedule = schedule.copy(initialLevelIndex = targetLevel)
      }
  }

  "WorkflowExecutionManager" should
    "start the next region only after previous region termination succeeds" in {
    val firstOp = createSourceOp("first-op")
    val firstWorkerId = createWorkerId(firstOp)
    val firstRegion = createSingleWorkerRegion(1, firstOp, firstWorkerId)

    val secondOp = createSourceOp("second-op")
    val secondWorkerId = createWorkerId(secondOp)
    val secondRegion = createSingleWorkerRegion(2, secondOp, secondWorkerId)

    val workflowExecution = WorkflowExecution()
    seedReusableWorkerExecution(workflowExecution, seedRegionId = 101, firstOp, firstWorkerId)
    seedReusableWorkerExecution(workflowExecution, seedRegionId = 102, secondOp, secondWorkerId)

    // First region's worker holds endWorker pending until we explicitly fulfill it; the second
    // region's worker terminates immediately. This lets us assert the second region cannot start
    // until termination of the first finishes.
    val rpcProbe = new CoordinatorRpcProbe(
      endWorkerResponse = call => if (call.receiver == firstWorkerId) None else Some(EmptyReturn())
    )
    val coordinator = createCoordinatorHarness()
    registerLiveWorker(coordinator.actorRefService, firstWorkerId)
    registerLiveWorker(coordinator.actorRefService, secondWorkerId)

    val workflowManager = new WorkflowExecutionManager(
      workflowExecution,
      CoordinatorConfig(None, None, None, None),
      rpcProbe.asyncRPCClient
    )
    workflowManager.schedule = Schedule(Map(0 -> Set(firstRegion), 1 -> Set(secondRegion)))
    workflowManager.setupActorRefService(coordinator.actorRefService)

    await(workflowManager.advanceRegionExecutions(coordinator.actorService))
    assert(rpcProbe.startedWorkers == Seq(firstWorkerId))

    val advanceFuture = workflowManager.advanceRegionExecutions(coordinator.actorService)

    waitUntil(rpcProbe.endWorkerCalls.size == 1)
    assert(advanceFuture.poll.isEmpty)
    assert(!rpcProbe.initializedWorkers.contains(secondWorkerId))
    assert(coordinator.actorRefService.hasActorRef(firstWorkerId))

    rpcProbe.fulfill(rpcProbe.onlyEndWorkerCall, EmptyReturn())
    await(advanceFuture)

    assert(!coordinator.actorRefService.hasActorRef(firstWorkerId))
    assert(rpcProbe.initializedWorkers.contains(secondWorkerId))
    assert(rpcProbe.startedWorkers.contains(secondWorkerId))
  }

  // Same failure as a Python UDF whose code is missing `from pytexera import *`.
  private val udfInitError = "NameError: name 'UDFTableOperator' is not defined"

  /**
    * Mirrors a region like HashJoin-probe + Python UDF: the probe op has a dependee input port,
    * so the region first runs a dependee phase with only the probe, and launches the remaining
    * operators (here, the UDF) only once the dependee port completes. That second launch is
    * triggered from a later advance round.
    *
    * Runs round 1, marks the probe's dependee port completed, and returns the round-2 future
    * together with the probe and the UDF worker id.
    */
  private def runTwoPhaseRegion(
      udfInitFails: Boolean
  ): (Future[Unit], CoordinatorRpcProbe, ActorVirtualIdentity) = {
    val probeOp = createSourceOp("probe-op").withInputPorts(
      List(
        InputPort(PortIdentity(0)),
        InputPort(PortIdentity(1), dependencies = List(PortIdentity(0)))
      )
    )
    val probeWorkerId = createWorkerId(probeOp)
    val udfOp = createSourceOp("udf-op")
    val udfWorkerId = createWorkerId(udfOp)

    val region = Region(
      RegionIdentity(1),
      physicalOps = Set(probeOp, udfOp),
      physicalLinks = Set.empty,
      resourceConfig = Some(
        ResourceConfig(
          operatorConfigs = Map(
            probeOp.id -> OperatorConfig(List(WorkerConfig(probeWorkerId))),
            udfOp.id -> OperatorConfig(List(WorkerConfig(udfWorkerId)))
          )
        )
      )
    )

    val workflowExecution = WorkflowExecution()
    seedReusableWorkerExecution(workflowExecution, seedRegionId = 101, probeOp, probeWorkerId)
    seedReusableWorkerExecution(workflowExecution, seedRegionId = 102, udfOp, udfWorkerId)

    val rpcProbe = new CoordinatorRpcProbe(
      endWorkerResponse = _ => Some(EmptyReturn()),
      initializeExecutorResponse = call =>
        if (udfInitFails && call.receiver == udfWorkerId)
          ControlError(
            errorMessage = udfInitError,
            errorDetails = "",
            stackTrace = "",
            language = ErrorLanguage.PYTHON
          )
        else EmptyReturn()
    )
    val coordinator = createCoordinatorHarness()
    registerLiveWorker(coordinator.actorRefService, probeWorkerId)
    registerLiveWorker(coordinator.actorRefService, udfWorkerId)

    val workflowManager = new WorkflowExecutionManager(
      workflowExecution,
      CoordinatorConfig(None, None, None, None),
      rpcProbe.asyncRPCClient
    )
    workflowManager.schedule = Schedule(Map(0 -> Set(region)))
    workflowManager.setupActorRefService(coordinator.actorRefService)

    // Round 1: only the dependee phase (the probe op) is launched.
    await(workflowManager.advanceRegionExecutions(coordinator.actorService))
    assert(rpcProbe.initializedWorkers == Seq(probeWorkerId))
    assert(rpcProbe.startedWorkers == Seq(probeWorkerId))

    // The probe's dependee port completes (e.g., the hash-join build side is fully read).
    workflowExecution
      .getRegionExecution(region.id)
      .getOperatorExecution(probeOp.id)
      .getWorkerExecution(probeWorkerId)
      .getInputPortExecution(PortIdentity(0))
      .setCompleted()

    // Round 2 launches the non-dependee phase (the UDF).
    (
      workflowManager.advanceRegionExecutions(coordinator.actorService),
      rpcProbe,
      udfWorkerId
    )
  }

  it should "fail coordination when a worker fails to initialize in a later region phase" in {
    val (round2, rpcProbe, udfWorkerId) = runTwoPhaseRegion(udfInitFails = true)

    val error = intercept[WorkflowRuntimeException](await(round2))
    assert(error.getMessage.contains(udfInitError))
    // The failing worker is carried so the surfaced FatalError can point at it.
    assert(error.relatedWorkerId.contains(udfWorkerId))
    assert(rpcProbe.initializedWorkers.contains(udfWorkerId))
    assert(!rpcProbe.startedWorkers.contains(udfWorkerId))
  }

  it should "launch the later region phase when its workers initialize successfully" in {
    val (round2, rpcProbe, udfWorkerId) = runTwoPhaseRegion(udfInitFails = false)

    await(round2)
    assert(rpcProbe.initializedWorkers.contains(udfWorkerId))
    assert(rpcProbe.startedWorkers.contains(udfWorkerId))
  }

  "Jumping to an operator's region" should
    "make the next scheduled region contain the target operator's region" in {
    val (first, second, _, schedule) = threeLevelSchedule()
    val manager = newJumpManager(schedule)

    assert(nextRegions(manager) == Set(first))
    assert(nextRegions(manager) == Set(second))

    jumpTo(manager, "first")

    assert(nextRegions(manager) == Set(first))
  }

  it should "support multiple sequential jumps interleaved with region pulls" in {
    val (first, second, third, schedule) = threeLevelSchedule()
    val manager = newJumpManager(schedule)

    assert(nextRegions(manager) == Set(first))
    assert(nextRegions(manager) == Set(second))

    jumpTo(manager, "first")
    assert(nextRegions(manager) == Set(first))

    jumpTo(manager, "second")
    assert(nextRegions(manager) == Set(second))
    assert(nextRegions(manager) == Set(third))

    jumpTo(manager, "first")
    assert(nextRegions(manager) == Set(first))
  }

  it should "be a no-op when the target operator is not in any scheduled region" in {
    val (first, second, _, schedule) = threeLevelSchedule()
    val manager = newJumpManager(schedule)

    assert(nextRegions(manager) == Set(first))

    jumpTo(manager, "does-not-exist")

    // Iteration position must be unaffected by an unknown target.
    assert(nextRegions(manager) == Set(second))
  }

  it should "leave the schedule untouched when called repeatedly with unknown operators" in {
    val (first, second, third, schedule) = threeLevelSchedule()
    val manager = newJumpManager(schedule)

    jumpTo(manager, "ghost-1")
    jumpTo(manager, "ghost-2")
    jumpTo(manager, "ghost-3")

    assert(nextRegions(manager) == Set(first))
    assert(nextRegions(manager) == Set(second))
    assert(nextRegions(manager) == Set(third))
  }

  it should "allow jumping back to the first region after the schedule is exhausted" in {
    val (first, second, third, schedule) = threeLevelSchedule()
    val manager = newJumpManager(schedule)

    assert(nextRegions(manager) == Set(first))
    assert(nextRegions(manager) == Set(second))
    assert(nextRegions(manager) == Set(third))
    assert(nextRegions(manager) == Set.empty)

    jumpTo(manager, "first")
    assert(nextRegions(manager) == Set(first))
  }

  it should "support jumping forward past regions that have not yet been pulled" in {
    val (first, _, third, schedule) = threeLevelSchedule()
    val manager = newJumpManager(schedule)

    assert(nextRegions(manager) == Set(first))

    jumpTo(manager, "third")
    assert(nextRegions(manager) == Set(third))
    assert(nextRegions(manager) == Set.empty)
  }

  it should "replay the target-onward range each time it jumps back" in {
    // Schedule ABCDEF: jumping from E back to C yields the visible sequence ABCDECDEF; jumping
    // again from E back to C yields ABCDECDECDEF.
    val a = jumpRegion(1, "a")
    val b = jumpRegion(2, "b")
    val c = jumpRegion(3, "c")
    val d = jumpRegion(4, "d")
    val e = jumpRegion(5, "e")
    val f = jumpRegion(6, "f")
    val schedule = Schedule(
      Map(0 -> Set(a), 1 -> Set(b), 2 -> Set(c), 3 -> Set(d), 4 -> Set(e), 5 -> Set(f))
    )
    val manager = newJumpManager(schedule)

    Seq(a, b, c, d, e).foreach { region =>
      assert(nextRegions(manager) == Set(region))
    }

    jumpTo(manager, "c")
    Seq(c, d, e).foreach { region =>
      assert(nextRegions(manager) == Set(region))
    }

    jumpTo(manager, "c")
    Seq(c, d, e, f).foreach { region =>
      assert(nextRegions(manager) == Set(region))
    }

    assert(nextRegions(manager) == Set.empty)
  }
}
