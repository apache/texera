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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.PhysicalOp
import org.apache.texera.amber.engine.architecture.controller.ControllerConfig
import org.apache.texera.amber.engine.architecture.controller.execution.WorkflowExecution
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.scheduling.RegionCoordinatorTestSupport._
import org.apache.texera.amber.engine.common.AmberRuntime
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

class WorkflowExecutionCoordinatorSpec
    extends TestKit(ActorSystem("WorkflowExecutionCoordinatorSpec", AmberRuntime.akkaConfig))
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with RegionCoordinatorTestSupport {

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

  private def newJumpCoordinator(schedule: Schedule): WorkflowExecutionCoordinator =
    new WorkflowExecutionCoordinator(schedule, WorkflowExecution(), null, null)

  // Mirrors what JumpToOperatorRegionHandler does: read the current schedule, find the level
  // containing the target operator, and replace the schedule with a copy seeded at that level.
  private def jumpTo(coordinator: WorkflowExecutionCoordinator, opName: String): Unit = {
    val schedule = coordinator.getSchedule
    schedule.getLevelIndexOfOperator(OperatorIdentity(opName)).foreach { levelIndex =>
      coordinator.replaceSchedule(schedule.copy(initialLevelIndex = levelIndex))
    }
  }

  "WorkflowExecutionCoordinator" should
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
    val rpcProbe = new ControllerRpcProbe(
      endWorkerResponse = call => if (call.receiver == firstWorkerId) None else Some(EmptyReturn())
    )
    val controller = createControllerHarness()
    registerLiveWorker(controller.actorRefService, firstWorkerId)
    registerLiveWorker(controller.actorRefService, secondWorkerId)

    val workflowCoordinator = new WorkflowExecutionCoordinator(
      Schedule(Map(0 -> Set(firstRegion), 1 -> Set(secondRegion))),
      workflowExecution,
      ControllerConfig(None, None, None, None),
      rpcProbe.asyncRPCClient
    )
    workflowCoordinator.setupActorRefService(controller.actorRefService)

    await(workflowCoordinator.coordinateRegionExecutors(controller.actorService))
    assert(rpcProbe.startedWorkers == Seq(firstWorkerId))

    val coordination = workflowCoordinator.coordinateRegionExecutors(controller.actorService)

    waitUntil(rpcProbe.endWorkerCalls.size == 1)
    assert(coordination.poll.isEmpty)
    assert(!rpcProbe.initializedWorkers.contains(secondWorkerId))
    assert(controller.actorRefService.hasActorRef(firstWorkerId))

    rpcProbe.fulfill(rpcProbe.onlyEndWorkerCall, EmptyReturn())
    await(coordination)

    assert(!controller.actorRefService.hasActorRef(firstWorkerId))
    assert(rpcProbe.initializedWorkers.contains(secondWorkerId))
    assert(rpcProbe.startedWorkers.contains(secondWorkerId))
  }

  "Jumping to an operator's region" should
    "make the next scheduled region contain the target operator's region" in {
    val (first, second, _, schedule) = threeLevelSchedule()
    val coordinator = newJumpCoordinator(schedule)

    assert(coordinator.getNextRegions == Set(first))
    assert(coordinator.getNextRegions == Set(second))

    jumpTo(coordinator, "first")

    assert(coordinator.getNextRegions == Set(first))
  }

  it should "support multiple sequential jumps interleaved with region pulls" in {
    val (first, second, third, schedule) = threeLevelSchedule()
    val coordinator = newJumpCoordinator(schedule)

    assert(coordinator.getNextRegions == Set(first))
    assert(coordinator.getNextRegions == Set(second))

    jumpTo(coordinator, "first")
    assert(coordinator.getNextRegions == Set(first))

    jumpTo(coordinator, "second")
    assert(coordinator.getNextRegions == Set(second))
    assert(coordinator.getNextRegions == Set(third))

    jumpTo(coordinator, "first")
    assert(coordinator.getNextRegions == Set(first))
  }

  it should "be a no-op when the target operator is not in any scheduled region" in {
    val (first, second, _, schedule) = threeLevelSchedule()
    val coordinator = newJumpCoordinator(schedule)

    assert(coordinator.getNextRegions == Set(first))

    jumpTo(coordinator, "does-not-exist")

    // Iteration position must be unaffected by an unknown target.
    assert(coordinator.getNextRegions == Set(second))
  }

  it should "leave the schedule untouched when called repeatedly with unknown operators" in {
    val (first, second, third, schedule) = threeLevelSchedule()
    val coordinator = newJumpCoordinator(schedule)

    jumpTo(coordinator, "ghost-1")
    jumpTo(coordinator, "ghost-2")
    jumpTo(coordinator, "ghost-3")

    assert(coordinator.getNextRegions == Set(first))
    assert(coordinator.getNextRegions == Set(second))
    assert(coordinator.getNextRegions == Set(third))
  }

  it should "allow jumping back to the first region after the schedule is exhausted" in {
    val (first, second, third, schedule) = threeLevelSchedule()
    val coordinator = newJumpCoordinator(schedule)

    assert(coordinator.getNextRegions == Set(first))
    assert(coordinator.getNextRegions == Set(second))
    assert(coordinator.getNextRegions == Set(third))
    assert(coordinator.getNextRegions == Set.empty)

    jumpTo(coordinator, "first")
    assert(coordinator.getNextRegions == Set(first))
  }

  it should "support jumping forward past regions that have not yet been pulled" in {
    val (first, _, third, schedule) = threeLevelSchedule()
    val coordinator = newJumpCoordinator(schedule)

    assert(coordinator.getNextRegions == Set(first))

    jumpTo(coordinator, "third")
    assert(coordinator.getNextRegions == Set(third))
    assert(coordinator.getNextRegions == Set.empty)
  }
}
