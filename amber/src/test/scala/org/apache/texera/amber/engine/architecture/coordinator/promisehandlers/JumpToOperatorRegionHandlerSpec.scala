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

import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{PhysicalOp, WorkflowContext}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorAsyncRPCHandlerInitializer,
  CoordinatorConfig,
  CoordinatorProcessor
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  JumpToOperatorRegionRequest
}
import org.apache.texera.amber.engine.architecture.scheduling.{Region, RegionIdentity, Schedule}
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.scalatest.flatspec.AnyFlatSpec

class JumpToOperatorRegionHandlerSpec extends AnyFlatSpec {

  // A single-operator Region, matching the construction pattern used by
  // WorkflowExecutionManagerSpec. The logical operator id (opId) is what the handler
  // matches against JumpToOperatorRegionRequest.targetOperatorId.
  private def singleOpRegion(regionId: Long, opId: String): Region = {
    val physicalOp = PhysicalOp(
      PhysicalOpIdentity(OperatorIdentity(opId), "main"),
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OpExecInitInfo.Empty
    )
    Region(RegionIdentity(regionId), Set(physicalOp), Set.empty)
  }

  // Build a real CoordinatorProcessor + our own initializer (the processor's own
  // initializer is private/inaccessible). Seed a 3-level, single-op-per-region schedule
  // directly on the WorkflowExecutionManager, which is what the handler reads/mutates.
  private def newHarness()
      : (CoordinatorAsyncRPCHandlerInitializer, CoordinatorProcessor, Region, Region, Region) = {
    val cp = new CoordinatorProcessor(
      new WorkflowContext(),
      CoordinatorConfig(None, None, None, None),
      COORDINATOR,
      _ => ()
    )
    val first = singleOpRegion(1, "first")
    val second = singleOpRegion(2, "second")
    val third = singleOpRegion(3, "third")
    cp.workflowExecutionManager.schedule = Schedule(
      Map(
        0 -> Set(first),
        1 -> Set(second),
        2 -> Set(third)
      )
    )
    val init = new CoordinatorAsyncRPCHandlerInitializer(cp)
    (init, cp, first, second, third)
  }

  private val ctx: AsyncRPCContext = AsyncRPCContext(COORDINATOR, COORDINATOR)

  "JumpToOperatorRegionHandler" should
    "reposition the schedule cursor to the level whose region contains the target operator" in {
    val (init, cp, first, second, _) = newHarness()

    // Consume levels 0 and 1, leaving the cursor at level 2 — natural progression would next
    // yield `third`.
    assert(cp.workflowExecutionManager.schedule.next() == Set(first))
    assert(cp.workflowExecutionManager.schedule.next() == Set(second))

    // Jump *back* to the region holding "first" (level 0). collectFirst finds level 0 and the
    // foreach rebuilds the schedule with initialLevelIndex = 0. Because level 0 is behind the
    // current cursor, the next pull yields `first` again instead of the `third` that natural
    // progression would give — proving the cursor was actually repositioned.
    init.jumpToOperatorRegion(JumpToOperatorRegionRequest(OperatorIdentity("first")), ctx)

    assert(cp.workflowExecutionManager.schedule.next() == Set(first))
  }

  it should "leave the schedule cursor untouched when the target operator is not scheduled" in {
    val (init, cp, first, second, _) = newHarness()

    // Pull the first region; the cursor now sits at level 1.
    assert(cp.workflowExecutionManager.schedule.next() == Set(first))

    // No level matches "does-not-exist": collectFirst is empty, the foreach is a no-op, and the
    // schedule (and its cursor) is left exactly where it was.
    init.jumpToOperatorRegion(JumpToOperatorRegionRequest(OperatorIdentity("does-not-exist")), ctx)

    assert(cp.workflowExecutionManager.schedule.next() == Set(second))
  }
}
