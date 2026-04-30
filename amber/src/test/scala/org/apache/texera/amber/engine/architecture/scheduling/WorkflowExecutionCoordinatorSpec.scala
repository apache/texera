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

import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.PhysicalOp
import org.apache.texera.amber.engine.architecture.controller.execution.WorkflowExecution
import org.scalatest.flatspec.AnyFlatSpec

class WorkflowExecutionCoordinatorSpec extends AnyFlatSpec {

  private def region(regionId: Long, opId: String): Region = {
    val physicalOp = PhysicalOp(
      PhysicalOpIdentity(OperatorIdentity(opId), "main"),
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OpExecInitInfo.Empty
    )
    Region(RegionIdentity(regionId), Set(physicalOp), Set.empty)
  }

  private def threeLevelSchedule(): (Region, Region, Region, Schedule) = {
    val first = region(1, "first")
    val second = region(2, "second")
    val third = region(3, "third")
    val schedule = Schedule(
      Map(
        0 -> Set(first),
        1 -> Set(second),
        2 -> Set(third)
      )
    )
    (first, second, third, schedule)
  }

  private def newCoordinator(schedule: Schedule): WorkflowExecutionCoordinator =
    new WorkflowExecutionCoordinator(schedule, WorkflowExecution(), null, null)

  "WorkflowExecutionCoordinator.jumpToRegionContainingOperator" should
    "make the next scheduled region contain the target operator's region" in {
    val (first, second, _, schedule) = threeLevelSchedule()
    val coordinator = newCoordinator(schedule)

    assert(coordinator.pullNextRegions == Set(first))
    assert(coordinator.pullNextRegions == Set(second))

    coordinator.jumpToRegionContainingOperator(OperatorIdentity("first"))

    assert(coordinator.pullNextRegions == Set(first))
  }

  it should "support multiple sequential jumps interleaved with region pulls" in {
    val (first, second, third, schedule) = threeLevelSchedule()
    val coordinator = newCoordinator(schedule)

    assert(coordinator.pullNextRegions == Set(first))
    assert(coordinator.pullNextRegions == Set(second))

    coordinator.jumpToRegionContainingOperator(OperatorIdentity("first"))
    assert(coordinator.pullNextRegions == Set(first))

    coordinator.jumpToRegionContainingOperator(OperatorIdentity("second"))
    assert(coordinator.pullNextRegions == Set(second))
    assert(coordinator.pullNextRegions == Set(third))

    coordinator.jumpToRegionContainingOperator(OperatorIdentity("first"))
    assert(coordinator.pullNextRegions == Set(first))
  }

  it should "be a no-op when the target operator is not in any scheduled region" in {
    val (first, second, _, schedule) = threeLevelSchedule()
    val coordinator = newCoordinator(schedule)

    assert(coordinator.pullNextRegions == Set(first))

    coordinator.jumpToRegionContainingOperator(OperatorIdentity("does-not-exist"))

    // Iteration position must be unaffected by an unknown target.
    assert(coordinator.pullNextRegions == Set(second))
  }

  it should "leave the schedule untouched when called repeatedly with unknown operators" in {
    val (first, second, third, schedule) = threeLevelSchedule()
    val coordinator = newCoordinator(schedule)

    coordinator.jumpToRegionContainingOperator(OperatorIdentity("ghost-1"))
    coordinator.jumpToRegionContainingOperator(OperatorIdentity("ghost-2"))
    coordinator.jumpToRegionContainingOperator(OperatorIdentity("ghost-3"))

    assert(coordinator.pullNextRegions == Set(first))
    assert(coordinator.pullNextRegions == Set(second))
    assert(coordinator.pullNextRegions == Set(third))
  }

  it should "allow jumping back to the first region after the schedule is exhausted" in {
    val (first, second, third, schedule) = threeLevelSchedule()
    val coordinator = newCoordinator(schedule)

    assert(coordinator.pullNextRegions == Set(first))
    assert(coordinator.pullNextRegions == Set(second))
    assert(coordinator.pullNextRegions == Set(third))
    assert(coordinator.pullNextRegions == Set.empty)

    coordinator.jumpToRegionContainingOperator(OperatorIdentity("first"))
    assert(coordinator.pullNextRegions == Set(first))
  }

  it should "support jumping forward past regions that have not yet been pulled" in {
    val (first, _, third, schedule) = threeLevelSchedule()
    val coordinator = newCoordinator(schedule)

    assert(coordinator.pullNextRegions == Set(first))

    coordinator.jumpToRegionContainingOperator(OperatorIdentity("third"))
    assert(coordinator.pullNextRegions == Set(third))
    assert(coordinator.pullNextRegions == Set.empty)
  }
}
