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

import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorAsyncRPCHandlerInitializer,
  CoordinatorConfig,
  CoordinatorProcessor
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  JumpToOperatorRegionRequest
}
import org.apache.texera.amber.engine.architecture.scheduling.{
  Region,
  RegionLink,
  RegionPlan,
  Schedule,
  ScheduleGenerator
}
import org.apache.texera.amber.operator.loop.LoopEndOpDesc
import org.apache.texera.amber.engine.common.BarePhysicalPlanFixture
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.scalatest.flatspec.AnyFlatSpec

class JumpToOperatorRegionHandlerSpec extends AnyFlatSpec with BarePhysicalPlanFixture {

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
    val first = region(1, "first")
    val second = region(2, "second")
    val third = region(3, "third")
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

    // A jump to an ordinary region takes effect on the next dispatch.
    init.jumpToOperatorRegion(JumpToOperatorRegionRequest(OperatorIdentity("first")), ctx)

    assert(cp.workflowExecutionManager.schedule.next() == Set(first))
  }

  it should "leave the schedule cursor untouched when the target operator is not scheduled" in {
    val (init, cp, first, second, _) = newHarness()

    // Pull the first region; the cursor now sits at level 1.
    assert(cp.workflowExecutionManager.schedule.next() == Set(first))

    // An unknown target leaves the next dispatch unchanged.
    init.jumpToOperatorRegion(JumpToOperatorRegionRequest(OperatorIdentity("does-not-exist")), ctx)

    assert(cp.workflowExecutionManager.schedule.next() == Set(second))
  }
  it should "finish both side-sink writes before repeating or releasing the downstream report" in {
    val (handler, cp, _, _, _) = newHarness()
    val source = region(0, "source")
    val start = region(1, "start").copy(physicalOps = Set(op("start").withIsLoopStart(true)))
    val evaluate = region(2, "evaluate")
    val endOp = new LoopEndOpDesc().getPhysicalOp(op("end").workflowId, op("end").executionId)
    val end = region(3, "end").copy(physicalOps = Set(endOp))
    val sideA = region(4, "sideA")
    val sideB = region(5, "sideB")
    val sink = region(6, "sink")
    val report = region(7, "report")
    val independent = region(8, "independent")
    val regions = Set(source, start, evaluate, end, sideA, sideB, sink, report, independent)
    val links = Seq(
      source -> start,
      start -> evaluate,
      evaluate -> end,
      evaluate -> sideA,
      sideA -> sideB,
      sideB -> sink,
      end -> report,
      source -> independent
    ).map { case (from, to) => RegionLink(from.id, to.id) }.toSet
    val generator =
      new ScheduleGenerator(new WorkflowContext(), PhysicalPlan(Set.empty, Set.empty)) {
        override def generate(): (Schedule, PhysicalPlan) =
          (generateScheduleFromRegionPlan(RegionPlan(regions, links)), physicalPlan)
      }
    cp.workflowExecutionManager.schedule = generator.generate()._1
    def schedule: Schedule = cp.workflowExecutionManager.schedule
    val batches = List.newBuilder[Set[Region]]
    var iterations = 0
    while (schedule.hasNext) {
      val batch = schedule.next()
      batches += batch
      if (batch.contains(end)) {
        iterations += 1
        if (iterations == 1)
          handler.jumpToOperatorRegion(JumpToOperatorRegionRequest(OperatorIdentity("start")), ctx)
      }
    }
    val observed = batches.result()
    def positions(r: Region): List[Int] =
      observed.indices.filter(i => observed(i).contains(r)).toList
    assert(positions(start).size == 2 && positions(sink).size == 2 && positions(end).size == 2)
    assert(positions(sink).head < positions(start).last)
    assert(positions(sink).last < positions(report).head)
    Seq(source, independent, report).foreach(r => assert(positions(r).size == 1))
  }

}
