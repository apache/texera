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

import org.apache.texera.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import org.apache.texera.amber.engine.common.BarePhysicalPlanFixture
import org.apache.texera.common.config.ApplicationConfig
import org.scalatest.flatspec.AnyFlatSpec

class ScheduleSpec extends AnyFlatSpec with BarePhysicalPlanFixture {

  "Schedule.getRegions" should "return all regions across all levels" in {
    val r0 = region(0, "a")
    val r1a = region(1, "b")
    val r1b = region(2, "c")
    val schedule = Schedule(Map(0 -> Set(r0), 1 -> Set(r1a, r1b)))

    assert(schedule.getRegions.toSet == Set(r0, r1a, r1b))
  }

  it should "return an empty list when the schedule is empty" in {
    assert(Schedule(Map.empty).getRegions.isEmpty)
  }

  "Schedule" should "iterate level sets in ascending key order starting from the minimum level" in {
    val r0 = region(0, "a")
    val r1 = region(1, "b")
    val r2 = region(2, "c")
    val schedule = Schedule(Map(1 -> Set(r1), 0 -> Set(r0), 2 -> Set(r2)))

    assert(schedule.toList == List(Set(r0), Set(r1), Set(r2)))
  }

  it should "report hasNext as false for an empty schedule" in {
    assert(!Schedule(Map.empty).hasNext)
  }

  it should "report hasNext as false after the last contiguous level is consumed" in {
    val schedule = Schedule(Map(0 -> Set(region(0, "a")), 1 -> Set(region(1, "b"))))
    schedule.next()
    schedule.next()
    assert(!schedule.hasNext)
  }

  it should "reject construction when level keys contain a gap" in {
    assertThrows[IllegalArgumentException] {
      Schedule(Map(0 -> Set(region(0, "a")), 2 -> Set(region(2, "b"))))
    }
  }

  it should "reject construction when level keys do not start at zero" in {
    assertThrows[IllegalArgumentException] {
      Schedule(Map(3 -> Set(region(3, "a")), 4 -> Set(region(4, "b"))))
    }
  }

  "ScheduleGenerator.generateScheduleFromRegionPlan" should "return an empty schedule for an empty region plan" in {
    val generator = new ScheduleGenerator(
      new WorkflowContext(),
      PhysicalPlan(Set.empty, Set.empty)
    ) {
      override def generate(): (Schedule, PhysicalPlan) = (Schedule(Map.empty), physicalPlan)
    }

    val schedule = generator.generateScheduleFromRegionPlan(RegionPlan(Set.empty, Set.empty))
    assert(schedule.getRegions.isEmpty)
    assert(!schedule.hasNext)
  }

  it should "schedule a DAG of regions in topological batches capped by maxConcurrentRegions" in {
    val r0 = region(0, "a")
    val r1 = region(1, "b")
    val r2 = region(2, "c")
    val r3 = region(3, "d")
    val plan = RegionPlan(
      Set(r0, r1, r2, r3),
      Set(
        RegionLink(r0.id, r1.id),
        RegionLink(r0.id, r2.id),
        RegionLink(r1.id, r3.id),
        RegionLink(r2.id, r3.id)
      )
    )
    val generator = new ScheduleGenerator(
      new WorkflowContext(),
      PhysicalPlan(Set.empty, Set.empty)
    ) {
      override def generate(): (Schedule, PhysicalPlan) = (Schedule(Map.empty), physicalPlan)
    }

    val levels = generator.generateScheduleFromRegionPlan(plan).toList
    val order = levels.flatten
    assert(levels.forall(_.size <= ApplicationConfig.maxConcurrentRegions))
    assert(order.toSet == Set(r0, r1, r2, r3))
    assert(order.indexOf(r0) < order.indexOf(r1))
    assert(order.indexOf(r0) < order.indexOf(r2))
    assert(order.indexOf(r1) < order.indexOf(r3))
    assert(order.indexOf(r2) < order.indexOf(r3))
  }

  it should "drain an inner side branch before applying a pending outer repeat" in {
    val outer = region(0, "outer").copy(physicalOps = Set(op("outer").copy(isLoopStart = true)))
    val inner = region(1, "inner").copy(physicalOps = Set(op("inner").copy(isLoopStart = true)))
    val outerEnd =
      region(2, "outerEnd").copy(physicalOps = Set(op("outerEnd").copy(isLoopEnd = true)))
    val innerEnd =
      region(3, "innerEnd").copy(physicalOps = Set(op("innerEnd").copy(isLoopEnd = true)))
    val side = region(4, "side")
    val report = region(5, "report")
    val plan = RegionPlan(
      Set(outer, inner, outerEnd, innerEnd, side, report),
      Seq(
        outer -> outerEnd,
        outer -> inner,
        inner -> innerEnd,
        inner -> side,
        side -> report,
        outerEnd -> report
      ).map { case (from, to) => RegionLink(from.id, to.id) }.toSet
    )
    val members = plan.loopIterations.map(loop => loop.start -> loop.members).toMap
    val outId = outer.getOperators.head.id.logicalOpId
    val inId = inner.getOperators.head.id.logicalOpId
    assert(members(inId) == Set(inner.id, innerEnd.id, side.id))
    assert(members(outId) == Set(outer.id, outerEnd.id, inner.id, innerEnd.id, side.id))

    // Both ends can decide in parallel before the last side region finishes.
    val levels = List(Set(outer), Set(inner), Set(outerEnd, innerEnd), Set(side), Set(report))
    val schedule = Schedule(levels.indices.map(i => i -> levels(i)).toMap, loopMembers = members)
    assert(schedule.next() == Set(outer))
    assert(schedule.next() == Set(inner))
    assert(schedule.next() == Set(outerEnd, innerEnd))
    schedule.jumpTo(outId)
    schedule.jumpTo(inId)
    assert(schedule.toList == List(Set(side), Set(inner), Set(innerEnd), Set(side)) ++ levels)
  }

  it should "repeat at a terminal barrier when there is no downstream consumer" in {
    val start = region(0, "start")
    val end = region(1, "end")
    val sink = region(2, "sink")
    val target = start.getOperators.head.id.logicalOpId
    val batches = List(Set(start), Set(end), Set(sink))
    val schedule = Schedule(
      batches.indices.map(i => i -> batches(i)).toMap,
      loopMembers = Map(target -> Set(start.id, end.id, sink.id))
    )
    assert(schedule.next() == Set(start))
    assert(schedule.next() == Set(end))
    schedule.jumpTo(target)
    assert(schedule.next() == Set(sink))
    assert(schedule.hasNext)
    assert(schedule.toList == batches)
  }
}
