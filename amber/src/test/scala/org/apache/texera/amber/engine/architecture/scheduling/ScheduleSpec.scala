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

}
