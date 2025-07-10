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

package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies.{
  DefaultResourceAllocator,
  ExecutionClusterInfo
}

abstract class ScheduleGenerator(
    workflowContext: WorkflowContext,
    var physicalPlan: PhysicalPlan
) {
  private val executionClusterInfo = new ExecutionClusterInfo()
  val resourceAllocator =
    new DefaultResourceAllocator(
      physicalPlan,
      executionClusterInfo,
      workflowContext.workflowSettings
    )

  def generate(): (Schedule, PhysicalPlan)

  /**
    * A schedule is a ranking on the regions of a region plan. Currently we use a total order of the regions.
    */
  def generateScheduleFromRegionPlan(regionPlan: RegionPlan): Schedule = {
    val levelSets = regionPlan
      .topologicalIterator()
      .zipWithIndex
      .map(zippedRegionId => {
        zippedRegionId._2 -> Set.apply(regionPlan.getRegion(zippedRegionId._1))
      })
      .toMap
    Schedule.apply(levelSets)
  }
}
