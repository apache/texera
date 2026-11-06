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

package org.apache.texera.amber.engine.architecture.coordinator

import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import org.apache.texera.amber.engine.architecture.scheduling.{
  CostBasedScheduleGenerator,
  Region,
  RunSkeleton,
  Schedule,
  SkeletonGenerator
}
import org.apache.texera.amber.engine.common.AmberLogging

class WorkflowScheduler(
    workflowContext: WorkflowContext,
    val actorId: ActorVirtualIdentity
) extends java.io.Serializable
    with AmberLogging {
  var physicalPlan: PhysicalPlan = _
  private var schedule: Schedule = _

  def getSchedule: Schedule = schedule

  /**
    * Update the schedule to be executed, based on the given physicalPlan.
    *
    * With no matched results on the context, the whole plan goes to the schedule generator.
    * Otherwise [[SkeletonGenerator]] marks which operators are skipped and which links read
    * saved results: the schedule generator plans the operators that run, without the links
    * that read saved results, and the skipped operators form skip regions in a level of their
    * own ahead of the generated levels. Either way `physicalPlan` keeps the full plan.
    */
  def updateSchedule(physicalPlan: PhysicalPlan): Unit = {
    if (workflowContext.matchedResults.isEmpty) {
      // generate a schedule using a region plan generator.
      val (generatedSchedule, updatedPhysicalPlan) =
        // CostBasedRegionPlanGenerator considers costs to try to find an optimal plan.
        new CostBasedScheduleGenerator(
          workflowContext,
          physicalPlan,
          actorId
        ).generate()
      this.schedule = generatedSchedule
      this.physicalPlan = updatedPhysicalPlan
    } else {
      val skeleton = SkeletonGenerator.generate(workflowContext, physicalPlan)
      skeleton.unusable.foreach {
        case (port, reason) => logger.warn(s"saved result for $port is unusable: $reason")
      }
      this.schedule = scheduleWith(skeleton, physicalPlan)
      this.physicalPlan = physicalPlan
    }
  }

  /**
    * The generated levels of the part of `physicalPlan` that runs, preceded by one level
    * holding every skip region when there is one. Skip region ids start above the largest
    * generated id. A run that skips every operator has nothing to generate and gets the skip
    * level only.
    */
  private def scheduleWith(skeleton: RunSkeleton, physicalPlan: PhysicalPlan): Schedule = {
    val retainedPart = skeleton.retainedPart(physicalPlan)
    val generatedLevels: Map[Int, Set[Region]] =
      if (retainedPart.operators.isEmpty) Map.empty
      else
        new CostBasedScheduleGenerator(
          workflowContext,
          retainedPart,
          actorId,
          skeleton.cacheReadInputs
        ).generate()._1.levelSets
    if (!skeleton.skipsAnything) {
      Schedule(generatedLevels)
    } else {
      val firstSkipRegionId =
        generatedLevels.values.flatten.map(_.id.id).maxOption.map(_ + 1).getOrElse(0L)
      Schedule(
        Map(0 -> skeleton.skipRegions(physicalPlan, firstSkipRegionId)) ++
          generatedLevels.map { case (level, regions) => (level + 1) -> regions }
      )
    }
  }

  def getNextRegions: Set[Region] = if (!schedule.hasNext) Set() else schedule.next()

  def hasPendingRegions: Boolean = schedule != null && schedule.hasNext

}
