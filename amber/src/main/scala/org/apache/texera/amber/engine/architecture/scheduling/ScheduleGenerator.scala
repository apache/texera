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

import org.apache.texera.common.config.ApplicationConfig
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.engine.architecture.scheduling.resourcePolicies.{
  DefaultResourceAllocator,
  ExecutionClusterInfo
}

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

abstract class ScheduleGenerator(
    workflowContext: WorkflowContext,
    var physicalPlan: PhysicalPlan
) {
  private val executionClusterInfo = new ExecutionClusterInfo()
  val resourceAllocator =
    new DefaultResourceAllocator(
      physicalPlan,
      executionClusterInfo,
      workflowContext.workflowSettings,
      cuid = workflowContext.cuid
    )

  def generate(): (Schedule, PhysicalPlan)

  /**
    * A schedule is a ranking on the regions of a region plan. Skip regions (`Region.skipped`)
    * share level 0 so they complete together without worker allocation; active regions follow in
    * topological batches of up to ApplicationConfig.maxConcurrentRegions. When
    * maxConcurrentRegions == 1, the active levels form a total order.
    */
  def generateScheduleFromRegionPlan(regionPlan: RegionPlan): Schedule = {
    val skippedIds = regionPlan.regions.filter(_.skipped).map(_.id)
    require(
      regionPlan.regionLinks.forall(link =>
        !skippedIds.contains(link.fromRegionId) && !skippedIds.contains(link.toRegionId)
      ),
      "skip regions must not have region links"
    )

    val tmpLevelSets = mutable.Map.empty[Int, Set[RegionIdentity]]
    var level = 0
    if (skippedIds.nonEmpty) {
      tmpLevelSets(0) = skippedIds
      level = 1
    }

    val inDegree = mutable.Map.empty[RegionIdentity, Int]
    regionPlan
      .topologicalIterator()
      .filterNot(skippedIds.contains)
      .foreach { rid =>
        inDegree(rid) = regionPlan.dag.inDegreeOf(rid)
      }

    val readyRegionsQueue = mutable.Queue(
      inDegree.collect { case (rid, 0) => rid }.toSeq: _*
    )

    // While there are ready active regions:
    // 1. Dequeue up to maxConcurrentRegions regions to form the current batch.
    // 2. Record this batch under the current level.
    // 3. For each region in the batch:
    //      a. For each successor region, decrement its in-degree.
    //      b. If a successor's in-degree reaches zero, enqueue it to the readyRegionsQueue.
    // 4. Increment level and repeat.
    while (readyRegionsQueue.nonEmpty) {
      val batch = Set.newBuilder[RegionIdentity]
      var remaining = ApplicationConfig.maxConcurrentRegions
      while (remaining > 0 && readyRegionsQueue.nonEmpty) {
        batch += readyRegionsQueue.dequeue()
        remaining -= 1
      }
      val batchIds = batch.result()

      tmpLevelSets(level) = batchIds
      batchIds.foreach { rid =>
        regionPlan.dag
          .outgoingEdgesOf(rid)
          .asScala
          .map(edge => regionPlan.dag.getEdgeTarget(edge))
          .foreach { succ =>
            inDegree(succ) -= 1
            if (inDegree(succ) == 0) readyRegionsQueue.enqueue(succ)
          }
      }
      level += 1
    }
    val levelSets: Map[Int, Set[Region]] = tmpLevelSets.view.map {
      case (lvl, idSet) =>
        lvl -> idSet.iterator.map(regionPlan.getRegion).toSet
    }.toMap
    Schedule(levelSets)
  }
}
