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

import edu.uci.ics.amber.config.ApplicationConfig
import edu.uci.ics.amber.core.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.engine.architecture.scheduling.ScheduleGenerator.replaceVertex
import edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies.{
  DefaultResourceAllocator,
  ExecutionClusterInfo
}
import org.jgrapht.graph.DirectedAcyclicGraph
import org.jgrapht.traverse.TopologicalOrderIterator
import scala.collection.mutable
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IteratorHasAsScala}

object ScheduleGenerator {
  def replaceVertex(
      graph: DirectedAcyclicGraph[Region, RegionLink],
      oldVertex: Region,
      newVertex: Region
  ): Unit = {
    if (oldVertex.equals(newVertex)) {
      return
    }
    graph.addVertex(newVertex)
    graph
      .outgoingEdgesOf(oldVertex)
      .asScala
      .toList
      .foreach(oldEdge => {
        val dest = graph.getEdgeTarget(oldEdge)
        graph.removeEdge(oldEdge)
        graph.addEdge(newVertex, dest, RegionLink(newVertex.id, dest.id))
      })
    graph
      .incomingEdgesOf(oldVertex)
      .asScala
      .toList
      .foreach(oldEdge => {
        val source = graph.getEdgeSource(oldEdge)
        graph.removeEdge(oldEdge)
        graph.addEdge(source, newVertex, RegionLink(source.id, newVertex.id))
      })
    graph.removeVertex(oldVertex)
  }
}

abstract class ScheduleGenerator(
    workflowContext: WorkflowContext,
    var physicalPlan: PhysicalPlan
) {
  private val executionClusterInfo = new ExecutionClusterInfo()

  def generate(): (Schedule, PhysicalPlan)

  /**
    * A schedule is a ranking on the regions of a region plan.
    * Regions are dispatched in batches of up to AmberConfig.maxConcurrentRegions, respecting the DAG dependencies.
    * When maxConcurrentRegions == 1, this is equivalent to a total order (fully sequential execution).
    */
  def generateScheduleFromRegionPlan(regionPlan: RegionPlan): Schedule = {
    val inDegree = mutable.Map.empty[RegionIdentity, Int]
    regionPlan.topologicalIterator().foreach { rid =>
      inDegree(rid) = regionPlan.dag.incomingEdgesOf(rid).asScala.size
    }

    val readyRegionsQueue = mutable.Queue(
      inDegree.collect { case (rid, 0) => rid }.toSeq: _*
    )

    val tmpLevelSets = mutable.Map.empty[Int, Set[RegionIdentity]]
    var level = 0

    // While there are ready regions:
    // 1. Dequeue up to maxConcurrentRegions regions to form the current batch.
    // 2. Record this batch under the current level.
    // 3. For each region in the batch:
    //      a. For each successor region, decrement its in-degree.
    //      b. If a successor's in-degree reaches zero, enqueue it to the readyRegionsQueue.
    // 4. Increment level and repeat.
    while (readyRegionsQueue.nonEmpty) {
      val batchIds = (1 to ApplicationConfig.maxConcurrentRegions).flatMap { _ =>
        if (readyRegionsQueue.nonEmpty) Some(readyRegionsQueue.dequeue()) else None
      }.toSet

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

  def allocateResource(
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Unit = {

    val resourceAllocator =
      new DefaultResourceAllocator(
        physicalPlan,
        executionClusterInfo,
        workflowContext.workflowSettings
      )
    // generate the resource configs
    new TopologicalOrderIterator(regionDAG).asScala
      .foreach(region => {
        val (newRegion, _) = resourceAllocator.allocate(region)
        replaceVertex(regionDAG, region, newRegion)
      })
  }

  def getRegions(
      physicalOpId: PhysicalOpIdentity,
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Set[Region] = {
    regionDAG
      .vertexSet()
      .asScala
      .filter(region => region.getOperators.map(_.id).contains(physicalOpId))
      .toSet
  }

  /**
    * For a dependee input link, although it connects two regions A->B, we include this link and its toOp in region A
    * so that the dependee link will be completed first.
    */
  def populateDependeeLinks(
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Unit = {

    val dependeeLinks = physicalPlan
      .topologicalIterator()
      .flatMap { physicalOpId =>
        val upstreamPhysicalOpIds = physicalPlan.getUpstreamPhysicalOpIds(physicalOpId)
        upstreamPhysicalOpIds.flatMap { upstreamPhysicalOpId =>
          physicalPlan
            .getLinksBetween(upstreamPhysicalOpId, physicalOpId)
            .filter(link =>
              physicalPlan
                .getOperator(physicalOpId)
                .isInputLinkDependee(link)
            )
        }
      }
      .toSet

    dependeeLinks
      .flatMap { link => getRegions(link.fromOpId, regionDAG).map(region => region -> link) }
      .groupBy(_._1)
      .view
      .mapValues(_.map(_._2))
      .foreach {
        case (region, links) =>
          val newRegion = region.copy(
            physicalLinks = region.physicalLinks ++ links,
            physicalOps =
              region.getOperators ++ links.map(_.toOpId).map(id => physicalPlan.getOperator(id)),
            ports = region.getPorts ++ links.map(dependeeLink =>
              GlobalPortIdentity(dependeeLink.toOpId, dependeeLink.toPortId, input = true)
            )
          )
          replaceVertex(regionDAG, region, newRegion)
      }
  }
}
