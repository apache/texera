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

import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PhysicalLink}
import org.jgrapht.graph.DirectedAcyclicGraph
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.mutable
import scala.jdk.CollectionConverters._

private[scheduling] case class LoopIteration(
    start: OperatorIdentity,
    ends: Set[RegionIdentity],
    members: Set[RegionIdentity]
)

case class RegionPlan(
    regions: Set[Region],
    regionLinks: Set[RegionLink]
) {

  @transient lazy val dag: DirectedAcyclicGraph[RegionIdentity, RegionLink] = {
    val jgraphtDag = new DirectedAcyclicGraph[RegionIdentity, RegionLink](classOf[RegionLink])
    regionMapping.keys.foreach(regionId => jgraphtDag.addVertex(regionId))
    regionLinks.foreach(l => jgraphtDag.addEdge(l.fromRegionId, l.toRegionId, l))
    jgraphtDag
  }
  @transient private lazy val regionMapping: Map[RegionIdentity, Region] =
    regions.map(region => region.id -> region).toMap

  def getRegionOfLink(link: PhysicalLink): Region = {
    regions.find(region => region.getLinks.contains(link)).get
  }

  def getRegionOfPortId(portId: GlobalPortIdentity): Option[Region] = {
    regions.find(region => region.getPorts.contains(portId))
  }

  // Loops use a fully materialized plan, with one operator per region. A side branch belongs to
  // the iteration even if it never rejoins Loop End. A join downstream of Loop End is outside.
  private[scheduling] lazy val loopIterations: List[LoopIteration] = {
    val starts = regions.flatMap(r =>
      r.getOperators.filter(_.isLoopStart).map(op => r.id -> op.id.logicalOpId)
    )
    if (starts.isEmpty) Nil
    else {
      val order = topologicalIterator().toVector
      val parents = regionLinks.groupMap(_.toRegionId)(_.fromRegionId)
      val ends = regions.filter(_.getOperators.exists(_.isLoopEnd)).map(_.id)
      val startIds = starts.map(_._1)
      val blocks = starts.toList.flatMap {
        case (start, opId) =>
          val depth = mutable.Map(start -> 1)
          val exits = mutable.Set.empty[RegionIdentity]
          order.dropWhile(_ != start).tail.foreach { id =>
            val incoming = parents.getOrElse(id, Set.empty).flatMap(depth.get)
            // Once a path has passed the matching end, its consumers stay outside, even if
            // another path reaches the same consumer directly from a body operator.
            if (incoming.contains(0)) depth(id) = 0
            else if (incoming.nonEmpty) {
              val delta = if (startIds(id)) 1 else if (ends(id)) -1 else 0
              depth(id) = incoming.max + delta
              if (depth(id) == 0) exits += id
            }
          }
          Option.when(exits.nonEmpty) {
            val members = depth.collect { case (id, d) if d > 0 => id }.toSet ++ exits
            LoopIteration(opId, exits.toSet, members)
          }
      }
      // A side path in an inner loop may join a consumer beyond the outer end. That consumer
      // is outside both iterations, even when it has no input from the inner end.
      val regionOfStart = starts.map(_.swap).toMap
      blocks
        .map { block =>
          val enclosing = blocks.filter(parent =>
            parent.start != block.start && parent.members(regionOfStart(block.start))
          )
          val members =
            enclosing.foldLeft(block.members)((inside, parent) => inside.intersect(parent.members))
          block.copy(members = members, ends = block.ends.intersect(members))
        }
        .filter(_.ends.nonEmpty)
    }
  }

  // These dependencies constrain dispatch only. Data and resource allocation use the original links.
  private[scheduling] def withLoopBarriers(loops: List[LoopIteration]): RegionPlan = {
    lazy val children =
      regionLinks.groupMap(_.fromRegionId)(_.toRegionId).withDefaultValue(Set.empty)
    val barriers = for {
      loop <- loops
      tail <- loop.members -- loop.ends
      if children(tail).intersect(loop.members).isEmpty
      end <- loop.ends
      consumer <- children(end)
    } yield RegionLink(tail, consumer)
    if (barriers.isEmpty) this else copy(regionLinks = regionLinks ++ barriers)
  }

  def topologicalIterator(): Iterator[RegionIdentity] = {
    new TopologicalOrderIterator(dag).asScala
  }

  def getRegion(regionId: RegionIdentity): Region = {
    regionMapping(regionId)
  }
}
