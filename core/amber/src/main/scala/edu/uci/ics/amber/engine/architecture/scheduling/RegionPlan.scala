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

import edu.uci.ics.amber.core.workflow.{GlobalPortIdentity, PhysicalLink}
import org.jgrapht.graph.DirectedAcyclicGraph
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.jdk.CollectionConverters.IteratorHasAsScala

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

  def topologicalIterator(): Iterator[RegionIdentity] = {
    new TopologicalOrderIterator(dag).asScala
  }

  def getRegion(regionId: RegionIdentity): Region = {
    regionMapping(regionId)
  }
}
