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

import edu.uci.ics.amber.core.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.core.workflow.{GlobalPortIdentity, PhysicalLink, PhysicalOp}
import edu.uci.ics.amber.engine.architecture.scheduling.config.ResourceConfig
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.jdk.CollectionConverters.IteratorHasAsScala

case class RegionLink(fromRegionId: RegionIdentity, toRegionId: RegionIdentity)

case class RegionIdentity(id: Long)
case class Region(
    id: RegionIdentity,
    physicalOps: Set[PhysicalOp],
    physicalLinks: Set[PhysicalLink],
    ports: Set[GlobalPortIdentity] = Set.empty,
    resourceConfig: Option[ResourceConfig] = None
) {

  private val operators: Map[PhysicalOpIdentity, PhysicalOp] =
    getOperators.map(op => op.id -> op).toMap

  @transient lazy val dag: DirectedAcyclicGraph[PhysicalOpIdentity, DefaultEdge] = {
    val jgraphtDag = new DirectedAcyclicGraph[PhysicalOpIdentity, DefaultEdge](classOf[DefaultEdge])
    getOperators.foreach(op => jgraphtDag.addVertex(op.id))
    getLinks.foreach(link => jgraphtDag.addEdge(link.fromOpId, link.toOpId))
    jgraphtDag
  }

  def topologicalIterator(): Iterator[PhysicalOpIdentity] = {
    new TopologicalOrderIterator(dag).asScala
  }

  def getOperators: Set[PhysicalOp] = physicalOps

  def getLinks: Set[PhysicalLink] = physicalLinks

  /**
    * Ideally ports should be derived from operators. However, as we are including an operator with a dependee input
    * link in the previous region, such operator's other ports should not belong to the previous region. As a result
    * ports of a regioin are saved separately.
    * TODO: Improve this design once we have clean separation of regions.
    */
  def getPorts: Set[GlobalPortIdentity] = ports

  def getOperator(physicalOpId: PhysicalOpIdentity): PhysicalOp = {
    operators(physicalOpId)
  }

  /**
    * Effective source operators in a region.
    * The effective source contains operators that have 0 input links in this region.
    */
  def getSourceOperators: Set[PhysicalOp] = {
    getOperators
      .filter(physicalOp =>
        physicalOp
          .getInputLinks()
          .map(link => link.fromOpId)
          .forall(upstreamOpId => !getOperators.map(_.id).contains(upstreamOpId))
      )

  }

  /**
    * Operators that should be started first. An operator need to start first either because it is a source operator,
    * or because it has an input port that needs to read from materialization.
    */
  def getStarterOperators: Set[PhysicalOp] = {
    val opsReadingFromMaterialization = resourceConfig match {
      case Some(config) =>
        config.portConfigs
          .filter {
            case (globalPortId, config) =>
              globalPortId.input && config.storageURIs.nonEmpty
          }
          .map {
            case (globalPortId, _) => globalPortId.opId
          }
          .toSet
          .map(opId => getOperator(opId))
      case None => Set.empty
    }
    opsReadingFromMaterialization ++ getSourceOperators
  }

}
