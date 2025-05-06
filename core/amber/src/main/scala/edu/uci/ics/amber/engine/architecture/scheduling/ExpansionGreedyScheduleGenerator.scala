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

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.WorkflowRuntimeException
import edu.uci.ics.amber.core.storage.VFSURIFactory.createResultURI
import edu.uci.ics.amber.core.workflow.{
  GlobalPortIdentity,
  PhysicalLink,
  PhysicalPlan,
  WorkflowContext
}
import edu.uci.ics.amber.core.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.engine.architecture.scheduling.ScheduleGenerator.replaceVertex
import edu.uci.ics.amber.engine.architecture.scheduling.config.{PortConfig, ResourceConfig}
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.graph.DirectedAcyclicGraph

import java.net.URI
import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

class ExpansionGreedyScheduleGenerator(
    workflowContext: WorkflowContext,
    initialPhysicalPlan: PhysicalPlan
) extends ScheduleGenerator(workflowContext, initialPhysicalPlan)
    with LazyLogging {
  def generate(): (Schedule, PhysicalPlan) = {

    val regionDAG = createRegionDAG()
    val regionPlan = RegionPlan(
      regions = regionDAG.vertexSet().asScala.toSet,
      regionLinks = regionDAG.edgeSet().asScala.toSet
    )
    val schedule = generateScheduleFromRegionPlan(regionPlan)

    (
      schedule,
      physicalPlan
    )
  }

  /**
    * Takes in a pair of operatorIds, `upstreamOpId` and `downstreamOpId`, finds all regions they each
    * belong to, and creates the order relationships between the Regions of upstreamOpId, with the Regions
    * of downstreamOpId. The relation ship can be N to M.
    *
    * This method does not consider ports.
    *
    * Returns pairs of (upstreamRegion, downstreamRegion) indicating the order from
    * upstreamRegion to downstreamRegion.
    */
  private def toRegionOrderPairs(
      upstreamOpId: PhysicalOpIdentity,
      downstreamOpId: PhysicalOpIdentity,
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Set[(Region, Region)] = {

    val upstreamRegions = getRegions(upstreamOpId, regionDAG)
    val downstreamRegions = getRegions(downstreamOpId, regionDAG)

    upstreamRegions.flatMap { upstreamRegion =>
      downstreamRegions
        .filterNot(regionDAG.getDescendants(upstreamRegion).contains(_))
        .map(downstreamRegion => (upstreamRegion, downstreamRegion))
    }
  }

  /**
    * Create Regions based on the PhysicalPlan. The Region are to be added to regionDAG separately.
    */
  private def createRegions(physicalPlan: PhysicalPlan): Set[Region] = {
    val dependeeLinksRemovedDAG = physicalPlan.getDependeeLinksRemovedDAG
    val connectedComponents = new BiconnectivityInspector[PhysicalOpIdentity, PhysicalLink](
      dependeeLinksRemovedDAG.dag
    ).getConnectedComponents.asScala.toSet
    connectedComponents.zipWithIndex.map {
      case (connectedSubDAG, idx) =>
        val operatorIds = connectedSubDAG.vertexSet().asScala.toSet
        val links = operatorIds
          .flatMap(operatorId => {
            physicalPlan.getUpstreamPhysicalLinks(operatorId) ++ physicalPlan
              .getDownstreamPhysicalLinks(operatorId)
          })
          .filter(link => operatorIds.contains(link.fromOpId))
        val operators = operatorIds.map(operatorId => physicalPlan.getOperator(operatorId))
        val ports = operators.flatMap(op =>
          op.inputPorts.keys
            .map(inputPortId => GlobalPortIdentity(op.id, inputPortId, input = true))
            .toSet ++ op.outputPorts.keys
            .map(outputPortId => GlobalPortIdentity(op.id, outputPortId))
            .toSet
        )
        Region(
          id = RegionIdentity(idx),
          physicalOps = operators,
          physicalLinks = links,
          ports = ports
        )
    }
  }

  /**
    * Try connect the regions in the DAG while respecting the dependencies of PhysicalLinks (e.g., HashJoin).
    * This function returns either a successful connected region DAG, or a list of PhysicalLinks that should be
    * replaced for materialization.
    *
    * This function builds a region DAG from scratch. It first adds all the regions into the DAG. Then it starts adding
    * edges on the DAG. To do so, it examines each PhysicalOp and checks its input links. The links will be problematic
    * if the link's toOp (this PhysicalOp) has another link that has higher priority to run than this link (i.e., it has
    * a dependency). If such links are found, the function will terminate after this PhysicalOp and return the set of
    * links.
    *
    * If the function finds no such links for all PhysicalOps, it will return the connected Region DAG.
    *
    * @return Either a partially connected region DAG, or a set of PhysicalLinks for materialization replacement.
    */
  private def tryConnectRegionDAG()
      : Either[DirectedAcyclicGraph[Region, RegionLink], Set[PhysicalLink]] = {

    // creates an empty regionDAG
    val regionDAG = new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink])

    // add Regions as vertices
    createRegions(physicalPlan).foreach(region => regionDAG.addVertex(region))

    // add regionLinks as edges, if failed, return the problematic PhysicalLinks.
    physicalPlan
      .topologicalIterator()
      .foreach(physicalOpId => {
        handleInputPortDependencies(physicalOpId, regionDAG)
          .map(links => return Right(links))
      })

    // if success, a partially connected region DAG without edges between materialization operators is returned.
    // The edges between materialization are to be added later.
    Left(regionDAG)
  }

  private def handleInputPortDependencies(
      physicalOpId: PhysicalOpIdentity,
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Option[Set[PhysicalLink]] = {
    // for operators like HashJoin that have an order among their blocking and pipelined inputs
    physicalPlan
      .getOperator(physicalOpId)
      .getInputPortsInProcessingOrder
      .sliding(2, 1)
      .foreach {
        case List(prevPort, nextPort) =>
          // Create edges between regions
          val prevLinks =
            physicalPlan.getUpstreamPhysicalLinks(physicalOpId).filter(l => l.toPortId == prevPort)
          val nextLinks =
            physicalPlan.getUpstreamPhysicalLinks(physicalOpId).filter(l => l.toPortId == nextPort)
          if (nextLinks.nonEmpty) {
            val regionOrderPairs =
              toRegionOrderPairs(prevLinks.head.fromOpId, nextLinks.head.fromOpId, regionDAG)
            // Attempt to add edges to regionDAG
            try {
              regionOrderPairs.foreach {
                case (fromRegion, toRegion) =>
                  regionDAG.addEdge(fromRegion, toRegion, RegionLink(fromRegion.id, toRegion.id))
              }
            } catch {
              case _: IllegalArgumentException =>
                // adding the edge causes cycle. return the link for materialization replacement
                return Some(Set(nextLinks.head))
            }
          } else {
            try {
              // Use port to find regions
              val fromRegions = getRegions(prevLinks.head.fromOpId, regionDAG)
              val toRegion = getRegions(physicalOpId, regionDAG)
                .filter(region =>
                  region.getPorts.contains(
                    GlobalPortIdentity(
                      opId = physicalOpId,
                      portId = nextPort,
                      input = true
                    )
                  )
                )
                .head
              fromRegions.foreach(fromRegion =>
                regionDAG.addEdge(fromRegion, toRegion, RegionLink(fromRegion.id, toRegion.id))
              )
            } catch {
              case _: IllegalArgumentException =>
                // a cycle is detected. it should not reach here.
                throw new WorkflowRuntimeException(
                  "Cyclic dependency when trying to handle dependent ports in building a region plan"
                )
            }
          }
        case _ =>
      }
    None
  }

  /**
    * @param matReaderWriterPairs
    * @param regionDAG
    */
  private def assignPortConfigs(
      matReaderWriterPairs: Set[(GlobalPortIdentity, GlobalPortIdentity)],
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Unit = {

    val outputPortsToMaterialize = matReaderWriterPairs.map(_._1)

    (outputPortsToMaterialize ++ workflowContext.workflowSettings.outputPortsNeedingStorage)
      .foreach(outputPortId => {
        getRegions(outputPortId.opId, regionDAG).foreach(fromRegion => {
          val portConfigToAdd = outputPortId -> {
            val uriToAdd = getStorageURIFromGlobalOutputPortId(outputPortId)
            PortConfig(storageURIs = List(uriToAdd))
          }
          val newResourceConfig = fromRegion.resourceConfig match {
            case Some(existingConfig) =>
              existingConfig.copy(portConfigs = existingConfig.portConfigs + portConfigToAdd)
            case None => ResourceConfig(portConfigs = Map(portConfigToAdd))
          }
          val newFromRegion = fromRegion.copy(resourceConfig = Some(newResourceConfig))
          replaceVertex(regionDAG, fromRegion, newFromRegion)
        })
      })

    matReaderWriterPairs
      // group all pairs by the input port (_2)
      .groupBy { case (_, inputPort) => inputPort }
      // for each input port, build its PortConfig based on all its upstream output ports
      .foreach {
        case (inputPort, pairsForThisInput) =>
          // extract all the output ports paired with this input
          val urisToAdd: List[URI] = pairsForThisInput.map {
            case (outputPort, _) => getStorageURIFromGlobalOutputPortId(outputPort)
          }.toList

          val portConfigToAdd = inputPort -> PortConfig(storageURIs = urisToAdd)

          getRegions(inputPort.opId, regionDAG).foreach(toRegion => {
            val newResourceConfig = toRegion.resourceConfig match {
              case Some(existingConfig) =>
                existingConfig.copy(portConfigs = existingConfig.portConfigs + portConfigToAdd)
              case None => ResourceConfig(portConfigs = Map(portConfigToAdd))
            }
            val newToRegion = toRegion.copy(resourceConfig = Some(newResourceConfig))
            replaceVertex(regionDAG, toRegion, newToRegion)
          })
      }
  }

  private def getStorageURIFromGlobalOutputPortId(outputPortId: GlobalPortIdentity) = {
    assert(!outputPortId.input)
    createResultURI(
      workflowId = workflowContext.workflowId,
      executionId = workflowContext.executionId,
      globalPortId = outputPortId
    )
  }

  /**
    * This function creates and connects a region DAG while conducting materialization replacement.
    * It keeps attempting to create a region DAG from the given PhysicalPlan. When failed, a list
    * of PhysicalLinks that causes the failure will be given to conduct materialization replacement,
    * which changes the PhysicalPlan. It keeps attempting with the updated PhysicalPLan until a
    * region DAG is built after connecting materialized pairs.
    *
    * @return a fully connected region DAG.
    */
  private def createRegionDAG(): DirectedAcyclicGraph[Region, RegionLink] = {

    val matReaderWriterPairs =
      new mutable.HashSet[(GlobalPortIdentity, GlobalPortIdentity)]()

    val outputPortsToMaterialize = new mutable.HashSet[GlobalPortIdentity]()

    @tailrec
    def recConnectRegionDAG(): DirectedAcyclicGraph[Region, RegionLink] = {
      tryConnectRegionDAG() match {
        case Left(dag) => dag
        case Right(links) =>
          links.foreach { link =>
            physicalPlan = replaceLinkWithMaterialization(
              link,
              matReaderWriterPairs
            )
            outputPortsToMaterialize += GlobalPortIdentity(
              opId = link.fromOpId,
              portId = link.fromPortId
            )
          }
          recConnectRegionDAG()
      }
    }

    // the region is partially connected successfully.
    val regionDAG: DirectedAcyclicGraph[Region, RegionLink] = recConnectRegionDAG()

    // try to add dependencies between materialization writer and reader regions
    try {
      matReaderWriterPairs.foreach {
        case (upstreamOutputPort, downstreamInputPort) =>
          toRegionOrderPairs(upstreamOutputPort.opId, downstreamInputPort.opId, regionDAG).foreach {
            case (fromRegion, toRegion) =>
              regionDAG.addEdge(fromRegion, toRegion, RegionLink(fromRegion.id, toRegion.id))
          }
      }
    } catch {
      case _: IllegalArgumentException =>
        // a cycle is detected. it should not reach here.
        throw new WorkflowRuntimeException(
          "Cyclic dependency between regions detected"
        )
    }

    assignPortConfigs(matReaderWriterPairs.toSet, regionDAG)

    // mark links that go to downstream regions
    populateDependeeLinks(regionDAG)

    // allocate resources on regions
    allocateResource(regionDAG)

    regionDAG
  }
}
