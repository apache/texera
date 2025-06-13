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
import edu.uci.ics.amber.core.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.core.workflow.{
  GlobalPortIdentity,
  PhysicalLink,
  PhysicalPlan,
  WorkflowContext
}
import edu.uci.ics.amber.engine.architecture.scheduling.ScheduleGenerator.replaceVertex
import edu.uci.ics.amber.engine.architecture.scheduling.config.{
  IntermediateInputPortConfig,
  OutputPortConfig,
  ResourceConfig
}
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
          .diff(physicalPlan.getDependeeLinks) // dependee links should not belong to a region.
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

  /**
    * A dependee input port is one that is depended on by another input port of the same operator.
    * The incoming edge of a dependee input port is called a dependee edge.
    * Similarly, the other port of this dependency relationship is called a depender input port and connects
    * to a depender edge.
    *
    * Core design: a dependee edge needs to be materialized, and is mapped to a region edge in the region DAG.
    * Note: currently we assume there CANNOT be dependencies between two dependee input ports.
    * This method reasons about the input port dependencies of a given operator during the greedy expansion-based
    * construction of a region DAG.
    *
    * This method first reasons about the dependencies of the input ports of the given operator to find
    * pairs of dependency relationships, and then enforces the dependency of each pair:
    * All the incoming edges of a dependee port will be added to the partial region DAG as a region edge.
    * If adding a dependee edge results in a cycle that breaks the region DAG, we use a heurestic which is to
    * return the other depender edge and indicate that this depender edge needs to be materialized. This will
    * break the cycle and maintain the acyclicity of the region DAG.
    *
    * Previously we relied purely on edges and cache read operators for implementing materializations for
    * materialized edges and find regions in this method.
    *
    * After introducing materailizations on output and input ports, materializing an
    * edge could result in an operator that does not have any edges connected to one or more of its input
    * ports (i.e., it becomes a "starter" operator in a region). For such input ports, we can only use port to
    * find regions.
    *
    * @param physicalOpId The id of the input physical operator on which we need to handle input port dependies.
    * @param regionDAG The partial region DAG that is always acyclic.
    * @return Optionally a set of [[PhysicalLink]]s to do materialization-replacements on.
    */
  private def handleInputPortDependencies(
      physicalOpId: PhysicalOpIdentity,
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Option[Set[PhysicalLink]] = {
    // For operators like HashJoin's Probe that have dependencies between their input ports
    physicalPlan
      .getOperator(physicalOpId)
      .getInputPortDependencyPairs
      .sliding(2, 1)
      .foreach {
        case List(dependeePort, dependerPort) =>
          // Create edges between regions
          val dependeeEdges =
            physicalPlan
              .getUpstreamPhysicalLinks(physicalOpId)
              .filter(l => l.toPortId == dependeePort)
          val dependerEdges =
            physicalPlan
              .getUpstreamPhysicalLinks(physicalOpId)
              .filter(l => l.toPortId == dependerPort)

          if (dependerEdges.nonEmpty) {
            // The depender port is connected to some edges of this same region
            val regionOrderPairs =
              toRegionOrderPairs(
                dependeeEdges.head.fromOpId,
                dependerEdges.head.fromOpId,
                regionDAG
              )
            // Attempt to add these depender edges to regionDAG
            try {
              regionOrderPairs.foreach {
                case (dependeeRegion, dependerRegion) =>
                  regionDAG.addEdge(
                    dependeeRegion,
                    dependerRegion,
                    RegionLink(dependeeRegion.id, dependerRegion.id)
                  )
              }
            } catch {
              case _: IllegalArgumentException =>
                // Adding the depender edge causes cycle. return the edge for materialization replacement
                return Some(Set(dependerEdges.head))
            }
          } else {
            // The depender port is not connected to any edges (due to materializations)
            try {
              // Any region that the dependee port belongs to needs to run first.
              val dependeeRegions = getRegions(dependeeEdges.head.fromOpId, regionDAG)
              // Any region that this depender port belongs to need to run after those dependee regions.
              val dependerRegion = getRegions(physicalOpId, regionDAG)
                .filter(region =>
                  region.getPorts.contains(
                    GlobalPortIdentity(
                      opId = physicalOpId,
                      portId = dependerPort,
                      input = true
                    )
                  )
                )
                .head
              // We can safely add region edges created from this dependency relationship and it should
              // never cause cycles (since the edges of this depender port are already "cut").
              dependeeRegions.foreach(fromRegion =>
                regionDAG
                  .addEdge(fromRegion, dependerRegion, RegionLink(fromRegion.id, dependerRegion.id))
              )
            } catch {
              case _: IllegalArgumentException =>
                // A cycle is detected. This logic should never be reached.
                throw new WorkflowRuntimeException(
                  "Cyclic dependency when trying to handle input port dependencies in building a region plan"
                )
            }
          }
        case _ =>
      }
    None
  }

  /**
    * Create `PortConfig`s containing only `URI`s for both input and output ports. For the greedy scheduler, this step
    * after a region DAG is created.
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
            OutputPortConfig(uriToAdd)
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
      // Group all pairs by the input port (_2)
      .groupBy { case (_, inputPort) => inputPort }
      // For each input port, build its PortConfig based on all its upstream output ports
      .foreach {
        case (inputPort, pairsForThisInput) =>
          // Extract all the output ports paired with this input
          val urisToAdd: List[URI] = pairsForThisInput.map {
            case (outputPort, _) => getStorageURIFromGlobalOutputPortId(outputPort)
          }.toList

          val portConfigToAdd =
            inputPort -> IntermediateInputPortConfig(urisToAdd)

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

  private def replaceLinkWithMaterialization(
      physicalLink: PhysicalLink,
      writerReaderPairs: mutable.Set[(GlobalPortIdentity, GlobalPortIdentity)]
  ): PhysicalPlan = {
    val outputGlobalPortId = GlobalPortIdentity(
      physicalLink.fromOpId,
      physicalLink.fromPortId
    )

    val inputGlobalPortId = GlobalPortIdentity(
      physicalLink.toOpId,
      physicalLink.toPortId,
      input = true
    )

    val pair = (outputGlobalPortId, inputGlobalPortId)

    writerReaderPairs += pair

    val newPhysicalPlan = physicalPlan
      .removeLink(physicalLink)
    newPhysicalPlan
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

    val materializedOutputInputPortPairs =
      new mutable.HashSet[(GlobalPortIdentity, GlobalPortIdentity)]()

    @tailrec
    def recConnectRegionDAG(): DirectedAcyclicGraph[Region, RegionLink] = {
      tryConnectRegionDAG() match {
        case Left(dag) => dag
        case Right(links) =>
          links.foreach { link =>
            physicalPlan = replaceLinkWithMaterialization(
              link,
              materializedOutputInputPortPairs
            )
          }
          recConnectRegionDAG()
      }
    }

    // the region is partially connected successfully.
    val regionDAG: DirectedAcyclicGraph[Region, RegionLink] = recConnectRegionDAG()

    // also need to materialize all the dependee links.
    physicalPlan.getDependeeLinks.foreach(link => {
      physicalPlan = replaceLinkWithMaterialization(link, materializedOutputInputPortPairs)
    })

    // try to add dependencies between materialization writer and reader regions
    try {
      materializedOutputInputPortPairs.foreach {
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

    assignPortConfigs(materializedOutputInputPortPairs.toSet, regionDAG)

    // mark links that go to downstream regions
    populateDependeeLinks(regionDAG)

    // allocate resources on regions
    allocateResource(regionDAG)

    regionDAG
  }
}
