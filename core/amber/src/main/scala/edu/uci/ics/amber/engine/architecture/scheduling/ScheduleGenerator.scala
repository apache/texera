package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.core.storage.result.{OpResultStorage, ResultStorage}
import edu.uci.ics.amber.core.workflow.{PhysicalOp, PhysicalPlan, WorkflowContext}
import edu.uci.ics.amber.engine.architecture.scheduling.ScheduleGenerator.replaceVertex
import edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies.{
  DefaultResourceAllocator,
  ExecutionClusterInfo
}
import edu.uci.ics.amber.operator.SpecialPhysicalOpFactory
import edu.uci.ics.amber.core.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.core.workflow.PhysicalLink
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

  def allocateResource(
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Unit = {
    val dataTransferBatchSize = workflowContext.workflowSettings.dataTransferBatchSize

    val resourceAllocator =
      new DefaultResourceAllocator(physicalPlan, executionClusterInfo, dataTransferBatchSize)
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
              !physicalPlan.getOperator(physicalOpId).isSinkOperator && physicalPlan
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
              region.getOperators ++ links.map(_.toOpId).map(id => physicalPlan.getOperator(id))
          )
          replaceVertex(regionDAG, region, newRegion)
      }
  }

  def replaceLinkWithMaterialization(
      physicalLink: PhysicalLink,
      writerReaderPairs: mutable.HashMap[PhysicalOpIdentity, PhysicalOpIdentity]
  ): PhysicalPlan = {

    val fromOp = physicalPlan.getOperator(physicalLink.fromOpId)
    val fromPortId = physicalLink.fromPortId

    val toOp = physicalPlan.getOperator(physicalLink.toOpId)
    val toPortId = physicalLink.toPortId

    var newPhysicalPlan = physicalPlan
      .removeLink(physicalLink)

    // create cache writer and link
    val storageKey = OpResultStorage.createStorageKey(
      physicalLink.fromOpId.logicalOpId,
      physicalLink.fromPortId,
      isMaterialized = true
    )
    val fromPortOutputMode =
      physicalPlan.getOperator(physicalLink.fromOpId).outputPorts(physicalLink.fromPortId)._1.mode
    val matWriterPhysicalOp: PhysicalOp = SpecialPhysicalOpFactory.newSinkPhysicalOp(
      workflowContext.workflowId,
      workflowContext.executionId,
      storageKey,
      fromPortOutputMode
    )
    val sourceToWriterLink =
      PhysicalLink(
        fromOp.id,
        fromPortId,
        matWriterPhysicalOp.id,
        matWriterPhysicalOp.inputPorts.keys.head
      )
    newPhysicalPlan = newPhysicalPlan
      .addOperator(matWriterPhysicalOp)
      .addLink(sourceToWriterLink)

    // sink has exactly one input port and one output port
    val schema = newPhysicalPlan
      .getOperator(matWriterPhysicalOp.id)
      .outputPorts(matWriterPhysicalOp.outputPorts.keys.head)
      ._3
      .toOption
      .get
    ResultStorage
      .getOpResultStorage(workflowContext.workflowId)
      .create(
        key = storageKey,
        mode = OpResultStorage.defaultStorageMode,
        schema = schema
      )

    // create cache reader and link
    val matReaderPhysicalOp: PhysicalOp = SpecialPhysicalOpFactory.newSourcePhysicalOp(
      workflowContext.workflowId,
      workflowContext.executionId,
      storageKey
    )
    val readerToDestLink =
      PhysicalLink(
        matReaderPhysicalOp.id,
        matReaderPhysicalOp.outputPorts.keys.head,
        toOp.id,
        toPortId
      )
    // add the pair to the map for later adding edges between 2 regions.
    writerReaderPairs(matWriterPhysicalOp.id) = matReaderPhysicalOp.id
    newPhysicalPlan
      .addOperator(matReaderPhysicalOp)
      .addLink(readerToDestLink)
  }
}
