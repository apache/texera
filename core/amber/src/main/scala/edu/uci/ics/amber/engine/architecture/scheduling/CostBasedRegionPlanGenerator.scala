package edu.uci.ics.amber.engine.architecture.scheduling

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.scheduling.ExpansionGreedyRegionPlanGenerator.replaceVertex
import edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies.{DefaultResourceAllocator, ExecutionClusterInfo}
import edu.uci.ics.amber.engine.common.virtualidentity.{OperatorIdentity, PhysicalOpIdentity}
import edu.uci.ics.amber.engine.common.workflow.{OutputPort, PhysicalLink, PortIdentity}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PhysicalPlan}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.mutable
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IteratorHasAsScala}

class CostBasedRegionPlanGenerator(
    workflowContext: WorkflowContext,
    var physicalPlan: PhysicalPlan,
    opResultStorage: OpResultStorage
) extends RegionPlanGenerator(
      workflowContext,
      physicalPlan,
      opResultStorage
    ) with LazyLogging {

  private val executionClusterInfo = new ExecutionClusterInfo()

  private val candidateNBEdges = mutable.Set.empty[PhysicalLink]

  override def generate(): (RegionPlan, PhysicalPlan) = {

    val regionDAG = createRegionDAG(workflowContext)

    (
      RegionPlan(
        regions = regionDAG.iterator().asScala.toSet,
        regionLinks = regionDAG.edgeSet().asScala.toSet
      ),
      physicalPlan
    )
  }

  private def createRegions(physicalPlan: PhysicalPlan, matEdges: Set[PhysicalLink]): Set[Region] = {
    val matEdgesRemovedDAG = physicalPlan.removeLinks(matEdges)
    val connectedComponents = new BiconnectivityInspector[PhysicalOpIdentity, DefaultEdge](
      matEdgesRemovedDAG.dag
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
        Region(
          id = RegionIdentity(idx),
          physicalOps = operators,
          physicalLinks = links
        )
    }
  }

  private def getRegionDAGorUnschedulable(matEdges: Set[PhysicalLink]): Option[DirectedAcyclicGraph[Region, RegionLink]] = {
    val regionGraph = new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink])
    val opToRegionMap = new mutable.HashMap[PhysicalOpIdentity, Region]
    createRegions(physicalPlan, matEdges).foreach(region => {
      region.getOperators.foreach(op=>opToRegionMap(op.id) = region)
      regionGraph.addVertex(region)
    })
    matEdges.foreach(blockingEdge => {
      val fromRegion = opToRegionMap(blockingEdge.fromOpId)
      val toRegion = opToRegionMap(blockingEdge.toOpId)
      try {
        regionGraph.addEdge(fromRegion, toRegion, RegionLink(fromRegion.id, toRegion.id))
      } catch {
        case _: IllegalArgumentException =>
          candidateNBEdges ++= (physicalPlan.getUpstreamPhysicalLinks(blockingEdge.toOpId).filter(nbEdge => nbEdge != blockingEdge))
          None
      }
    })
    Option(regionGraph)
  }

  private def createRegionDAG(context: WorkflowContext): DirectedAcyclicGraph[Region, RegionLink] = {
    val searchResult = bfs()
    val linksToMaterialize = searchResult.state
    if (linksToMaterialize.nonEmpty) {
      val matReaderWriterPairs = new mutable.HashMap[PhysicalOpIdentity, PhysicalOpIdentity]()
      linksToMaterialize.foreach(link=>physicalPlan = replaceLinkWithMaterialization(
        link,
        matReaderWriterPairs
      ))
    }
    val updatedSearchResult = bfs()
    val regionDAG = updatedSearchResult.regionDAG
    populateDownstreamLinks(regionDAG)
    allocateResource(regionDAG)
    regionDAG
  }

  private case class SearchResult(state: Set[PhysicalLink], regionDAG: DirectedAcyclicGraph[Region, RegionLink], cost: Double)

  private def bfs(): SearchResult = {
    val originalNonBlockingEdges = physicalPlan.getNonBridgeNonBlockingLinks
    // Queue to hold states to be explored, starting with the empty set
    val queue: mutable.Queue[Set[PhysicalLink]] = mutable.Queue(Set.empty[PhysicalLink])
    // Keep track of visited states to avoid revisiting
    val visited: mutable.Set[Set[PhysicalLink]] = mutable.Set.empty[Set[PhysicalLink]]
    // Initialize the result with an impossible high cost for comparison
    var result: SearchResult = SearchResult(Set.empty, new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink]), Double.PositiveInfinity)

    while (queue.nonEmpty) {
      val currentState = queue.dequeue()
      visited.add(currentState)
      getRegionDAGorUnschedulable(physicalPlan.getOriginalBlockingLinks ++ currentState) match {
        case Some(regionDAG: DirectedAcyclicGraph[Region, RegionLink]) =>
          // Calculate the current state's cost and update the result if it's lower
          val currentCost = cost(currentState)
          if (currentCost < result.cost) {
            result = SearchResult(currentState, regionDAG, currentCost)
          }
          // No need to explore further
        case None =>
          val currentCost = cost(currentState)
          if (currentCost < result.cost) {
            val allBlockingEdges = currentState ++ physicalPlan.getOriginalBlockingLinks
            // Generate and enqueue all neighbor states that haven't been visited
            val edgesInChainWithBlockingEdge = physicalPlan.getMaxChains.filter(chain => chain.intersect(allBlockingEdges).nonEmpty).flatten
            val candidateEdges = originalNonBlockingEdges
              .diff(edgesInChainWithBlockingEdge)
              .diff(currentState)
//            candidateEdges.foreach { link =>
//                val nextState = currentState + link
//                if (!visited.contains(nextState) && !queue.contains(nextState)) {
//                  queue.enqueue(nextState)
//                }
//              }
                  val nextLink = candidateEdges.head
                  val nextState = currentState + nextLink
                  if (!visited.contains(nextState) && !queue.contains(nextState)) {
                    queue.enqueue(nextState)
                  }
          }
      }
    }

    result
  }

  private def cost(state: Set[PhysicalLink]): Double = {
    // Using number of materialization (region) edges as a cost.
//    regionDAG.edgeSet().size().toDouble
    if (state.intersect(candidateNBEdges).nonEmpty) 0
    else Double.PositiveInfinity
//    state.size
  }

  private def replaceLinkWithMaterialization(
                                              physicalLink: PhysicalLink,
                                              writerReaderPairs: mutable.HashMap[PhysicalOpIdentity, PhysicalOpIdentity]
                                            ): PhysicalPlan = {
    val fromOp = physicalPlan.getOperator(physicalLink.fromOpId)
    val fromPortId = physicalLink.fromPortId

    val toOp = physicalPlan.getOperator(physicalLink.toOpId)
    val toPortId = physicalLink.toPortId

    val outputSchema = fromOp.outputPorts(fromPortId)._3
    val matWriterPhysicalOp: PhysicalOp =
      createMatWriter(outputSchema, fromOp.id.logicalOpId)
    val matReaderPhysicalOp: PhysicalOp =
      createMatReader(outputSchema, matWriterPhysicalOp.id.logicalOpId)

    // create 2 links for materialization
    val readerToDestLink =
      PhysicalLink(
        matReaderPhysicalOp.id,
        matReaderPhysicalOp.outputPorts.keys.head,
        toOp.id,
        toPortId
      )
    val sourceToWriterLink =
      PhysicalLink(
        fromOp.id,
        fromPortId,
        matWriterPhysicalOp.id,
        matWriterPhysicalOp.inputPorts.keys.head
      )

    // add the pair to the map for later adding edges between 2 regions.
    writerReaderPairs(matWriterPhysicalOp.id) = matReaderPhysicalOp.id

    physicalPlan
      .removeLink(physicalLink)
      .addOperator(matWriterPhysicalOp)
      .addOperator(matReaderPhysicalOp)
      .addLink(readerToDestLink)
      .addLink(sourceToWriterLink)
      .setOperatorUnblockPort(toOp.id, toPortId)

  }

  private def createMatReader(
                               inputSchema: Schema,
                               matWriterLogicalOpId: OperatorIdentity
                             ): PhysicalOp = {
    val matReader = new CacheSourceOpDesc(
      matWriterLogicalOpId,
      opResultStorage: OpResultStorage
    )
    matReader.setContext(workflowContext)
    matReader.setOperatorId("cacheSource_" + matWriterLogicalOpId.id)
    matReader.schema = inputSchema

    // expect exactly one output port
    matReader.outputPortToSchemaMapping(matReader.operatorInfo.outputPorts.head.id) =
      matReader.getOutputSchema(Array())

    matReader.getPhysicalOp(
      workflowContext.workflowId,
      workflowContext.executionId
    )
  }

  private def createMatWriter(
                               inputSchema: Schema,
                               fromLogicalOpId: OperatorIdentity
                             ): PhysicalOp = {
    val matWriter = new ProgressiveSinkOpDesc()
    matWriter.setContext(workflowContext)
    matWriter.setOperatorId("materialized_" + fromLogicalOpId.id)

    // expect exactly one input port and one output port
    val inputPort = matWriter.operatorInfo().inputPorts.head
    val outputPort = matWriter.operatorInfo().outputPorts.head
    matWriter.inputPortToSchemaMapping(inputPort.id) = inputSchema
    matWriter.outputPortToSchemaMapping(outputPort.id) =
      matWriter.getOutputSchema(Array(inputSchema))
    matWriter.setStorage(
      opResultStorage.create(
        key = matWriter.operatorIdentifier,
        mode = OpResultStorage.defaultStorageMode
      )
    )
    opResultStorage.get(matWriter.operatorIdentifier).setSchema(inputSchema)

    matWriter.getPhysicalOp(
      workflowContext.workflowId,
      workflowContext.executionId
    )

  }

  private def populateDownstreamLinks(
                                       regionDAG: DirectedAcyclicGraph[Region, RegionLink]
                                     ): DirectedAcyclicGraph[Region, RegionLink] = {

    val blockingLinks = physicalPlan
      .topologicalIterator()
      .flatMap { physicalOpId =>
        val upstreamPhysicalOpIds = physicalPlan.getUpstreamPhysicalOpIds(physicalOpId)
        upstreamPhysicalOpIds.flatMap { upstreamPhysicalOpId =>
          physicalPlan
            .getLinksBetween(upstreamPhysicalOpId, physicalOpId)
            .filter(link => physicalPlan.getOperator(physicalOpId).isInputLinkBlocking(link))
        }
      }
      .toSet

    blockingLinks
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
    regionDAG
  }

  private def allocateResource(regionDAG: DirectedAcyclicGraph[Region, RegionLink]): Unit = {
    val resourceAllocator = new DefaultResourceAllocator(physicalPlan, executionClusterInfo)
    // generate the region configs
    new TopologicalOrderIterator(regionDAG).asScala
      .foreach(region => {
        val (newRegion, estimationCost) = resourceAllocator.allocate(region)
        replaceVertex(regionDAG, region, newRegion)
      })
  }

  private def getRegions(
                          physicalOpId: PhysicalOpIdentity,
                          regionDAG: DirectedAcyclicGraph[Region, RegionLink]
                        ): Set[Region] = {
    regionDAG
      .vertexSet()
      .asScala
      .filter(region => region.getOperators.map(_.id).contains(physicalOpId))
      .toSet
  }

}
