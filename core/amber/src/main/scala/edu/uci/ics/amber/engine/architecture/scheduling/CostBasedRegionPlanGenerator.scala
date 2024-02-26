package edu.uci.ics.amber.engine.architecture.scheduling

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}

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
    )
    with LazyLogging {

  private case class SearchResult(
      state: Set[PhysicalLink],
      regionDAG: DirectedAcyclicGraph[Region, RegionLink],
      cost: Double
  )

  private val useGlobalSearch = AmberConfig.useGlobalSearch

  override def generate(): (RegionPlan, PhysicalPlan) = {

    val regionDAG = createRegionDAG()

    (
      RegionPlan(
        regions = regionDAG.iterator().asScala.toSet,
        regionLinks = regionDAG.edgeSet().asScala.toSet
      ),
      physicalPlan
    )
  }

  private def createRegions(
      physicalPlan: PhysicalPlan,
      matEdges: Set[PhysicalLink]
  ): Set[Region] = {
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

  private def getRegionDAGorUnschedulable(
      matEdges: Set[PhysicalLink]
  ): Option[DirectedAcyclicGraph[Region, RegionLink]] = {
    val regionGraph = new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink])
    val opToRegionMap = new mutable.HashMap[PhysicalOpIdentity, Region]
    createRegions(physicalPlan, matEdges).foreach(region => {
      region.getOperators.foreach(op => opToRegionMap(op.id) = region)
      regionGraph.addVertex(region)
    })
    matEdges.foreach(blockingEdge => {
      val fromRegion = opToRegionMap(blockingEdge.fromOpId)
      val toRegion = opToRegionMap(blockingEdge.toOpId)
      try {
        regionGraph.addEdge(fromRegion, toRegion, RegionLink(fromRegion.id, toRegion.id))
      } catch {
        case _: IllegalArgumentException =>
          return None
      }
    })
    Option(regionGraph)
  }

  private def createRegionDAG(): DirectedAcyclicGraph[Region, RegionLink] = {
    val searchResult = search()
    val linksToMaterialize = searchResult.state
    if (linksToMaterialize.nonEmpty) {
      val matReaderWriterPairs = new mutable.HashMap[PhysicalOpIdentity, PhysicalOpIdentity]()
      linksToMaterialize.foreach(link =>
        physicalPlan = replaceLinkWithMaterialization(
          link,
          matReaderWriterPairs
        )
      )
    }
    val updatedSearchResult = search()
    val regionDAG = updatedSearchResult.regionDAG
    populateDownstreamLinks(regionDAG)
    allocateResource(regionDAG)
    regionDAG
  }

  private def search(): SearchResult = {
    val originalNonBlockingEdges = physicalPlan.getNonBridgeNonBlockingLinks
    // Queue to hold states to be explored, starting with the empty set
    val queue: mutable.Queue[Set[PhysicalLink]] = mutable.Queue(Set.empty[PhysicalLink])
    // Keep track of visited states to avoid revisiting
    val visited: mutable.Set[Set[PhysicalLink]] = mutable.Set.empty[Set[PhysicalLink]]
    // Initialize the result with an impossible high cost for comparison
    var result: SearchResult = SearchResult(
      Set.empty,
      new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink]),
      Double.PositiveInfinity
    )

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
            val edgesInChainWithBlockingEdge = physicalPlan.getMaxChains
              .filter(chain => chain.intersect(allBlockingEdges).nonEmpty)
              .flatten
            val candidateEdges = originalNonBlockingEdges
              .diff(edgesInChainWithBlockingEdge)
              .diff(currentState)
            if (useGlobalSearch) {
              candidateEdges.foreach { link =>
                val nextState = currentState + link
                if (!visited.contains(nextState) && !queue.contains(nextState)) {
                  queue.enqueue(nextState)
                }
              }
            } else {
              val nextLink = candidateEdges.minBy(e => cost(currentState + e))
              val nextState = currentState + nextLink
              if (!visited.contains(nextState) && !queue.contains(nextState)) {
                queue.enqueue(nextState)
              }
            }
          }
      }
    }

    result
  }

  private def cost(state: Set[PhysicalLink]): Double = {
    // Using number of materialization (region) edges as the cost.
    // This is independent of the schedule / resource allocator
    // In the future we may need to use the ResourceAllocator to get the cost
    state.size
  }

}
