package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.core.storage.VFSURIFactory.createResultURI
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, PhysicalOpIdentity}
import edu.uci.ics.amber.core.workflow.{
  GlobalPortIdentity,
  PhysicalLink,
  PhysicalPlan,
  WorkflowContext
}
import edu.uci.ics.amber.engine.architecture.scheduling.config.{PortConfig, ResourceConfig}
import edu.uci.ics.amber.engine.common.{AmberConfig, AmberLogging}
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.graph.{DirectedAcyclicGraph, DirectedPseudograph}

import java.util.concurrent.TimeoutException
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

class CostBasedScheduleGenerator(
    workflowContext: WorkflowContext,
    initialPhysicalPlan: PhysicalPlan,
    val actorId: ActorVirtualIdentity
) extends ScheduleGenerator(
      workflowContext,
      initialPhysicalPlan
    )
    with AmberLogging {

  case class SearchResult(
      state: Set[PhysicalLink],
      regionDAG: DirectedAcyclicGraph[Region, RegionLink],
      cost: Double,
      searchTimeNanoSeconds: Long = 0,
      numStatesExplored: Int = 0
  )

  private val costEstimator =
    new DefaultCostEstimator(workflowContext = workflowContext, actorId = actorId)

  def generate(): (Schedule, PhysicalPlan) = {
    val startTime = System.nanoTime()
    val regionDAG = createRegionDAG()
    val totalRPGTime = System.nanoTime() - startTime
    val regionPlan = RegionPlan(
      regions = regionDAG.iterator().asScala.toSet,
      regionLinks = regionDAG.edgeSet().asScala.toSet
    )
    val schedule = generateScheduleFromRegionPlan(regionPlan)
    logger.info(
      s"WID: ${workflowContext.workflowId.id}, EID: ${workflowContext.executionId.id}, total RPG time: " +
        s"${totalRPGTime / 1e6} ms."
    )
    (
      schedule,
      physicalPlan
    )
  }

  /**
    * Create regions based on only pipelined edges. This does not add the region links.
    *
    * @param physicalPlan The original physical plan without materializations added yet.
    * @param matEdges     Set of edges to materialize (including the original blocking edges).
    * @return A set of regions.
    */
  private def createRegions(
      physicalPlan: PhysicalPlan,
      matEdges: Set[PhysicalLink]
  ): Set[Region] = {
    val matEdgesRemovedDAG = matEdges.foldLeft(physicalPlan) { (currentPlan, linkToRemove) =>
      currentPlan.removeLink(linkToRemove)
    }
    val connectedComponents = new BiconnectivityInspector[PhysicalOpIdentity, PhysicalLink](
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
        val portIdsToViewResult: Set[GlobalPortIdentity] =
          workflowContext.workflowSettings.outputPortsNeedingStorage
            .filter(outputPort => operatorIds.contains(outputPort.opId))
        val portIdsNeedingStorage: Set[GlobalPortIdentity] = matEdges
          .diff(physicalPlan.getDependeeLinks)
          .filter(matLink => operatorIds.contains(matLink.fromOpId))
          .flatMap(link =>
            List(
              GlobalPortIdentity(link.fromOpId, link.fromPortId)
            )
          ) ++ portIdsToViewResult
        val portConfigs = portIdsNeedingStorage
          .map(outputPortId =>
            outputPortId -> {
              val uri = createResultURI(
                workflowId = workflowContext.workflowId,
                executionId = workflowContext.executionId,
                globalPortId = outputPortId
              )
              PortConfig(storageURI = uri)
            }
          )
          .toMap
        val resourceConfig = ResourceConfig(portConfigs = portConfigs)
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
          ports = ports,
          resourceConfig = Some(resourceConfig)
        )
    }
  }

  /**
    * Checks a plan for schedulability, and returns a region DAG if the plan is schedulable.
    *
    * @param matEdges Set of edges to materialize (including the original blocking edges).
    * @return If the plan is schedulable, a region DAG will be returned. Otherwise a DirectedPseudograph (with directed
    *         cycles) will be returned to indicate that the plan is unschedulable.
    */
  private def tryConnectRegionDAG(
      matEdges: Set[PhysicalLink]
  ): Either[DirectedAcyclicGraph[Region, RegionLink], DirectedPseudograph[Region, RegionLink]] = {
    val regionDAG = new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink])
    val regionGraph = new DirectedPseudograph[Region, RegionLink](classOf[RegionLink])
    val opToRegionMap = new mutable.HashMap[PhysicalOpIdentity, Region]
    createRegions(physicalPlan, matEdges).foreach(region => {
      region.getOperators.foreach(op => opToRegionMap(op.id) = region)
      regionGraph.addVertex(region)
      regionDAG.addVertex(region)
    })
    var isAcyclic = true
    matEdges.foreach(matEdge => {
      val fromRegion = opToRegionMap(matEdge.fromOpId)
      val toRegion = opToRegionMap(matEdge.toOpId)
      regionGraph.addEdge(fromRegion, toRegion, RegionLink(fromRegion.id, toRegion.id))
      try {
        regionDAG.addEdge(fromRegion, toRegion, RegionLink(fromRegion.id, toRegion.id))
      } catch {
        case _: IllegalArgumentException =>
          isAcyclic = false
      }
    })
    if (isAcyclic) Left(regionDAG)
    else Right(regionGraph)
  }

  /**
    * Performs a search to generate a region DAG.
    * Materializations are added only after the plan is determined to be schedulable.
    *
    * @return A region DAG.
    */
  private def createRegionDAG(): DirectedAcyclicGraph[Region, RegionLink] = {
    val searchResultFuture: Future[SearchResult] = Future {
      if (AmberConfig.useTopDownSearch)
        topDownSearch(globalSearch = AmberConfig.useGlobalSearch)
      else
        bottomUpSearch(globalSearch = AmberConfig.useGlobalSearch)
    }
    val searchResult = Try(
      Await.result(searchResultFuture, AmberConfig.searchTimeoutMilliseconds.milliseconds)
    ) match {
      case Failure(exception) =>
        exception match {
          case _: TimeoutException =>
            logger.warn(
              s"WID: ${workflowContext.workflowId.id}, EID: ${workflowContext.executionId.id}, search for region plan " +
                s"timed out, falling back to bottom-up greedy search.",
              exception
            )
            bottomUpSearch()
          case _ => throw new RuntimeException(exception)
        }

      case Success(result) =>
        result
    }
    logger.info(
      s"WID: ${workflowContext.workflowId.id}, EID: ${workflowContext.executionId.id}, search time: " +
        s"${searchResult.searchTimeNanoSeconds / 1e6} ms."
    )
    // Only a non-dependee blocking link that has not already been materialized should be replaced
    // with a materialization write op + materialization read op.
    val linksToMaterialize =
      (searchResult.state ++ physicalPlan.getBlockingAndDependeeLinks).diff(
        physicalPlan.getDependeeLinks
      )
    if (linksToMaterialize.nonEmpty) {
      val matReaderWriterPairs = new mutable.HashMap[PhysicalOpIdentity, PhysicalOpIdentity]()
      linksToMaterialize.foreach(link =>
        physicalPlan = replaceLinkWithMaterialization(
          link,
          matReaderWriterPairs
        )
      )
    }
    // Calling the search again to include cache read ops in the regions.
    // This new search is only needed because of additional cache read operators which alters the physical plan.
    // However, as the new physical plan is already schedulable, the original materialized ports of each region will not
    // be included in the new region DAG.
    // TODO: remove this step after cache read is removed.
    val regionDAG = bottomUpSearch().regionDAG

    addMaterializationsAsRegionLinks(linksToMaterialize, regionDAG)
    populateDependeeLinks(regionDAG)
    // The materialized ports of each region are already decided by the first RPG search
    // However they will be lost after the second search as a consequence of the modified physical plan.
    // The second search is only needed because of additional cache read operators.
    // Need to add the original materialized ports back. This will not be needed after removal of cache read ops.
    // TODO: remove this step after cache read is removed.
    val outputPortsToMaterialize = linksToMaterialize.map(link =>
      GlobalPortIdentity(opId = link.fromOpId, portId = link.fromPortId)
    )
    updateRegionsWithOutputPortStorage(outputPortsToMaterialize, regionDAG)
    allocateResource(regionDAG)
    regionDAG
  }

  /**
    * Adds materialization links as region links within the given region DAG.
    * This method processes each physical link in the input set, identifying the source and destination
    * regions for each link. It then adds an edge between these regions in the DAG to represent
    * the materialization relationship.
    *
    * @param linksToMaterialize The set of physical links to be materialized as region links in the DAG.
    * @param regionDAG The DAG of regions to be modified
    */
  private def addMaterializationsAsRegionLinks(
      linksToMaterialize: Set[PhysicalLink],
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Unit = {
    linksToMaterialize.foreach(link => {
      val fromOpRegions = getRegions(link.fromOpId, regionDAG)
      val toOpRegions = getRegions(link.toOpId, regionDAG)
      fromOpRegions.foreach(fromRegion => {
        toOpRegions.foreach(toRegion => {
          regionDAG.addEdge(fromRegion, toRegion, RegionLink(fromRegion.id, toRegion.id))
        })
      })
    })
  }

  /**
    * The core of the search algorithm. If the input physical plan is already schedulable, no search will be executed.
    * Otherwise, depending on the configuration, either a global search or a greedy search will be performed to find
    * an optimal plan. The search starts from a plan where all non-blocking edges are pipelined, and leads to a low-cost
    * schedulable plan by changing pipelined non-blocking edges to materialized. By default all pruning techniques
    * are enabled (chains, clean edges, and early stopping on schedulable states).
    *
    * @return A SearchResult containing the plan, the region DAG (without materializations added yet), the cost, the
    *         time to finish search, and the number of states explored.
    */
  def bottomUpSearch(
      globalSearch: Boolean = false,
      oChains: Boolean = true,
      oCleanEdges: Boolean = true,
      oEarlyStop: Boolean = true
  ): SearchResult = {
    val startTime = System.nanoTime()
    val originalNonBlockingEdges =
      if (oCleanEdges) {
        physicalPlan.getNonBridgeNonBlockingLinks
      } else {
        physicalPlan.links.diff(
          physicalPlan.getBlockingAndDependeeLinks
        )
      }
    // Queue to hold states to be explored, starting with the empty set
    val queue: mutable.Queue[Set[PhysicalLink]] = mutable.Queue(Set.empty[PhysicalLink])
    // Keep track of visited states to avoid revisiting
    val visited: mutable.Set[Set[PhysicalLink]] = mutable.Set.empty[Set[PhysicalLink]]
    // Used for the Early Stop optimization technique
    val schedulableStates: mutable.Set[Set[PhysicalLink]] = mutable.Set.empty[Set[PhysicalLink]]
    // Initialize the bestResult with an impossible high cost for comparison
    var bestResult: SearchResult = SearchResult(
      state = Set.empty,
      regionDAG = new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink]),
      cost = Double.PositiveInfinity
    )

    while (queue.nonEmpty) {
      // A state is represented as a set of materialized non-blocking edges.
      val currentState = queue.dequeue()
      breakable {
        if (
          oEarlyStop && schedulableStates
            .exists(ancestorState => ancestorState.subsetOf(currentState))
        ) {
          // Early stop: stopping exploring states beyond a schedulable state since the cost will only increase.
          // A state X is a descendant of an ancestor state Y in the bottom-up search process if Y's set of materialized
          // edges is a subset of that of X's (since X is reachable from Y by adding more materialized edges.)
          break
        }
        visited.add(currentState)
        tryConnectRegionDAG(
          physicalPlan.getBlockingAndDependeeLinks ++ currentState
        ) match {
          case Left(regionDAG) =>
            updateOptimumIfApplicable(regionDAG)
            addNeighborStatesToFrontier()
          case Right(_) =>
            addNeighborStatesToFrontier()
        }
      }

      /**
        * An internal method of bottom-up search that updates the current optimum if the examined state is schedulable
        * and has a lower cost.
        */
      def updateOptimumIfApplicable(regionDAG: DirectedAcyclicGraph[Region, RegionLink]): Unit = {
        if (oEarlyStop) schedulableStates.add(currentState)
        // Calculate the current state's cost and update the bestResult if it's lower
        val cost =
          evaluate(
            RegionPlan(regionDAG.vertexSet().asScala.toSet, regionDAG.edgeSet().asScala.toSet)
          )
        if (cost < bestResult.cost) {
          bestResult = SearchResult(currentState, regionDAG, cost)
        }
      }

      /**
        * An internal method of bottom-up search that performs state transitions (changing an pipelined edge to
        * materialized) to include the unvisited neighbor(s) of the current state in the frontier (i.e., the queue).
        * If using global search, all unvisited neighbors will be included. Otherwise in a greedy search, only the
        * neighbor with the lowest cost will be included.
        */
      def addNeighborStatesToFrontier(): Unit = {
        val allCurrentMaterializedEdges =
          currentState ++ physicalPlan.getBlockingAndDependeeLinks
        // Generate and enqueue all neighbour states that haven't been visited
        var candidateEdges = originalNonBlockingEdges
          .diff(currentState)
        if (oChains) {
          val edgesInChainWithMaterializedEdges = physicalPlan.maxChains
            .filter(chain => chain.intersect(allCurrentMaterializedEdges).nonEmpty)
            .flatten
          candidateEdges = candidateEdges.diff(
            edgesInChainWithMaterializedEdges
          ) // Edges in chain with blocking edges should not be materialized
        }

        val unvisitedNeighborStates = candidateEdges
          .map(edge => currentState + edge)
          .filter(neighborState =>
            !visited.contains(neighborState) && !queue.contains(neighborState)
          )

        val filteredNeighborStates = if (oEarlyStop) {
          // Any descendant state of a schedulable state is not worth exploring.
          unvisitedNeighborStates.filter(neighborState =>
            !schedulableStates.exists(ancestorState => ancestorState.subsetOf(neighborState))
          )
        } else {
          unvisitedNeighborStates
        }

        if (globalSearch) {
          // include all unvisited neighbors
          filteredNeighborStates.foreach(neighborState => queue.enqueue(neighborState))
        } else {
          // greedy search, only include an unvisited neighbor with the lowest cost
          if (filteredNeighborStates.nonEmpty) {
            val minCostNeighborState = filteredNeighborStates.minBy(neighborState =>
              tryConnectRegionDAG(
                physicalPlan.getBlockingAndDependeeLinks ++ neighborState
              ) match {
                case Left(regionDAG) =>
                  evaluate(
                    RegionPlan(
                      regionDAG.vertexSet().asScala.toSet,
                      regionDAG.edgeSet().asScala.toSet
                    )
                  )
                case Right(_) =>
                  Double.MaxValue
              }
            )
            queue.enqueue(minCostNeighborState)
          }
        }
      }
    }

    val searchTime = System.nanoTime() - startTime
    bestResult.copy(
      searchTimeNanoSeconds = searchTime,
      numStatesExplored = visited.size
    )
  }

  /**
    * Another direction to perform the search. Depending on the configuration, either a global search or a greedy search
    * will be performed to find an optimal plan. The search starts from a plan where all edges are materialized, and
    * leads to a low-cost schedulable plan by changing materialized non-blocking edges to pipelined.
    * By default, all pruning techniques are enabled (chains, clean edges).
    *
    * @return A SearchResult containing the plan, the region DAG (without materializations added yet), the cost, the
    *         time to finish search, and the number of states explored.
    */
  def topDownSearch(
      globalSearch: Boolean = false,
      oChains: Boolean = true,
      oCleanEdges: Boolean = true
  ): SearchResult = {
    val startTime = System.nanoTime()
    // Starting from a state where all non-blocking edges are materialized
    val originalSeedState = physicalPlan.links.diff(
      physicalPlan.getBlockingAndDependeeLinks
    )

    // Chain optimization: an edge in the same chain as a blocking edge should not be materialized
    val seedStateOptimizedByChainsIfApplicable = if (oChains) {
      val edgesInChainWithBlockingEdge = physicalPlan.maxChains
        .filter(chain => chain.intersect(physicalPlan.getBlockingAndDependeeLinks).nonEmpty)
        .flatten
      originalSeedState.diff(edgesInChainWithBlockingEdge)
    } else {
      originalSeedState
    }

    // Clean edge optimization: a clean edge should not be materialized
    val finalSeedState = if (oCleanEdges) {
      seedStateOptimizedByChainsIfApplicable.intersect(physicalPlan.getNonBridgeNonBlockingLinks)
    } else {
      seedStateOptimizedByChainsIfApplicable
    }

    // Queue to hold states to be explored, starting with the seed state
    val queue: mutable.Queue[Set[PhysicalLink]] = mutable.Queue(finalSeedState)
    // Keep track of visited states to avoid revisiting
    val visited: mutable.Set[Set[PhysicalLink]] = mutable.Set.empty[Set[PhysicalLink]]
    // Initialize the bestResult with an impossible high cost for comparison
    var bestResult: SearchResult = SearchResult(
      state = Set.empty,
      regionDAG = new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink]),
      cost = Double.PositiveInfinity
    )

    while (queue.nonEmpty) {
      val currentState = queue.dequeue()
      visited.add(currentState)
      tryConnectRegionDAG(
        physicalPlan.getBlockingAndDependeeLinks ++ currentState
      ) match {
        case Left(regionDAG) =>
          updateOptimumIfApplicable(regionDAG)
          addNeighborStatesToFrontier()
        // No need to explore further
        case Right(_) =>
          addNeighborStatesToFrontier()
      }

      /**
        * An internal method of top-down search that updates the current optimum if the examined state is schedulable
        * and has a lower cost.
        */
      def updateOptimumIfApplicable(regionDAG: DirectedAcyclicGraph[Region, RegionLink]): Unit = {
        // Calculate the current state's cost and update the bestResult if it's lower
        val cost =
          evaluate(
            RegionPlan(regionDAG.vertexSet().asScala.toSet, regionDAG.edgeSet().asScala.toSet)
          )
        if (cost < bestResult.cost) {
          bestResult = SearchResult(currentState, regionDAG, cost)
        }
      }

      /**
        * An internal method of top-down search that performs state transitions (changing an materialized edge to
        * pipelined) to include the unvisited neighbor(s) of the current state in the frontier (i.e., the queue).
        * If using global search, all unvisited neighbors will be included. Otherwise in a greedy search, only the
        * neighbor with the lowest cost will be included.
        */
      def addNeighborStatesToFrontier(): Unit = {
        val unvisitedNeighborStates = currentState
          .map(edge => currentState - edge)
          .filter(neighborState =>
            !visited.contains(neighborState) && !queue.contains(neighborState)
          )

        if (globalSearch) {
          // include all unvisited neighbors
          unvisitedNeighborStates.foreach(neighborState => queue.enqueue(neighborState))
        } else {
          // greedy search, only include an unvisited neighbor with the lowest cost
          if (unvisitedNeighborStates.nonEmpty) {
            val minCostNeighborState = unvisitedNeighborStates.minBy(neighborState =>
              tryConnectRegionDAG(
                physicalPlan.getBlockingAndDependeeLinks ++ neighborState
              ) match {
                case Left(regionDAG) =>
                  evaluate(
                    RegionPlan(
                      regionDAG.vertexSet().asScala.toSet,
                      regionDAG.edgeSet().asScala.toSet
                    )
                  )
                case Right(_) =>
                  Double.MaxValue
              }
            )
            queue.enqueue(minCostNeighborState)
          }
        }
      }
    }

    val searchTime = System.nanoTime() - startTime
    bestResult.copy(
      searchTimeNanoSeconds = searchTime,
      numStatesExplored = visited.size
    )
  }

  /**
    * The cost function used by the search. Takes a region plan, generates one or more (to be done in the future)
    * schedules based on the region plan, and calculates the cost of the schedule(s) using Cost Estimator. Uses the cost
    * of the best schedule (currently only considers one schedule) as the cost of the region plan.
    *
    * @return A cost determined by the cost estimator.
    */
  private def evaluate(regionPlan: RegionPlan): Double = {
    val schedule = generateScheduleFromRegionPlan(regionPlan)
    // In the future we may allow multiple regions in a level and split the resources.
    schedule.map(level => level.map(region => costEstimator.estimate(region, 1)).sum).sum
  }

}
