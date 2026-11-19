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

import org.apache.texera.common.config.ApplicationConfig
import org.apache.texera.amber.core.storage.VFSURIFactory.createPortBaseURI
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, PhysicalOpIdentity}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.engine.architecture.scheduling.EdgeType._
import org.apache.texera.amber.engine.architecture.scheduling.SchedulingUtils.replaceVertex
import org.apache.texera.amber.engine.architecture.scheduling.config.{
  IntermediateInputPortConfig,
  OutputPortConfig,
  ResourceConfig
}
import org.apache.texera.amber.engine.architecture.scheduling.resourcePolicies.{
  DefaultResourceAllocator,
  ExecutionClusterInfo
}
import org.apache.texera.amber.engine.common.AmberLogging
import org.jgrapht.Graph
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.graph.{DirectedAcyclicGraph, DirectedPseudograph}

import java.net.URI
import java.util.concurrent.TimeoutException
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

object CostBasedScheduleGenerator {

  /**
    * The execution mode to schedule under: MATERIALIZED when any operator in
    * `physicalPlan` requires it (e.g. the loop operators, whose back-edge is a
    * cross-region materialized state channel), otherwise the requested mode.
    */
  private[scheduling] def effectiveExecutionMode(
      physicalPlan: PhysicalPlan,
      requestedMode: ExecutionMode
  ): ExecutionMode =
    if (physicalPlan.operators.exists(_.requiresMaterializedExecution))
      ExecutionMode.MATERIALIZED
    else
      requestedMode
}

class CostBasedScheduleGenerator(
    workflowContext: WorkflowContext,
    initialPhysicalPlan: PhysicalPlan,
    val actorId: ActorVirtualIdentity
) extends ScheduleGenerator(
      workflowContext,
      initialPhysicalPlan
    )
    with AmberLogging {

  private[scheduling] case class SearchResult(
      state: SearchState,
      regionDAG: DirectedAcyclicGraph[Region, RegionLink],
      cost: Double,
      searchTimeNanoSeconds: Long = 0,
      numStatesExplored: Int = 0
  )

  private[scheduling] val allowPipelined: Boolean =
    CostBasedScheduleGenerator.effectiveExecutionMode(
      physicalPlan,
      workflowContext.workflowSettings.executionMode
    ) == ExecutionMode.PIPELINED

  private val matchedResults: Map[GlobalPortIdentity, CachedResult] =
    workflowContext.matchedResults

  private[scheduling] val requiredOutputs: Set[GlobalPortIdentity] =
    SearchState.requiredOutputPorts(workflowContext, physicalPlan)

  private val allowedLinkTypes: Map[PhysicalLink, Set[EdgeType]] =
    SearchState.allowedLinkTypes(physicalPlan, matchedResults, allowPipelined)

  private val costEstimator =
    new DefaultCostEstimator(
      workflowContext = workflowContext,
      resourceAllocator = resourceAllocator,
      actorId = actorId
    )

  private case class CostEstimatorMemoKey(
      physicalOpIds: Set[PhysicalOpIdentity],
      physicalLinkIds: Set[PhysicalLink],
      portIds: Set[GlobalPortIdentity],
      resourceConfig: Option[ResourceConfig]
  )

  private type SubPlanKey =
    (Set[PhysicalOpIdentity], Set[PhysicalLink], Map[GlobalPortIdentity, List[URI]])
  private type ReuseBoundaryKey = (Set[PhysicalOpIdentity], Set[PhysicalLink])

  private class SubPlanEvaluation(val estimator: DefaultCostEstimator) {
    val memoization: TrieMap[CostEstimatorMemoKey, (ResourceConfig, Double)] =
      new TrieMap()
  }

  private val defaultEvaluation = new SubPlanEvaluation(costEstimator)

  private val evaluations: TrieMap[SubPlanKey, SubPlanEvaluation] = {
    val map = new TrieMap[SubPlanKey, SubPlanEvaluation]()
    map.put(
      (physicalPlan.operators.map(_.id), physicalPlan.links, Map.empty),
      defaultEvaluation
    )
    map
  }

  private val retainedSubPlans
      : TrieMap[ReuseBoundaryKey, (PhysicalPlan, Map[GlobalPortIdentity, List[URI]])] =
    new TrieMap()

  private def evaluationOf(
      targetPlan: PhysicalPlan,
      readerUrisByInputPort: Map[GlobalPortIdentity, List[URI]]
  ): SubPlanEvaluation = {
    if (matchedResults.isEmpty && readerUrisByInputPort.isEmpty && (targetPlan eq physicalPlan)) {
      return defaultEvaluation
    }
    evaluations.getOrElseUpdate(
      (targetPlan.operators.map(_.id), targetPlan.links, readerUrisByInputPort), {
        val scopedAllocator = new DefaultResourceAllocator(
          physicalPlan,
          new ExecutionClusterInfo(),
          workflowContext.workflowSettings,
          cuid = workflowContext.cuid
        )
        new SubPlanEvaluation(costEstimator.withResourceAllocator(scopedAllocator))
      }
    )
  }

  private def subPlanOf(state: SearchState): (PhysicalPlan, Map[GlobalPortIdentity, List[URI]]) = {
    if (matchedResults.isEmpty) return (physicalPlan, Map.empty)
    retainedSubPlans.getOrElseUpdate(
      (state.skippedOperators(physicalPlan, requiredOutputs), state.linksOfType(CacheRead)),
      (
        state.retainedPlan(physicalPlan, requiredOutputs),
        state.readerUrisByInputPort(physicalPlan, matchedResults, requiredOutputs)
      )
    )
  }

  def generate(): (Schedule, PhysicalPlan) = {
    val startTime = System.nanoTime()
    val searchResult = createRegionDAGSearchResult()
    val totalRPGTime = System.nanoTime() - startTime
    val regionPlan = buildRegionPlan(searchResult.state, searchResult.regionDAG)
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
    * Builds the extended [[RegionPlan]] for `state`: the active regions and links from
    * `regionDAG`, plus any skip regions (`Region.skipped = true`) when `state` skips operators,
    * with skip region ids starting above the largest active region id. When every operator is
    * skipped, `regionDAG` is empty and the `RegionPlan` holds only the skip regions.
    */
  private[scheduling] def buildRegionPlan(
      state: SearchState,
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): RegionPlan = {
    val activeRegions = regionDAG.vertexSet().asScala.toSet
    val regionLinks = regionDAG.edgeSet().asScala.toSet
    if (!state.skipsAnything(physicalPlan, requiredOutputs)) {
      RegionPlan(activeRegions, regionLinks)
    } else {
      val firstSkipRegionId = activeRegions.map(_.id.id).maxOption.map(_ + 1).getOrElse(0L)
      val skipRegions = state.skipRegions(
        physicalPlan,
        matchedResults,
        requiredOutputs,
        firstSkipRegionId
      )
      RegionPlan(activeRegions ++ skipRegions, regionLinks)
    }
  }

  /**
    * Partitions a physical plan into Regions and assigns storage URIs in two passes.
    *
    * <p><strong>Overview</strong></p>
    * <ol>
    *   <li><strong>Region construction:</strong>
    *     Remove all materialized edges from the DAG and compute undirected connected
    *     components. The resulting “Region Graph” may contain directed cycles.</li>
    *   <li><strong>Pass 1 – Output URIs:</strong>
    *     For each Region, allocate storage URIs on every output port of materialized edges.</li>
    *   <li><strong>Pass 2 – Input URIs:</strong>
    *     Re-traverse the same Regions and attach reader URIs on input ports using
    *     the URIs created in Pass 1 and any saved-result locations in `readerUrisByInputPort`.</li>
    * </ol>
    *
    * <p><strong>Why two passes?</strong></p>
    * <ul>
    *   <li>Potential directed cycles in the Region Graph makes a topological
    *       traversal of regions inpossible.</li>
    *   <li>To ensure every output URI exists before its corresponding reader is assigned,
    *       and avoiding “reader before writer” holes, two passes are required.</li>
    * </ul>
    *
    * @param targetPlan            the original physical plan or retained sub-plan (without materializations)
    * @param matEdges              edges to be materialized (including blocking edges)
    * @param readerUrisByInputPort saved-result locations read by input ports of `targetPlan`
    * @return a set of `Region`s whose `ResourceConfig` contains only `URI`s for `PortConfig`s
    *         (`Partitioning` to be assigned later in `ResourceAllocator`; see `IntermediateInputPortConfig`.)
    */
  private[scheduling] def createRegions(
      targetPlan: PhysicalPlan,
      matEdges: Set[PhysicalLink],
      readerUrisByInputPort: Map[GlobalPortIdentity, List[URI]] = Map.empty
  ): Set[Region] = {
    require(
      readerUrisByInputPort.keys.forall(_.input),
      s"readerUrisByInputPort requires input ports, got ${readerUrisByInputPort.keys.filterNot(_.input)}"
    )

    // Pass 0 – remove materialized edges and create connected components

    val matEdgesRemovedDAG: PhysicalPlan = matEdges.foldLeft(targetPlan)(_.removeLink(_))

    val connectedComponents: Set[Graph[PhysicalOpIdentity, PhysicalLink]] =
      new BiconnectivityInspector[PhysicalOpIdentity, PhysicalLink](
        matEdgesRemovedDAG.dag
      ).getConnectedComponents.asScala.toSet

    // Pass 1 – build Regions only output-port storage URIs

    val regionsWithOnlyOutputPortURIs: Set[Region] = connectedComponents.zipWithIndex.map {
      case (connectedSubDAG, idx) =>
        // Operators and intra‑region pipelined links

        val operators: Set[PhysicalOpIdentity] = connectedSubDAG.vertexSet().asScala.toSet

        val links: Set[PhysicalLink] = operators
          .flatMap { opId =>
            targetPlan.getUpstreamPhysicalLinks(opId) ++
              targetPlan.getDownstreamPhysicalLinks(opId)
          }
          .filter(link => operators.contains(link.fromOpId) && operators.contains(link.toOpId))
          .diff(matEdges) // keep only pipelined edges

        val physicalOps: Set[PhysicalOp] = operators.map(targetPlan.getOperator)

        // Frontend-specified ports that need to be materailized (output ports of "eye-icon" physicalOps)
        val outputPortIdsToViewResult: Set[GlobalPortIdentity] =
          workflowContext.workflowSettings.outputPortsNeedingStorage
            .filter(pid => operators.contains(pid.opId))

        // Contains both frontend-specified and scheduler-decided ports that require materailizations.
        val outputPortIdsNeedingStorage: Set[GlobalPortIdentity] =
          matEdges
            .filter(e => operators.contains(e.fromOpId))
            .map(e => GlobalPortIdentity(e.fromOpId, e.fromPortId)) ++
            outputPortIdsToViewResult

        // Allocate an URI for each of these output ports
        val outputPortConfigs: Map[GlobalPortIdentity, OutputPortConfig] =
          outputPortIdsNeedingStorage.map { gpid =>
            val portBaseURI = createPortBaseURI(
              workflowId = workflowContext.workflowId,
              executionId = workflowContext.executionId,
              globalPortId = gpid,
              warehouse = workflowContext.warehouse
            )
            gpid -> OutputPortConfig(portBaseURI)
          }.toMap

        val resourceConfig = ResourceConfig(portConfigs = outputPortConfigs)

        // Enumerate all ports belonging to the Region
        val ports: Set[GlobalPortIdentity] = physicalOps.flatMap { op =>
          op.inputPorts.keysIterator
            .map(inputPortId => GlobalPortIdentity(op.id, inputPortId, input = true)) ++
            op.outputPorts.keysIterator
              .map(outputPortId => GlobalPortIdentity(op.id, outputPortId))
        }

        // Build the Region skeleton (no input‑port URIs yet)
        Region(
          id = RegionIdentity(idx),
          physicalOps = physicalOps,
          physicalLinks = links,
          ports = ports,
          resourceConfig = Some(resourceConfig)
        )
    }

    // Collect writer‑side configs so we can look them up in Pass 2
    val allOutputPortConfigs: Map[GlobalPortIdentity, OutputPortConfig] =
      regionsWithOnlyOutputPortURIs
        .flatMap(_.resourceConfig) // Seq[ResourceConfig]
        .flatMap(_.portConfigs.collect { // PortConfig → OutputPortConfig
          case (id, cfg: OutputPortConfig) => id -> cfg
        })
        .toMap

    // Pass 2 – add input‑port storage configs (reader URIs)

    regionsWithOnlyOutputPortURIs.map { existingRegion =>
      val regionOpIds = existingRegion.getOperators.map(_.id)
      // MatEdges that originally connected to the input ports of this region.
      val relevantMatEdges: Set[PhysicalLink] =
        matEdges.filter(matEdge => regionOpIds.contains(matEdge.toOpId))

      // Assign storage URIs to input ports of each materialized edge (each input port could have more than one URI)
      val inputUrisFromMatEdges: Map[GlobalPortIdentity, List[URI]] =
        relevantMatEdges
          .foldLeft(Map.empty[GlobalPortIdentity, List[URI]]) { (acc, link) =>
            val globalOutputPortId = GlobalPortIdentity(link.fromOpId, link.fromPortId)
            val globalInputPortId = GlobalPortIdentity(link.toOpId, link.toPortId, input = true)

            // Writer‑side URI that must already exist thanks to Pass 1
            val inputReaderURI = allOutputPortConfigs
              .getOrElse(
                globalOutputPortId,
                throw new IllegalStateException(
                  s"Materialization edge $link: attempting to assign a materialization " +
                    s"reader URI for input port $globalInputPortId when " +
                    s"the outout port $globalOutputPortId has not been assigned a URI yet."
                )
              )
              .storageURIBase

            // Group all available URIs of this input port together
            acc.updated(
              globalInputPortId,
              acc.getOrElse(globalInputPortId, List.empty[URI]) :+ inputReaderURI
            )
          }

      val inputUrisFromCache: Map[GlobalPortIdentity, List[URI]] =
        readerUrisByInputPort.filter {
          case (inputPortId, _) => existingRegion.getPorts.contains(inputPortId)
        }

      val inputPortConfigs: Map[GlobalPortIdentity, IntermediateInputPortConfig] =
        (inputUrisFromMatEdges.keySet ++ inputUrisFromCache.keySet).map { inputPortId =>
          inputPortId -> IntermediateInputPortConfig(
            inputUrisFromMatEdges.getOrElse(inputPortId, List.empty[URI]) ++
              inputUrisFromCache.getOrElse(inputPortId, List.empty[URI])
          )
        }.toMap

      val newResourceConfig: Option[ResourceConfig] = existingRegion.resourceConfig match {
        case Some(existingConfig) =>
          Some(ResourceConfig(portConfigs = existingConfig.portConfigs ++ inputPortConfigs))
        case None =>
          if (inputPortConfigs.nonEmpty) Some(ResourceConfig(portConfigs = inputPortConfigs))
          else None
      }

      existingRegion.copy(resourceConfig = newResourceConfig)
    }
  }

  /**
    * Checks a plan for schedulability, and returns a region DAG if the plan is schedulable.
    *
    * @param targetPlan            the original physical plan or retained sub-plan
    * @param matEdges              Set of edges to materialize (including the original blocking edges).
    * @param readerUrisByInputPort saved-result locations read by input ports of `targetPlan`
    * @return If the plan is schedulable, an acyclic region DAG will be returned (empty when
    *         `targetPlan` has no operators). Otherwise a DirectedPseudograph (with directed cycles)
    *         will be returned to indicate that the plan is unschedulable.
    */
  private[scheduling] def tryConnectRegionDAG(
      targetPlan: PhysicalPlan,
      matEdges: Set[PhysicalLink],
      readerUrisByInputPort: Map[GlobalPortIdentity, List[URI]]
  ): Either[DirectedAcyclicGraph[Region, RegionLink], DirectedPseudograph[Region, RegionLink]] = {
    val regionDAG = new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink])
    if (targetPlan.operators.isEmpty) return Left(regionDAG)
    val regionGraph = new DirectedPseudograph[Region, RegionLink](classOf[RegionLink])
    val opToRegionMap = new mutable.HashMap[PhysicalOpIdentity, Region]
    createRegions(targetPlan, matEdges, readerUrisByInputPort).foreach(region => {
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

  private def tryConnectRegionDAG(
      state: SearchState
  ): Either[DirectedAcyclicGraph[Region, RegionLink], DirectedPseudograph[Region, RegionLink]] = {
    val (retainedPlan, readerUrisByInputPort) = subPlanOf(state)
    tryConnectRegionDAG(retainedPlan, state.linksOfType(Materialized), readerUrisByInputPort)
  }

  /**
    * Performs a search over 4-edge-state [[SearchState]]s to generate a region DAG for the
    * retained part of the workflow, falling back to bottom-up greedy search on timeout.
    */
  private def createRegionDAGSearchResult(): SearchResult = {
    val searchResultFuture: Future[SearchResult] = Future {
      val requestedMode = workflowContext.workflowSettings.executionMode
      val effectiveMode =
        CostBasedScheduleGenerator.effectiveExecutionMode(physicalPlan, requestedMode)
      if (effectiveMode != requestedMode) {
        // Surface the silent coercion: the user asked for one mode but an
        // operator (e.g. a loop) forces materialized scheduling.
        val requiringOps = physicalPlan.operators
          .filter(_.requiresMaterializedExecution)
          .map(_.id.logicalOpId.id)
        logger.warn(
          s"WID: ${workflowContext.workflowId.id}, EID: ${workflowContext.executionId.id}, " +
            s"overriding requested execution mode $requestedMode with $effectiveMode: " +
            s"operator(s) ${requiringOps.toSeq.sorted.mkString(", ")} require materialized execution."
        )
      }
      effectiveMode match {
        case ExecutionMode.MATERIALIZED =>
          getFullyMaterializedSearchState
        case ExecutionMode.PIPELINED =>
          if (ApplicationConfig.useTopDownSearch)
            topDownSearch(globalSearch = ApplicationConfig.useGlobalSearch)
          else
            bottomUpSearch(globalSearch = ApplicationConfig.useGlobalSearch)
      }
    }
    val searchResult = Try(
      Await.result(searchResultFuture, ApplicationConfig.searchTimeoutMilliseconds.milliseconds)
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
    searchResult
  }

  /**
    * The core of the search algorithm over 4-edge-state [[SearchState]]s. Starts from the
    * eager-reuse seed where matched saved ports stay CacheRead, links into skipped operators are
    * Unused, blocking active links are Materialized, and non-blocking active links are
    * Pipelined. Transitions Pipelined active links to Materialized using chain, clean-edge, and
    * early-stop pruning on the retained part of the plan.
    */
  private[scheduling] def bottomUpSearch(
      globalSearch: Boolean = false,
      oChains: Boolean = true,
      oCleanEdges: Boolean = true,
      oEarlyStop: Boolean = true
  ): SearchResult = {
    val startTime = System.nanoTime()
    val seedState = SearchState.eagerSeed(
      physicalPlan,
      matchedResults,
      requiredOutputs,
      allowPipelined,
      _ => Pipelined
    )
    val (retainedPlan, _) = subPlanOf(seedState)
    if (retainedPlan.operators.isEmpty) {
      return SearchResult(
        state = seedState,
        regionDAG = new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink]),
        cost = 0.0,
        searchTimeNanoSeconds = System.nanoTime() - startTime,
        numStatesExplored = 1
      )
    }

    val retainedBlockingLinks = retainedPlan.getBlockingAndDependeeLinks
    val originalNonBlockingEdges =
      if (oCleanEdges) {
        retainedPlan.getNonBridgeNonBlockingLinks
      } else {
        retainedPlan.links.diff(retainedBlockingLinks)
      }

    def nonBlockingMatLinks(state: SearchState): Set[PhysicalLink] =
      state.linksOfType(Materialized).diff(retainedBlockingLinks)

    // Queue to hold states to be explored, starting with the eager-reuse seed
    val queue: mutable.Queue[SearchState] = mutable.Queue(seedState)
    val inQueue: mutable.Set[SearchState] = mutable.Set(seedState)
    // Keep track of visited states to avoid revisiting
    val visited: mutable.Set[SearchState] = mutable.Set.empty[SearchState]
    // Used for the Early Stop optimization technique
    val schedulableNonBlockingMatSets: mutable.Set[Set[PhysicalLink]] =
      mutable.Set.empty[Set[PhysicalLink]]
    // Initialize the bestResult with an impossible high cost for comparison
    var bestResult: SearchResult = SearchResult(
      state = seedState,
      regionDAG = new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink]),
      cost = Double.PositiveInfinity
    )

    while (queue.nonEmpty) {
      val currentState = queue.dequeue()
      inQueue.remove(currentState)
      val currentNonBlockingMat = nonBlockingMatLinks(currentState)
      breakable {
        if (
          oEarlyStop && schedulableNonBlockingMatSets
            .exists(ancestorMat => ancestorMat.subsetOf(currentNonBlockingMat))
        ) {
          // Early stop: stopping exploring states beyond a schedulable state since the cost will only increase.
          // A state X is a descendant of an ancestor state Y in the bottom-up search process if Y's set of
          // non-blocking materialized edges is a subset of that of X's (since X is reachable from Y by adding more
          // materialized edges.)
          break()
        }
        visited.add(currentState)
        tryConnectRegionDAG(currentState) match {
          case Left(regionDAG) =>
            updateOptimumIfApplicable(currentState, currentNonBlockingMat, regionDAG)
            addNeighborStatesToFrontier(currentState)
          case Right(_) =>
            addNeighborStatesToFrontier(currentState)
        }
      }

      /**
        * An internal method of bottom-up search that updates the current optimum if the examined state is schedulable
        * and has a lower cost.
        */
      def updateOptimumIfApplicable(
          state: SearchState,
          nonBlockingMat: Set[PhysicalLink],
          regionDAG: DirectedAcyclicGraph[Region, RegionLink]
      ): Unit = {
        if (oEarlyStop) schedulableNonBlockingMatSets.add(nonBlockingMat)
        // Calculate the current state's cost and update the bestResult if it's lower
        val cost = allocateResourcesAndEvaluateCost(state, regionDAG)
        if (cost < bestResult.cost) {
          bestResult = SearchResult(state, regionDAG, cost)
        }
      }

      /**
        * An internal method of bottom-up search that performs state transitions (changing a pipelined active edge to
        * materialized) to include the unvisited neighbor(s) of the current state in the frontier (i.e., the queue).
        * If using global search, all unvisited neighbors will be included. Otherwise in a greedy search, only the
        * neighbor with the lowest cost will be included.
        */
      def addNeighborStatesToFrontier(state: SearchState): Unit = {
        val currentMatEdges = state.linksOfType(Materialized)
        // Generate and enqueue all neighbour states that haven't been visited
        var candidateEdges = originalNonBlockingEdges.filter { link =>
          state(link) == Pipelined && allowedLinkTypes(link).contains(Materialized)
        }
        if (oChains) {
          val edgesInChainWithMaterializedEdges = retainedPlan.maxChains
            .filter(chain => chain.intersect(currentMatEdges).nonEmpty)
            .flatten
          candidateEdges = candidateEdges.diff(
            edgesInChainWithMaterializedEdges
          ) // Edges in chain with blocking edges should not be materialized
        }

        val unvisitedNeighborStates = candidateEdges
          .map(edge => state.updated(edge, Materialized))
          .filter(neighborState =>
            !visited.contains(neighborState) && !inQueue.contains(neighborState)
          )

        val filteredNeighborStates = if (oEarlyStop) {
          // Any descendant state of a schedulable state is not worth exploring.
          unvisitedNeighborStates.filter { neighborState =>
            val neighborNonBlockingMat = nonBlockingMatLinks(neighborState)
            !schedulableNonBlockingMatSets.exists(ancestorMat =>
              ancestorMat.subsetOf(neighborNonBlockingMat)
            )
          }
        } else {
          unvisitedNeighborStates
        }

        enqueueFrontier(filteredNeighborStates, queue, inQueue, globalSearch)
      }
    }

    val searchTime = System.nanoTime() - startTime
    bestResult.copy(
      searchTimeNanoSeconds = searchTime,
      numStatesExplored = visited.size
    )
  }

  private def enqueueFrontier(
      neighbors: Iterable[SearchState],
      queue: mutable.Queue[SearchState],
      inQueue: mutable.Set[SearchState],
      globalSearch: Boolean
  ): Unit = {
    if (globalSearch) {
      neighbors.foreach { neighborState =>
        inQueue.add(neighborState)
        queue.enqueue(neighborState)
      }
    } else if (neighbors.nonEmpty) {
      val minCostNeighborState = neighbors.minBy { neighborState =>
        tryConnectRegionDAG(neighborState) match {
          case Left(regionDAG) => allocateResourcesAndEvaluateCost(neighborState, regionDAG)
          case Right(_)        => Double.MaxValue
        }
      }
      inQueue.add(minCostNeighborState)
      queue.enqueue(minCostNeighborState)
    }
  }

  /**
    * Constructs a baseline fully materialized region plan (one retained operator per region)
    * from the eager-reuse all-Materialized seed and evaluates its cost.
    */
  private[scheduling] def getFullyMaterializedSearchState: SearchResult = {
    val startTime = System.nanoTime()
    val seedState = SearchState.eagerSeed(
      physicalPlan,
      matchedResults,
      requiredOutputs,
      allowPipelined,
      _ => Materialized
    )
    val (retainedPlan, _) = subPlanOf(seedState)
    if (retainedPlan.operators.isEmpty) {
      return SearchResult(
        state = seedState,
        regionDAG = new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink]),
        cost = 0.0,
        searchTimeNanoSeconds = System.nanoTime() - startTime,
        numStatesExplored = 1
      )
    }

    val (regionDAG, cost) =
      tryConnectRegionDAG(seedState) match {
        case Left(dag) => (dag, allocateResourcesAndEvaluateCost(seedState, dag))
        case Right(_) =>
          (
            new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink]),
            Double.PositiveInfinity
          )
      }

    SearchResult(
      state = seedState,
      regionDAG = regionDAG,
      cost = cost,
      searchTimeNanoSeconds = System.nanoTime() - startTime,
      numStatesExplored = 1
    )
  }

  /**
    * Top-down search over 4-edge-state [[SearchState]]s. Starts from the eager-reuse seed where
    * matched saved ports stay CacheRead, links into skipped operators are Unused, and active
    * retained links start Materialized except those pre-pipelined by chain and clean-edge pruning
    * on the retained part. Transitions Materialized non-blocking active links to Pipelined.
    */
  private[scheduling] def topDownSearch(
      globalSearch: Boolean = false,
      oChains: Boolean = true,
      oCleanEdges: Boolean = true
  ): SearchResult = {
    val startTime = System.nanoTime()
    val matSeedState = SearchState.eagerSeed(
      physicalPlan,
      matchedResults,
      requiredOutputs,
      allowPipelined,
      _ => Materialized
    )
    val (retainedPlan, _) = subPlanOf(matSeedState)
    if (retainedPlan.operators.isEmpty) {
      return SearchResult(
        state = matSeedState,
        regionDAG = new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink]),
        cost = 0.0,
        searchTimeNanoSeconds = System.nanoTime() - startTime,
        numStatesExplored = 1
      )
    }

    val retainedBlockingLinks = retainedPlan.getBlockingAndDependeeLinks
    // Starting from a state where all non-blocking retained edges are materialized
    val originalSeedMatLinks = retainedPlan.links.diff(retainedBlockingLinks)
    // Chain optimization: an edge in the same chain as a blocking edge should not be materialized
    val seedMatLinksOptimizedByChains = if (oChains) {
      val edgesInChainWithBlockingEdge = retainedPlan.maxChains
        .filter(chain => chain.intersect(retainedBlockingLinks).nonEmpty)
        .flatten
      originalSeedMatLinks.diff(edgesInChainWithBlockingEdge)
    } else {
      originalSeedMatLinks
    }
    // Clean edge optimization: a clean edge should not be materialized
    val finalSeedNonBlockingMatLinks = if (oCleanEdges) {
      seedMatLinksOptimizedByChains.intersect(retainedPlan.getNonBridgeNonBlockingLinks)
    } else {
      seedMatLinksOptimizedByChains
    }
    val seedMatLinks = retainedBlockingLinks ++ finalSeedNonBlockingMatLinks
    val seedState = SearchState.eagerSeed(
      physicalPlan,
      matchedResults,
      requiredOutputs,
      allowPipelined,
      link => if (seedMatLinks.contains(link)) Materialized else Pipelined
    )

    // Queue to hold states to be explored, starting with the seed state
    val queue: mutable.Queue[SearchState] = mutable.Queue(seedState)
    val inQueue: mutable.Set[SearchState] = mutable.Set(seedState)
    // Keep track of visited states to avoid revisiting
    val visited: mutable.Set[SearchState] = mutable.Set.empty[SearchState]
    // Initialize the bestResult with an impossible high cost for comparison
    var bestResult: SearchResult = SearchResult(
      state = seedState,
      regionDAG = new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink]),
      cost = Double.PositiveInfinity
    )

    while (queue.nonEmpty) {
      val currentState = queue.dequeue()
      inQueue.remove(currentState)
      visited.add(currentState)
      tryConnectRegionDAG(currentState) match {
        case Left(regionDAG) =>
          updateOptimumIfApplicable(currentState, regionDAG)
          addNeighborStatesToFrontier(currentState)
        // No need to explore further
        case Right(_) =>
          addNeighborStatesToFrontier(currentState)
      }

      /**
        * An internal method of top-down search that updates the current optimum if the examined state is schedulable
        * and has a lower cost.
        */
      def updateOptimumIfApplicable(
          state: SearchState,
          regionDAG: DirectedAcyclicGraph[Region, RegionLink]
      ): Unit = {
        // Calculate the current state's cost and update the bestResult if it's lower
        val cost = allocateResourcesAndEvaluateCost(state, regionDAG)
        if (cost < bestResult.cost) {
          bestResult = SearchResult(state, regionDAG, cost)
        }
      }

      /**
        * An internal method of top-down search that performs state transitions (changing a materialized active edge to
        * pipelined) to include the unvisited neighbor(s) of the current state in the frontier (i.e., the queue).
        * If using global search, all unvisited neighbors will be included. Otherwise in a greedy search, only the
        * neighbor with the lowest cost will be included.
        */
      def addNeighborStatesToFrontier(state: SearchState): Unit = {
        val candidateEdges =
          state.linksOfType(Materialized).diff(retainedBlockingLinks).filter { link =>
            allowedLinkTypes(link).contains(Pipelined)
          }
        val unvisitedNeighborStates = candidateEdges
          .map(edge => state.updated(edge, Pipelined))
          .filter(neighborState =>
            !visited.contains(neighborState) && !inQueue.contains(neighborState)
          )

        enqueueFrontier(unvisitedNeighborStates, queue, inQueue, globalSearch)
      }
    }

    val searchTime = System.nanoTime() - startTime
    bestResult.copy(
      searchTimeNanoSeconds = searchTime,
      numStatesExplored = visited.size
    )
  }

  /**
    * Generates a schedule from `regionDAG`, allocates resources and estimates cost per active
    * region using the isolated allocator and memoization map for
    * `(targetPlan, readerUrisByInputPort)`, updates `regionDAG` vertices in place with their
    * allocated configs, and returns the total cost.
    */
  private[scheduling] def allocateResourcesAndEvaluateCost(
      targetPlan: PhysicalPlan,
      readerUrisByInputPort: Map[GlobalPortIdentity, List[URI]],
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Double = {
    if (regionDAG.vertexSet().isEmpty) return 0.0
    val evaluation = evaluationOf(targetPlan, readerUrisByInputPort)
    val regionPlan =
      RegionPlan(regionDAG.vertexSet().asScala.toSet, regionDAG.edgeSet().asScala.toSet)
    val schedule = generateScheduleFromRegionPlan(regionPlan)
    // In the future we may allow multiple regions in a level and split the resources.
    schedule
      .map(level =>
        level
          .map(region => {
            val costEstimatorMemoKey = CostEstimatorMemoKey(
              physicalOpIds = region.physicalOps.map(_.id),
              physicalLinkIds = region.physicalLinks,
              portIds = region.ports,
              resourceConfig = region.resourceConfig
            )
            val (resourceConfig, regionCost) = evaluation.memoization
              .getOrElseUpdate(
                costEstimatorMemoKey,
                evaluation.estimator.allocateResourcesAndEstimateCost(region, 1)
              )
            // Update the region in the regionDAG to be the new region with resources allocated.
            val regionWithResourceConfig = region.copy(resourceConfig = Some(resourceConfig))
            replaceVertex(regionDAG, region, regionWithResourceConfig)
            regionCost
          })
          .sum
      )
      .sum
  }

  private def allocateResourcesAndEvaluateCost(
      state: SearchState,
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Double = {
    val (retainedPlan, readerUrisByInputPort) = subPlanOf(state)
    allocateResourcesAndEvaluateCost(retainedPlan, readerUrisByInputPort, regionDAG)
  }

}
