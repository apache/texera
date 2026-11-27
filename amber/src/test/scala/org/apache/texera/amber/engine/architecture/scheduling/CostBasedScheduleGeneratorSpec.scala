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

import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.storage.VFSURIFactory
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{
  CachedResult,
  ExecutionMode,
  GlobalPortIdentity,
  HashPartition,
  InputPort,
  OutputPort,
  PartitionInfo,
  PhysicalLink,
  PhysicalOp,
  PhysicalPlan,
  PortIdentity,
  WorkflowContext,
  WorkflowSettings
}
import org.apache.texera.amber.engine.architecture.scheduling.config.{
  InputPortConfig,
  OutputPortConfig
}
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.{
  HashBasedShufflePartitioning,
  RoundRobinPartitioning
}
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.amber.engine.e2e.TestUtils.buildWorkflow
import org.apache.texera.amber.operator.union.UnionOpDesc
import org.apache.texera.amber.operator.{LogicalOp, TestOperators}
import org.apache.texera.common.compiler.model.LogicalLink
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI
import scala.jdk.CollectionConverters._

class CostBasedScheduleGeneratorSpec extends AnyFlatSpec {

  "CostBasedRegionPlanGenerator" should "finish bottom-up search using different pruning techniques with correct number of states explored in csv->->filter->join->filter2 workflow" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val keywordOpDesc2 = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val workflow = buildWorkflow(
      List(
        headerlessCsvOpDesc1,
        keywordOpDesc,
        joinOpDesc,
        keywordOpDesc2
      ),
      List(
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity(1)
        ),
        LogicalLink(
          joinOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc2.operatorIdentifier,
          PortIdentity()
        )
      ),
      new WorkflowContext()
    )

    val globalSearchNoPruningResult = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    ).bottomUpSearch(globalSearch = true, oChains = false, oCleanEdges = false, oEarlyStop = false)

    // Should have explored all possible states (2^4 states)
    assert(globalSearchNoPruningResult.numStatesExplored == 16)

    val globalSearchOChainsResult = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    ).bottomUpSearch(globalSearch = true, oCleanEdges = false, oEarlyStop = false)

    // By applying pruning based on Chains alone, it should skip 10 (8 + 2) states. 8 states where CSV->Build is
    // materialized should be skipped because this edge is in the same chain as another blocking edge.
    // Of the remaining states, 2 more states where both CSV->KeywordFilter and KeywordFilter->Probe are materialized
    // should be skipped because these two edges are in the same chain.
    assert(globalSearchOChainsResult.numStatesExplored == 6)

    val globalSearchOCleanEdgesResult = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    ).bottomUpSearch(globalSearch = true, oChains = false, oEarlyStop = false)

    // By applying pruning based on Clean edges (bridges) alone, it should skip 8 states. There is one clean edge
    // in the DAG (Probe->Keyword2) and the 8 states where this edge is materialized should be skipped.
    assert(globalSearchOCleanEdgesResult.numStatesExplored == 8)

    val globalSearchOEarlyStopResult = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    ).bottomUpSearch(globalSearch = true, oChains = false, oCleanEdges = false)

    // By applying pruning based on Early Stop alone, only 6 states that are not descendants of a schedulable states
    // should be explored.
    assert(globalSearchOEarlyStopResult.numStatesExplored == 6)

    val globalSearchAllPruningEnabledResult = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    ).bottomUpSearch(globalSearch = true)

    // By combining all pruning techniques, only 3 states should be visited (1 state where both CSV->KeywordFilter and
    // KeywordFilter->Probe are pipelined, and two states where only one of CSV->KeywordFilter or KeywordFilter->Probe
    // is materialized. The other two edges should always be pipelined.)
    assert(globalSearchAllPruningEnabledResult.numStatesExplored == 3)

  }

  "CostBasedRegionPlanGenerator" should "finish top-down search using different pruning techniques with correct number of states explored in csv->->filter->join->filter2 workflow" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val keywordOpDesc2 = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val workflow = buildWorkflow(
      List(
        headerlessCsvOpDesc1,
        keywordOpDesc,
        joinOpDesc,
        keywordOpDesc2
      ),
      List(
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity(1)
        ),
        LogicalLink(
          joinOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc2.operatorIdentifier,
          PortIdentity()
        )
      ),
      new WorkflowContext()
    )

    val globalSearchNoPruningResult = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    ).topDownSearch(globalSearch = true, oChains = false, oCleanEdges = false)

    // Should have explored all possible states (2^4 states)
    assert(globalSearchNoPruningResult.numStatesExplored == 16)

    val globalSearchOChainsResult = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    ).topDownSearch(globalSearch = true, oCleanEdges = false)

    // By applying pruning based on Chains alone, it should start with a state where CSV->Build is pipelined because
    // this edge is in the same chain as another blocking edge. That reduces the search space to 8 states.
    assert(globalSearchOChainsResult.numStatesExplored == 8)

    val globalSearchOCleanEdgesResult = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    ).topDownSearch(globalSearch = true, oChains = false)

    // By applying pruning based on Clean Edges (bridges) alone, it should start with a state where Probe->Keyword2 is
    // pipelined because this edge is a clean edge. That reduces the search space to 8 states.
    assert(globalSearchOCleanEdgesResult.numStatesExplored == 8)

    val globalSearchAllPruningEnabledResult = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    ).topDownSearch(globalSearch = true)

    // By combining both pruning techniques, the search should start with a state where both CSV->Build and
    // Probe->Keyword2 are pipelined, reducing the search space to 4 states.
    assert(globalSearchAllPruningEnabledResult.numStatesExplored == 4)

  }

  // MATERIALIZED ExecutionMode tests - each operator should be a separate region
  "CostBasedRegionPlanGenerator" should "create separate region for each operator in MATERIALIZED mode for simple csv workflow" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val materializedContext = new WorkflowContext(
      workflowSettings = WorkflowSettings(
        dataTransferBatchSize = 400,
        executionMode = ExecutionMode.MATERIALIZED
      )
    )
    val workflow = buildWorkflow(
      List(csvOpDesc),
      List(),
      materializedContext
    )

    val scheduleGenerator = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    )
    val result = scheduleGenerator.getFullyMaterializedSearchState

    // Should only explore 1 state (fully materialized)
    assert(result.numStatesExplored == 1)

    // Each physical operator should be in its own region
    val regions = result.regionDAG.vertexSet().asScala
    val numPhysicalOps = workflow.physicalPlan.operators.size
    assert(regions.size == numPhysicalOps, s"Expected $numPhysicalOps regions, got ${regions.size}")

    // Each region should contain exactly 1 operator
    regions.foreach { region =>
      assert(
        region.getOperators.size == 1,
        s"Expected region to have 1 operator, got ${region.getOperators.size}"
      )
    }
  }

  "CostBasedRegionPlanGenerator" should "create separate region for each operator in MATERIALIZED mode for csv->keyword workflow" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val materializedContext = new WorkflowContext(
      workflowSettings = WorkflowSettings(
        dataTransferBatchSize = 400,
        executionMode = ExecutionMode.MATERIALIZED
      )
    )
    val workflow = buildWorkflow(
      List(csvOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          csvOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      materializedContext
    )

    val scheduleGenerator = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    )
    val result = scheduleGenerator.getFullyMaterializedSearchState

    // Should only explore 1 state (fully materialized)
    assert(result.numStatesExplored == 1)

    // Each physical operator should be in its own region
    val regions = result.regionDAG.vertexSet().asScala
    val numPhysicalOps = workflow.physicalPlan.operators.size
    assert(regions.size == numPhysicalOps, s"Expected $numPhysicalOps regions, got ${regions.size}")

    // Each region should contain exactly 1 operator
    regions.foreach { region =>
      assert(
        region.getOperators.size == 1,
        s"Expected region to have 1 operator, got ${region.getOperators.size}"
      )
    }

    // All links should be materialized (represented as region links)
    val numRegionLinks = result.regionDAG.edgeSet().asScala.size
    val numPhysicalLinks = workflow.physicalPlan.links.size
    assert(
      numRegionLinks == numPhysicalLinks,
      s"Expected $numPhysicalLinks region links, got $numRegionLinks"
    )
  }

  "CostBasedRegionPlanGenerator" should "create separate region for each operator in MATERIALIZED mode for csv->keyword->count workflow" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val countOpDesc = TestOperators.aggregateAndGroupByDesc(
      "Region",
      org.apache.texera.amber.operator.aggregate.AggregationFunction.COUNT,
      List[String]()
    )
    val materializedContext = new WorkflowContext(
      workflowSettings = WorkflowSettings(
        dataTransferBatchSize = 400,
        executionMode = ExecutionMode.MATERIALIZED
      )
    )
    val workflow = buildWorkflow(
      List(csvOpDesc, keywordOpDesc, countOpDesc),
      List(
        LogicalLink(
          csvOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(),
          countOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      materializedContext
    )

    val scheduleGenerator = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    )
    val result = scheduleGenerator.getFullyMaterializedSearchState

    // Should only explore 1 state (fully materialized)
    assert(result.numStatesExplored == 1)

    // Each physical operator should be in its own region
    val regions = result.regionDAG.vertexSet().asScala
    val numPhysicalOps = workflow.physicalPlan.operators.size
    assert(regions.size == numPhysicalOps, s"Expected $numPhysicalOps regions, got ${regions.size}")

    // Each region should contain exactly 1 operator
    regions.foreach { region =>
      assert(
        region.getOperators.size == 1,
        s"Expected region to have 1 operator, got ${region.getOperators.size}"
      )
    }

    // All links should be materialized (represented as region links)
    val numRegionLinks = result.regionDAG.edgeSet().asScala.size
    val numPhysicalLinks = workflow.physicalPlan.links.size
    assert(
      numRegionLinks == numPhysicalLinks,
      s"Expected $numPhysicalLinks region links, got $numRegionLinks"
    )
  }

  "CostBasedRegionPlanGenerator" should "create separate region for each operator in MATERIALIZED mode for join workflow" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val headerlessCsvOpDesc2 = TestOperators.headerlessSmallCsvScanOpDesc()
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val materializedContext = new WorkflowContext(
      workflowSettings = WorkflowSettings(
        dataTransferBatchSize = 400,
        executionMode = ExecutionMode.MATERIALIZED
      )
    )
    val workflow = buildWorkflow(
      List(
        headerlessCsvOpDesc1,
        headerlessCsvOpDesc2,
        joinOpDesc
      ),
      List(
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          headerlessCsvOpDesc2.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity(1)
        )
      ),
      materializedContext
    )

    val scheduleGenerator = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    )
    val result = scheduleGenerator.getFullyMaterializedSearchState

    // Should only explore 1 state (fully materialized)
    assert(result.numStatesExplored == 1)

    // Each physical operator should be in its own region
    val regions = result.regionDAG.vertexSet().asScala
    val numPhysicalOps = workflow.physicalPlan.operators.size
    assert(regions.size == numPhysicalOps, s"Expected $numPhysicalOps regions, got ${regions.size}")

    // Each region should contain exactly 1 operator
    regions.foreach { region =>
      assert(
        region.getOperators.size == 1,
        s"Expected region to have 1 operator, got ${region.getOperators.size}"
      )
    }

    // All links should be materialized (represented as region links)
    val numRegionLinks = result.regionDAG.edgeSet().asScala.size
    val numPhysicalLinks = workflow.physicalPlan.links.size
    assert(
      numRegionLinks == numPhysicalLinks,
      s"Expected $numPhysicalLinks region links, got $numRegionLinks"
    )
  }

  "CostBasedRegionPlanGenerator" should "create separate region for each operator in MATERIALIZED mode for complex csv->->filter->join->filter2 workflow" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val keywordOpDesc2 = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val materializedContext = new WorkflowContext(
      workflowSettings = WorkflowSettings(
        dataTransferBatchSize = 400,
        executionMode = ExecutionMode.MATERIALIZED
      )
    )
    val workflow = buildWorkflow(
      List(
        headerlessCsvOpDesc1,
        keywordOpDesc,
        joinOpDesc,
        keywordOpDesc2
      ),
      List(
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity(1)
        ),
        LogicalLink(
          joinOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc2.operatorIdentifier,
          PortIdentity()
        )
      ),
      materializedContext
    )

    val scheduleGenerator = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    )
    val result = scheduleGenerator.getFullyMaterializedSearchState

    // Should only explore 1 state (fully materialized)
    assert(result.numStatesExplored == 1)

    // Each physical operator should be in its own region
    val regions = result.regionDAG.vertexSet().asScala
    val numPhysicalOps = workflow.physicalPlan.operators.size
    assert(regions.size == numPhysicalOps, s"Expected $numPhysicalOps regions, got ${regions.size}")

    // Each region should contain exactly 1 operator
    regions.foreach { region =>
      assert(
        region.getOperators.size == 1,
        s"Expected region to have 1 operator, got ${region.getOperators.size}"
      )
    }

    // All links should be materialized (represented as region links)
    val numRegionLinks = result.regionDAG.edgeSet().asScala.size
    val numPhysicalLinks = workflow.physicalPlan.links.size
    assert(
      numRegionLinks == numPhysicalLinks,
      s"Expected $numPhysicalLinks region links, got $numRegionLinks"
    )
  }

  "CostBasedScheduleGenerator.effectiveExecutionMode" should
    "force MATERIALIZED when an operator requires it, even if PIPELINED is requested" in {
    val workflow = buildWorkflow(
      List(TestOperators.headerlessSmallCsvScanOpDesc()),
      List(),
      new WorkflowContext()
    )
    val planRequiringMaterialization = PhysicalPlan(
      workflow.physicalPlan.operators.map(_.withRequiresMaterializedExecution(true)),
      workflow.physicalPlan.links
    )
    assert(
      CostBasedScheduleGenerator.effectiveExecutionMode(
        planRequiringMaterialization,
        ExecutionMode.PIPELINED
      ) == ExecutionMode.MATERIALIZED
    )
  }

  it should "keep the requested mode when no operator requires materialization" in {
    val workflow = buildWorkflow(
      List(TestOperators.headerlessSmallCsvScanOpDesc()),
      List(),
      new WorkflowContext()
    )
    val plan = workflow.physicalPlan
    assert(
      CostBasedScheduleGenerator.effectiveExecutionMode(plan, ExecutionMode.PIPELINED) ==
        ExecutionMode.PIPELINED
    )
    assert(
      CostBasedScheduleGenerator.effectiveExecutionMode(plan, ExecutionMode.MATERIALIZED) ==
        ExecutionMode.MATERIALIZED
    )
  }

  "CostBasedRegionPlanGenerator" should "finish bottom-up greedy search (globalSearch=false) in csv->->filter->join->filter2 workflow" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val keywordOpDesc2 = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val workflow = buildWorkflow(
      List(
        headerlessCsvOpDesc1,
        keywordOpDesc,
        joinOpDesc,
        keywordOpDesc2
      ),
      List(
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity(1)
        ),
        LogicalLink(
          joinOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc2.operatorIdentifier,
          PortIdentity()
        )
      ),
      new WorkflowContext()
    )

    val scheduleGenerator = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    )

    // Greedy search (globalSearch = false): at each schedulable/unschedulable state the frontier keeps only
    // the single lowest-cost neighbor, driving the greedy branch (filteredNeighborStates.nonEmpty + minBy).
    val greedyResult = scheduleGenerator.bottomUpSearch(globalSearch = false)

    // A schedulable plan should have been found: the region DAG is non-empty and the cost is finite.
    assert(greedyResult.regionDAG.vertexSet().asScala.nonEmpty)
    assert(greedyResult.cost < Double.PositiveInfinity)

    // The greedy search enqueues at most one neighbor per explored state, and each bottom-up transition materializes
    // one more edge, so the number of states it explores is bounded linearly by the number of physical links. This is
    // a guaranteed property of greedy search, unlike a comparison against global search whose explored count depends on
    // early-stop pruning and queue ordering.
    assert(greedyResult.numStatesExplored <= scheduleGenerator.physicalPlan.links.size + 1)

    // The chosen state is a 4-edge-state SearchState satisfying the consumption condition whose
    // materialized links are links of the physical plan.
    assert(
      greedyResult.state.satisfiesConsumptionCondition(
        scheduleGenerator.physicalPlan,
        scheduleGenerator.requiredOutputs
      )
    )
    assert(
      greedyResult.state
        .linksOfType(EdgeType.Materialized)
        .subsetOf(scheduleGenerator.physicalPlan.links)
    )
  }

  "CostBasedRegionPlanGenerator" should "finish top-down greedy search (globalSearch=false) in csv->->filter->join->filter2 workflow" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val keywordOpDesc2 = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val workflow = buildWorkflow(
      List(
        headerlessCsvOpDesc1,
        keywordOpDesc,
        joinOpDesc,
        keywordOpDesc2
      ),
      List(
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity(1)
        ),
        LogicalLink(
          joinOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc2.operatorIdentifier,
          PortIdentity()
        )
      ),
      new WorkflowContext()
    )

    val scheduleGenerator = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      COORDINATOR
    )

    // Greedy search (globalSearch = false): starting from the fully materialized seed state, each transition
    // keeps only the single lowest-cost neighbor, driving the greedy branch (unvisitedNeighborStates.nonEmpty + minBy)
    // over both the schedulable (Left) and unschedulable-intermediate (Right) legs.
    val greedyResult = scheduleGenerator.topDownSearch(globalSearch = false)

    // A schedulable plan should have been found: the region DAG is non-empty and the cost is finite.
    assert(greedyResult.regionDAG.vertexSet().asScala.nonEmpty)
    assert(greedyResult.cost < Double.PositiveInfinity)

    // The chosen state is a 4-edge-state SearchState satisfying the consumption condition whose
    // materialized links are links of the physical plan.
    assert(
      greedyResult.state.satisfiesConsumptionCondition(
        scheduleGenerator.physicalPlan,
        scheduleGenerator.requiredOutputs
      )
    )
    assert(
      greedyResult.state
        .linksOfType(EdgeType.Materialized)
        .subsetOf(scheduleGenerator.physicalPlan.links)
    )
  }

  // ---------------------------------------------------------------------------
  // Saved-result reads and extended RegionPlan in CostBasedScheduleGenerator
  // ---------------------------------------------------------------------------

  private def csvKeywordWorkflow(context: WorkflowContext) = {
    val csv = TestOperators.headerlessSmallCsvScanOpDesc()
    val keyword = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    buildWorkflow(
      List(csv, keyword),
      List(
        LogicalLink(
          csv.operatorIdentifier,
          PortIdentity(0),
          keyword.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      context
    )
  }

  private def scheduleSubPlan(
      generator: CostBasedScheduleGenerator,
      retainedPlan: PhysicalPlan,
      matEdges: Set[PhysicalLink],
      readerUrisByInputPort: Map[GlobalPortIdentity, List[URI]]
  ): Schedule = {
    val regionDAG =
      generator.tryConnectRegionDAG(retainedPlan, matEdges, readerUrisByInputPort) match {
        case Left(dag)    => dag
        case Right(graph) => fail(s"expected acyclic region DAG, got cycle: $graph")
      }
    generator.allocateResourcesAndEvaluateCost(retainedPlan, readerUrisByInputPort, regionDAG)
    generator.generateScheduleFromRegionPlan(
      RegionPlan(regionDAG.vertexSet().asScala.toSet, regionDAG.edgeSet().asScala.toSet)
    )
  }

  "CostBasedScheduleGenerator" should "add the reader URIs of cache read inputs to the input port configs" in {
    val context = new WorkflowContext(WorkflowIdentity(3L), ExecutionIdentity(2L))
    val workflow = csvKeywordWorkflow(context)
    val fullPlan = workflow.physicalPlan
    val link = fullPlan.links.head
    val keywordOp = fullPlan.getOperator(link.toOpId)
    // The part of the plan that runs when the csv operator is skipped: the keyword operator
    // alone. Its upstream link leaves the sub-plan, while its own link list still names it.
    val retainedPart = fullPlan.getSubPlan(Set(keywordOp.id))
    assert(retainedPart.links.isEmpty)
    assert(retainedPart.getOperator(keywordOp.id).getInputLinks() == List(link))
    val inputPort = GlobalPortIdentity(keywordOp.id, PortIdentity(0), input = true)
    val readerUri = VFSURIFactory.createPortBaseURI(
      WorkflowIdentity(3L),
      ExecutionIdentity(1L),
      GlobalPortIdentity(link.fromOpId, link.fromPortId)
    )
    val generator = new CostBasedScheduleGenerator(context, fullPlan, COORDINATOR)

    val withCacheRead = scheduleSubPlan(
      generator,
      retainedPart,
      Set.empty,
      Map(inputPort -> List(readerUri))
    )
    val regions = withCacheRead.getRegions
    assert(regions.size == 1)
    regions.head.resourceConfig.get.portConfigs(inputPort) match {
      case InputPortConfig(pairs) => assert(pairs.map(_._1) == List(readerUri))
      case other                  => fail(s"expected a reader for the saved result, got $other")
    }
    // The operator reads from storage, so it starts first, as after a materialized link.
    assert(regions.head.getStarterOperators.map(_.id) == Set(keywordOp.id))

    val withoutCacheRead = scheduleSubPlan(generator, retainedPart, Set.empty, Map.empty)
    assert(!withoutCacheRead.getRegions.head.resourceConfig.get.portConfigs.contains(inputPort))
  }

  /**
    * Two csv scans into the one input port of a union, with the first scan skipped: the part
    * of the plan that runs is the second scan and the union, and the union's input port reads
    * the first scan's saved result besides the second scan's link. The union's own link list
    * still names the first scan's link.
    */
  private def scheduleUnionReadingOneInputFromCache(materializeRemainingLink: Boolean) = {
    val context = new WorkflowContext(WorkflowIdentity(3L), ExecutionIdentity(2L))
    val csv1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val csv2 = TestOperators.headerlessSmallCsvScanOpDesc()
    val union = new UnionOpDesc()
    val workflow = buildWorkflow(
      List(csv1, csv2, union),
      List(csv1, csv2).map(csv =>
        LogicalLink(
          csv.operatorIdentifier,
          PortIdentity(0),
          union.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      context
    )
    val fullPlan = workflow.physicalPlan
    def idOf(op: LogicalOp) = fullPlan.getPhysicalOpsOfLogicalOp(op.operatorIdentifier).head.id
    val skippedLink = fullPlan.links.find(_.fromOpId == idOf(csv1)).get
    val retainedPart = fullPlan.getSubPlan(Set(idOf(csv2), idOf(union)))
    val matEdges = if (materializeRemainingLink) retainedPart.links else Set.empty[PhysicalLink]
    val unionInput = GlobalPortIdentity(idOf(union), PortIdentity(0), input = true)
    val savedUri = VFSURIFactory.createPortBaseURI(
      WorkflowIdentity(3L),
      ExecutionIdentity(1L),
      GlobalPortIdentity(skippedLink.fromOpId, skippedLink.fromPortId)
    )
    val generator = new CostBasedScheduleGenerator(context, fullPlan, COORDINATOR)
    val schedule = scheduleSubPlan(
      generator,
      retainedPart,
      matEdges,
      Map(unionInput -> List(savedUri))
    )
    (schedule, idOf(csv2), unionInput, savedUri)
  }

  it should "give an input port both the location of a materialized link and a saved result" in {
    val (schedule, csv2, unionInput, savedUri) =
      scheduleUnionReadingOneInputFromCache(materializeRemainingLink = true)
    val regionOf = schedule.levelSets.toList.flatMap {
      case (level, regions) => regions.map(region => region -> level)
    }
    val (csv2Region, csv2Level) = regionOf.find(_._1.getOperators.exists(_.id == csv2)).get
    val (unionRegion, unionLevel) =
      regionOf.find(_._1.getOperators.exists(_.id == unionInput.opId)).get
    assert(csv2Level < unionLevel)
    val csv2OutputUri =
      csv2Region.resourceConfig.get.portConfigs(GlobalPortIdentity(csv2, PortIdentity(0))) match {
        case config: OutputPortConfig => config.storageURIBase
        case other                    => fail(s"expected an output URI for the scan, got $other")
      }
    unionRegion.resourceConfig.get.portConfigs(unionInput) match {
      case InputPortConfig(pairs) =>
        assert(pairs.size == 2)
        assert(pairs.map(_._1).toSet == Set(csv2OutputUri, savedUri))
      case other => fail(s"expected a reader for both locations, got $other")
    }
  }

  it should "give an input port with a pipelined link only the location of the saved result" in {
    val (schedule, csv2, unionInput, savedUri) =
      scheduleUnionReadingOneInputFromCache(materializeRemainingLink = false)
    val regions = schedule.getRegions
    assert(regions.size == 1)
    val region = regions.head
    assert(region.getOperators.map(_.id) == Set(csv2, unionInput.opId))
    assert(
      region.getLinks.map(link => (link.fromOpId, link.toOpId)) == Set((csv2, unionInput.opId))
    )
    region.resourceConfig.get.portConfigs(unionInput) match {
      case InputPortConfig(pairs) => assert(pairs.map(_._1) == List(savedUri))
      case other                  => fail(s"expected a reader for the saved result, got $other")
    }
  }

  it should "put the context's warehouse on every new output URI" in {
    val context =
      new WorkflowContext(WorkflowIdentity(3L), ExecutionIdentity(2L), warehouse = Some("wh1"))
    val workflow = csvKeywordWorkflow(context)
    val (schedule, _) =
      new CostBasedScheduleGenerator(context, workflow.physicalPlan, COORDINATOR).generate()
    val outputUris = schedule.getRegions.flatMap(_.resourceConfig.get.portConfigs.values).collect {
      case config: OutputPortConfig => config.storageURIBase.toString
    }
    assert(outputUris.nonEmpty)
    assert(outputUris.forall(_.contains("/wh/wh1/")))
  }

  it should "partition the output of an operator reading a saved result as behind a materialized link and isolate distinct sub-plans" in {
    val context = new WorkflowContext(WorkflowIdentity(3L), ExecutionIdentity(2L))
    def id(name: String) = PhysicalOpIdentity(OperatorIdentity(name), "main")
    def parallelOp(name: String, inputs: Int, requirement: Option[PartitionInfo]): PhysicalOp =
      PhysicalOp
        .oneToOnePhysicalOp(id(name), context.workflowId, context.executionId, OpExecInitInfo.Empty)
        .withInputPorts((0 until inputs).map(i => InputPort(PortIdentity(i))).toList)
        .withOutputPorts(List(OutputPort(PortIdentity(0))))
        .withParallelizable(true)
        .withSuggestedWorkerNum(2)
        .withPartitionRequirement(if (inputs == 0) List.empty else List(requirement))
    val xy = PhysicalLink(id("x"), PortIdentity(0), id("y"), PortIdentity(0))
    val yz = PhysicalLink(id("y"), PortIdentity(0), id("z"), PortIdentity(0))
    val x = parallelOp("x", inputs = 0, None).addOutputLink(xy)
    val y =
      parallelOp("y", inputs = 1, Some(HashPartition(List("k")))).addInputLink(xy).addOutputLink(yz)
    val z = parallelOp("z", inputs = 1, None).addInputLink(yz)
    val fullPlan = PhysicalPlan(Set(x, y, z), Set(xy, yz))
    val retainedPart = fullPlan.getSubPlan(Set(id("y"), id("z")))
    val yInput = GlobalPortIdentity(id("y"), PortIdentity(0), input = true)
    val savedLocation = VFSURIFactory.createPortBaseURI(
      context.workflowId,
      ExecutionIdentity(1L),
      GlobalPortIdentity(id("x"), PortIdentity(0))
    )

    val generator = new CostBasedScheduleGenerator(context, fullPlan, COORDINATOR)
    // First evaluate the full pipelined plan on the same evaluator: x -> y hash-shuffles into y,
    // so y passes HashPartition onto y -> z.
    val fullSchedule = scheduleSubPlan(generator, fullPlan, Set.empty, Map.empty)
    assert(
      fullSchedule.getRegions.head.resourceConfig.get
        .linkConfigs(yz)
        .partitioning
        .isInstanceOf[HashBasedShufflePartitioning]
    )

    // Next evaluate the sub-plan {y, z} with y reading x's saved result on the SAME evaluator:
    // isolated per-sub-plan allocator state + fullPlan source check must give RoundRobinPartitioning,
    // identical to materializing x -> y on the full plan.
    val cacheReadSchedule = scheduleSubPlan(
      generator,
      retainedPart,
      Set.empty,
      Map(yInput -> List(savedLocation))
    )
    val materializedFullSchedule = scheduleSubPlan(
      new CostBasedScheduleGenerator(context, fullPlan, COORDINATOR),
      fullPlan,
      Set(xy),
      Map.empty
    )
    val cacheReadPartitioning = cacheReadSchedule.getRegions
      .flatMap(_.resourceConfig.get.linkConfigs.get(yz))
      .map(_.partitioning.getClass)
    val materializedPartitioning = materializedFullSchedule.getRegions
      .flatMap(_.resourceConfig.get.linkConfigs.get(yz))
      .map(_.partitioning.getClass)
    assert(cacheReadPartitioning == List(classOf[RoundRobinPartitioning]))
    assert(cacheReadPartitioning == materializedPartitioning)
  }

  it should "return an empty region DAG with zero cost when the sub-plan has no operators" in {
    val context = new WorkflowContext(WorkflowIdentity(3L), ExecutionIdentity(2L))
    val workflow = csvKeywordWorkflow(context)
    val generator =
      new CostBasedScheduleGenerator(context, workflow.physicalPlan, COORDINATOR)
    val emptyPlan = PhysicalPlan(Set.empty, Set.empty)
    val regionDAG = generator.tryConnectRegionDAG(emptyPlan, Set.empty, Map.empty) match {
      case Left(dag)    => dag
      case Right(graph) => fail(s"expected empty acyclic DAG, got $graph")
    }
    assert(regionDAG.vertexSet().isEmpty)
    assert(generator.allocateResourcesAndEvaluateCost(emptyPlan, Map.empty, regionDAG) == 0.0)
  }

  it should "refuse cache reader URIs keyed by an output port in createRegions" in {
    val context = new WorkflowContext(WorkflowIdentity(3L), ExecutionIdentity(2L))
    val workflow = csvKeywordWorkflow(context)
    val generator =
      new CostBasedScheduleGenerator(context, workflow.physicalPlan, COORDINATOR)
    val outputPort =
      GlobalPortIdentity(PhysicalOpIdentity(OperatorIdentity("x"), "main"), PortIdentity(0))
    assertThrows[IllegalArgumentException](
      generator.createRegions(
        workflow.physicalPlan,
        Set.empty,
        Map(outputPort -> List(new URI("vfs:///x")))
      )
    )
  }

  // ---------------------------------------------------------------------------
  // 4-edge-state CostBasedScheduleGenerator search and extended RegionPlan
  // ---------------------------------------------------------------------------

  it should "evaluate a single zero-cost empty region DAG state when every operator is skipped" in {
    val context = new WorkflowContext(WorkflowIdentity(7L), ExecutionIdentity(2L))
    val workflow = csvKeywordWorkflow(context)
    val terminalId = workflow.logicalPlan.getTerminalOperatorIds.head
    val terminalOp = workflow.physicalPlan.operators.find(_.id.logicalOpId == terminalId).get
    val terminalOut = GlobalPortIdentity(terminalOp.id, PortIdentity(0))
    val savedUri = VFSURIFactory.createPortBaseURI(
      context.workflowId,
      ExecutionIdentity(1L),
      terminalOut
    )
    context.matchedResults = Map(terminalOut -> CachedResult(savedUri, Some(12L)))

    val generator = new CostBasedScheduleGenerator(context, workflow.physicalPlan, COORDINATOR)
    List(
      generator.getFullyMaterializedSearchState,
      generator.bottomUpSearch(globalSearch = true),
      generator.topDownSearch(globalSearch = true)
    ).foreach { result =>
      assert(result.numStatesExplored == 1)
      assert(result.regionDAG.vertexSet().isEmpty)
      assert(result.cost == 0.0)
      assert(
        result.state.skippedOperators(workflow.physicalPlan, generator.requiredOutputs) ==
          workflow.physicalPlan.operators.map(_.id)
      )
    }

    val (schedule, returnedPlan) = generator.generate()
    assert(returnedPlan eq workflow.physicalPlan)
    assert(schedule.levelSets.keySet == Set(0))
    assert(schedule.levelSets(0).forall(_.skipped))
  }

  it should "skip an upstream cached branch, extend RegionPlan with skip regions, and break a downstream HashJoin cycle" in {
    // upstreamScan -> upstreamFilter -> join (build, port 0)
    //                                \-> probeFilter -> join (probe, port 1) -> tailFilter
    // Saving upstreamScan skips upstreamScan while keeping the downstream HashJoin cycle intact.
    val upstreamScan = TestOperators.headerlessSmallCsvScanOpDesc()
    val upstreamFilter = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val probeFilter = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val join = TestOperators.joinOpDesc("column-1", "column-1")
    val tailFilter = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val context = new WorkflowContext(WorkflowIdentity(7L), ExecutionIdentity(2L))
    val workflow = buildWorkflow(
      List(upstreamScan, upstreamFilter, probeFilter, join, tailFilter),
      List(
        LogicalLink(
          upstreamScan.operatorIdentifier,
          PortIdentity(0),
          upstreamFilter.operatorIdentifier,
          PortIdentity(0)
        ),
        LogicalLink(
          upstreamFilter.operatorIdentifier,
          PortIdentity(0),
          join.operatorIdentifier,
          PortIdentity(0)
        ),
        LogicalLink(
          upstreamFilter.operatorIdentifier,
          PortIdentity(0),
          probeFilter.operatorIdentifier,
          PortIdentity(0)
        ),
        LogicalLink(
          probeFilter.operatorIdentifier,
          PortIdentity(0),
          join.operatorIdentifier,
          PortIdentity(1)
        ),
        LogicalLink(
          join.operatorIdentifier,
          PortIdentity(0),
          tailFilter.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      context
    )
    val plan = workflow.physicalPlan
    val scanOp = plan.getPhysicalOpsOfLogicalOp(upstreamScan.operatorIdentifier).head
    val upstreamFilterOp = plan.getPhysicalOpsOfLogicalOp(upstreamFilter.operatorIdentifier).head
    val scanOut = GlobalPortIdentity(scanOp.id, PortIdentity(0))
    val savedUri = VFSURIFactory.createPortBaseURI(
      context.workflowId,
      ExecutionIdentity(1L),
      scanOut
    )
    context.matchedResults = Map(scanOut -> CachedResult(savedUri, Some(8L)))

    val generator = new CostBasedScheduleGenerator(context, plan, COORDINATOR)
    val results = List(
      generator.bottomUpSearch(globalSearch = true),
      generator.bottomUpSearch(globalSearch = false),
      generator.topDownSearch(globalSearch = true),
      generator.topDownSearch(globalSearch = false)
    )
    results.foreach { result =>
      assert(result.cost < Double.PositiveInfinity)
      assert(result.state.skippedOperators(plan, generator.requiredOutputs) == Set(scanOp.id))
      assert(
        result.regionDAG.vertexSet().asScala.flatMap(_.getOperators.map(_.id)).toSet ==
          plan.operators.map(_.id) - scanOp.id
      )
      val upstreamFilterInput =
        GlobalPortIdentity(upstreamFilterOp.id, PortIdentity(0), input = true)
      val readers = result.regionDAG
        .vertexSet()
        .asScala
        .flatMap(_.resourceConfig.get.portConfigs.get(upstreamFilterInput))
      assert(readers.size == 1)
      readers.head match {
        case InputPortConfig(pairs) => assert(pairs.map(_._1) == List(savedUri))
        case other                  => fail(s"expected saved reader on upstreamFilter, got $other")
      }
      val regionPlan = generator.buildRegionPlan(result.state, result.regionDAG)
      val skipRegions = regionPlan.regions.filter(_.skipped)
      val activeRegions = regionPlan.regions.filterNot(_.skipped)
      assert(skipRegions.size == 1)
      assert(skipRegions.flatMap(_.getOperators.map(_.id)) == Set(scanOp.id))
      assert(activeRegions == result.regionDAG.vertexSet().asScala.toSet)
      assert(regionPlan.regionLinks == result.regionDAG.edgeSet().asScala.toSet)
      assert(regionPlan.getRegionOfPortId(scanOut).contains(skipRegions.head))

      val fullSchedule = generator.generateScheduleFromRegionPlan(regionPlan)
      assert(fullSchedule.levelSets(0).forall(_.skipped))
      assert(fullSchedule.levelSets(0).flatMap(_.getOperators.map(_.id)) == Set(scanOp.id))
      assert(fullSchedule.levelSets.size >= 3)
    }
  }

  it should "schedule partial cache reuse under ExecutionMode.MATERIALIZED and keep retained links materialized on fallback" in {
    val csv = TestOperators.headerlessSmallCsvScanOpDesc()
    val filter1 = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val filter2 = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val context = new WorkflowContext(
      workflowId = WorkflowIdentity(7L),
      executionId = ExecutionIdentity(2L),
      workflowSettings = WorkflowSettings(executionMode = ExecutionMode.MATERIALIZED)
    )
    val workflow = buildWorkflow(
      List(csv, filter1, filter2),
      List(
        LogicalLink(
          csv.operatorIdentifier,
          PortIdentity(0),
          filter1.operatorIdentifier,
          PortIdentity(0)
        ),
        LogicalLink(
          filter1.operatorIdentifier,
          PortIdentity(0),
          filter2.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      context
    )
    val plan = workflow.physicalPlan
    val csvOp = plan.getPhysicalOpsOfLogicalOp(csv.operatorIdentifier).head
    val filter1Op = plan.getPhysicalOpsOfLogicalOp(filter1.operatorIdentifier).head
    val filter2Op = plan.getPhysicalOpsOfLogicalOp(filter2.operatorIdentifier).head
    val filter1ToFilter2 = plan.links.find(_.fromOpId == filter1Op.id).get
    val csvOut = GlobalPortIdentity(csvOp.id, PortIdentity(0))
    val savedUri = VFSURIFactory.createPortBaseURI(
      context.workflowId,
      ExecutionIdentity(1L),
      csvOut
    )
    context.matchedResults = Map(csvOut -> CachedResult(savedUri, Some(7L)))

    val generator = new CostBasedScheduleGenerator(context, plan, COORDINATOR)
    assert(!generator.allowPipelined)

    val (schedule, returnedPlan) = generator.generate()
    assert(returnedPlan eq plan)
    assert(schedule.levelSets.keySet == Set(0, 1, 2))
    assert(schedule.levelSets(0).forall(_.skipped))
    assert(schedule.levelSets(0).flatMap(_.getOperators.map(_.id)) == Set(csvOp.id))

    val filter1Region = schedule.levelSets(1).head
    assert(!filter1Region.skipped)
    assert(filter1Region.getOperators.map(_.id) == Set(filter1Op.id))
    val filter1Input = GlobalPortIdentity(filter1Op.id, PortIdentity(0), input = true)
    filter1Region.resourceConfig.get.portConfigs(filter1Input) match {
      case InputPortConfig(pairs) => assert(pairs.map(_._1) == List(savedUri))
      case other                  => fail(s"expected saved reader URI on filter1 in MATERIALIZED mode, got $other")
    }

    val filter2Region = schedule.levelSets(2).head
    assert(!filter2Region.skipped)
    assert(filter2Region.getOperators.map(_.id) == Set(filter2Op.id))

    // Timeout fallback path: bottomUpSearch under !allowPipelined keeps all retained links Materialized.
    val fallbackResult = generator.bottomUpSearch()
    assert(fallbackResult.numStatesExplored == 1)
    assert(fallbackResult.state.skippedOperators(plan, generator.requiredOutputs) == Set(csvOp.id))
    assert(fallbackResult.state.linksOfType(EdgeType.Materialized) == Set(filter1ToFilter2))
  }

}
