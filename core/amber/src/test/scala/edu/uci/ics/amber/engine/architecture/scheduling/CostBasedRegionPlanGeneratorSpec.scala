package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.amber.operator.TestOperators
import edu.uci.ics.amber.workflow.PortIdentity
import edu.uci.ics.texera.workflow.LogicalLink
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class CostBasedRegionPlanGeneratorSpec extends AnyFlatSpec with MockFactory {

  "CostBasedRegionPlanGenerator" should "finish bottom-up search using different pruning techniques with correct number of states explored in csv->->filter->join->sink workflow" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(
        headerlessCsvOpDesc1,
        keywordOpDesc,
        joinOpDesc,
        sink
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
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      new WorkflowContext()
    )

    val globalSearchNoPruningResult = new CostBasedRegionPlanGenerator(
      workflow.context,
      workflow.physicalPlan,
      CONTROLLER
    ).bottomUpSearch(globalSearch = true, oChains = false, oCleanEdges = false, oEarlyStop = false)

    // Should have explored all possible states (2^4 states)
    assert(globalSearchNoPruningResult.numStatesExplored == 16)

    val globalSearchOChainsResult = new CostBasedRegionPlanGenerator(
      workflow.context,
      workflow.physicalPlan,
      CONTROLLER
    ).bottomUpSearch(globalSearch = true, oCleanEdges = false, oEarlyStop = false)

    // By applying pruning based on Chains alone, it should skip 10 (8 + 2) states. 8 states where CSV->Build is
    // materialized should be skipped because this edge is in the same chain as another blocking edge.
    // Of the remaining states, 2 more states where both CSV->KeywordFilter and KeywordFilter->Probe are materialized
    // should be skipped because these two edges are in the same chain.
    assert(globalSearchOChainsResult.numStatesExplored == 6)

    val globalSearchOCleanEdgesResult = new CostBasedRegionPlanGenerator(
      workflow.context,
      workflow.physicalPlan,
      CONTROLLER
    ).bottomUpSearch(globalSearch = true, oChains = false, oEarlyStop = false)

    // By applying pruning based on Clean edges (bridges) alone, it should skip 8 states. There is one clean edge
    // in the DAG (Probe->Sink) and the 8 states where this edge is materialized should be skipped.
    assert(globalSearchOCleanEdgesResult.numStatesExplored == 8)

    val globalSearchOEarlyStopResult = new CostBasedRegionPlanGenerator(
      workflow.context,
      workflow.physicalPlan,
      CONTROLLER
    ).bottomUpSearch(globalSearch = true, oChains = false, oCleanEdges = false)

    // By applying pruning based on Early Stop alone, only 6 states that are not descendants of a schedulable states
    // should be explored.
    assert(globalSearchOEarlyStopResult.numStatesExplored == 6)

    val globalSearchAllPruningEnabledResult = new CostBasedRegionPlanGenerator(
      workflow.context,
      workflow.physicalPlan,
      CONTROLLER
    ).bottomUpSearch(globalSearch = true)

    // By combining all pruning techniques, only 3 states should be visited (1 state where both CSV->KeywordFilter and
    // KeywordFilter->Probe are pipelined, and two states where only one of CSV->KeywordFilter or KeywordFilter->Probe
    // is materialized. The other two edges should always be pipelined.)
    assert(globalSearchAllPruningEnabledResult.numStatesExplored == 3)

  }

  "CostBasedRegionPlanGenerator" should "finish top-down search using different pruning techniques with correct number of states explored in csv->->filter->join->sink workflow" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(
        headerlessCsvOpDesc1,
        keywordOpDesc,
        joinOpDesc,
        sink
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
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      new WorkflowContext()
    )

    val globalSearchNoPruningResult = new CostBasedRegionPlanGenerator(
      workflow.context,
      workflow.physicalPlan,
      CONTROLLER
    ).topDownSearch(globalSearch = true, oChains = false, oCleanEdges = false)

    // Should have explored all possible states (2^4 states)
    assert(globalSearchNoPruningResult.numStatesExplored == 16)

    val globalSearchOChainsResult = new CostBasedRegionPlanGenerator(
      workflow.context,
      workflow.physicalPlan,
      CONTROLLER
    ).topDownSearch(globalSearch = true, oCleanEdges = false)

    // By applying pruning based on Chains alone, it should start with a state where CSV->Build is pipelined because
    // this edge is in the same chain as another blocking edge. That reduces the search space to 8 states.
    assert(globalSearchOChainsResult.numStatesExplored == 8)

    val globalSearchOCleanEdgesResult = new CostBasedRegionPlanGenerator(
      workflow.context,
      workflow.physicalPlan,
      CONTROLLER
    ).topDownSearch(globalSearch = true, oChains = false)

    // By applying pruning based on Clean Edges (bridges) alone, it should start with a state where Probe->Sink is
    // pipelined because this edge is a clean edge. That reduces the search space to 8 states.
    assert(globalSearchOCleanEdgesResult.numStatesExplored == 8)

    val globalSearchAllPruningEnabledResult = new CostBasedRegionPlanGenerator(
      workflow.context,
      workflow.physicalPlan,
      CONTROLLER
    ).topDownSearch(globalSearch = true)

    // By combining both pruning techniques, the search should start with a state where both CSV->Build and Probe->Sink
    // are pipelined, reducing the search space to 4 states.
    assert(globalSearchAllPruningEnabledResult.numStatesExplored == 4)

  }

}
