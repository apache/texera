package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.amber.operator.TestOperators
import edu.uci.ics.amber.workflow.PortIdentity
import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.workflow.LogicalLink
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec

class DefaultCostEstimatorSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  it should "use fallback method when no past statistics are available" in {

    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, keywordOpDesc, sink),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(0),
          keywordOpDesc.operatorIdentifier,
          PortIdentity(0)
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(0),
          sink.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    )

    val costEstimator = new DefaultCostEstimator(
      workflow.context,
      CONTROLLER
    )

    val region = Region(
      id = RegionIdentity(0),
      physicalOps = workflow.physicalPlan.operators,
      physicalLinks = workflow.physicalPlan.links
    )

    val costOfRegion = costEstimator.estimate(region, 1)

    assert(costOfRegion == 0)
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

}
