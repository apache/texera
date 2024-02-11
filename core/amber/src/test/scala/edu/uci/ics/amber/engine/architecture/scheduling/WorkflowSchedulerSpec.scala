package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.amber.engine.e2e.TestOperators
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.texera.workflow.common.workflow.LogicalLink
import org.scalatest.flatspec.AnyFlatSpec

class WorkflowSchedulerSpec extends AnyFlatSpec {

  "Scheduler" should "correctly schedule regions in headerlessCsv->keyword->sink workflow" in {
    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, keywordOpDesc, sink),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      )
    )
    assert(workflow.regionPlan.regions.size == 1)
    workflow.regionPlan.topologicalIterator().zip(Iterator(3)).foreach {
      case (regionId, opCount) => assert(workflow.regionPlan.getRegion(regionId).getOperators.size == opCount)
    }

    workflow.regionPlan.topologicalIterator().zip(Iterator(2)).foreach {
      case (regionId, linkCount) => assert(workflow.regionPlan.getRegion(regionId).getLinks.size == linkCount)
    }

    workflow.regionPlan.topologicalIterator().zip(Iterator(4)).foreach {
      case (regionId, portCount) => assert(workflow.regionPlan.getRegion(regionId).getPorts.size == portCount)
    }

  }

  "Scheduler" should "correctly schedule regions in buildcsv->probecsv->hashjoin->hashjoin->sink workflow" in {
    val buildCsv = TestOperators.headerlessSmallCsvScanOpDesc()
    val probeCsv = TestOperators.smallCsvScanOpDesc()
    val hashJoin1 = TestOperators.joinOpDesc("column-1", "Region")
    val hashJoin2 = TestOperators.joinOpDesc("column-2", "Country")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(
        buildCsv,
        probeCsv,
        hashJoin1,
        hashJoin2,
        sink
      ),
      List(
        LogicalLink(
          buildCsv.operatorIdentifier,
          PortIdentity(),
          hashJoin1.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          probeCsv.operatorIdentifier,
          PortIdentity(),
          hashJoin1.operatorIdentifier,
          PortIdentity(1)
        ),
        LogicalLink(
          buildCsv.operatorIdentifier,
          PortIdentity(),
          hashJoin2.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          hashJoin1.operatorIdentifier,
          PortIdentity(),
          hashJoin2.operatorIdentifier,
          PortIdentity(1)
        ),
        LogicalLink(
          hashJoin2.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      )
    )
    assert(workflow.regionPlan.regions.size == 2)
    workflow.regionPlan.topologicalIterator().zip(Iterator(5, 4)).foreach {
      case (regionId, opCount) => assert(workflow.regionPlan.getRegion(regionId).getOperators.size == opCount)
    }

    workflow.regionPlan.topologicalIterator().zip(Iterator(4, 3)).foreach {
      case (regionId, linkCount) => assert(workflow.regionPlan.getRegion(regionId).getLinks.size == linkCount)
    }

    workflow.regionPlan.topologicalIterator().zip(Iterator(7, 6)).foreach {
      case (regionId, portCount) => assert(workflow.regionPlan.getRegion(regionId).getPorts.size == portCount)
    }
  }

}
