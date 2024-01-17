package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.engine.e2e.TestOperators
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.texera.workflow.common.workflow.{LogicalLink, LogicalPort}
import edu.uci.ics.texera.workflow.operators.split.SplitOpDesc
import edu.uci.ics.texera.workflow.operators.udf.python.{
  DualInputPortsPythonUDFOpDescV2,
  PythonUDFOpDescV2
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class ExpansionGreedyRegionPlanGeneratorSpec extends AnyFlatSpec with MockFactory {

  "RegionPlanGenerator" should "correctly find regions in headerlessCsv->keyword->sink workflow" in {
    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, keywordOpDesc, sink),
      List(
        LogicalLink(
          LogicalPort(headerlessCsvOpDesc.operatorIdentifier, 0),
          LogicalPort(keywordOpDesc.operatorIdentifier, 0)
        ),
        LogicalLink(
          LogicalPort(keywordOpDesc.operatorIdentifier, 0),
          LogicalPort(sink.operatorIdentifier, 0)
        )
      )
    )

    val regions = workflow.regionPlan.regions
    assert(regions.size == 1)
  }

  "RegionPlanGenerator" should "correctly find regions in csv->(csv->)->join->sink workflow" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val headerlessCsvOpDesc2 = TestOperators.headerlessSmallCsvScanOpDesc()
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(
        headerlessCsvOpDesc1,
        headerlessCsvOpDesc2,
        joinOpDesc,
        sink
      ),
      List(
        LogicalLink(
          LogicalPort(headerlessCsvOpDesc1.operatorIdentifier, 0),
          LogicalPort(joinOpDesc.operatorIdentifier, 0)
        ),
        LogicalLink(
          LogicalPort(headerlessCsvOpDesc2.operatorIdentifier, 0),
          LogicalPort(joinOpDesc.operatorIdentifier, 1)
        ),
        LogicalLink(
          LogicalPort(joinOpDesc.operatorIdentifier, 0),
          LogicalPort(sink.operatorIdentifier, 0)
        )
      )
    )

    val regions = workflow.regionPlan.regions
    assert(regions.size == 2)

    val buildRegion = regions
      .find(v =>
        v.physicalOpIds.exists(op =>
          OperatorIdentity(op.logicalOpId.id) == headerlessCsvOpDesc1.operatorIdentifier
        )
      )
      .get
    val probeRegion = regions
      .find(v =>
        v.physicalOpIds.exists(op =>
          OperatorIdentity(op.logicalOpId.id) == headerlessCsvOpDesc2.operatorIdentifier
        )
      )
      .get

    assert(workflow.regionPlan.getUpstreamRegions(probeRegion).size == 1)
    assert(workflow.regionPlan.getUpstreamRegions(probeRegion).contains(buildRegion))
    assert(buildRegion.downstreamLinks.size == 1)
    assert(
      buildRegion.downstreamLinks.exists(link =>
        link.to.logicalOpId == joinOpDesc.operatorIdentifier
      )
    )
  }

  "RegionPlanGenerator" should "correctly find regions in csv->->filter->join->sink workflow" in {
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
          LogicalPort(headerlessCsvOpDesc1.operatorIdentifier, 0),
          LogicalPort(joinOpDesc.operatorIdentifier, 0)
        ),
        LogicalLink(
          LogicalPort(headerlessCsvOpDesc1.operatorIdentifier, 0),
          LogicalPort(keywordOpDesc.operatorIdentifier, 0)
        ),
        LogicalLink(
          LogicalPort(keywordOpDesc.operatorIdentifier, 0),
          LogicalPort(joinOpDesc.operatorIdentifier, 1)
        ),
        LogicalLink(
          LogicalPort(joinOpDesc.operatorIdentifier, 0),
          LogicalPort(sink.operatorIdentifier, 0)
        )
      )
    )
    val regions = workflow.regionPlan.regions
    assert(regions.size == 2)
  }

  "RegionPlanGenerator" should "correctly find regions in buildcsv->probecsv->hashjoin->hashjoin->sink workflow" in {
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
          LogicalPort(buildCsv.operatorIdentifier, 0),
          LogicalPort(hashJoin1.operatorIdentifier, 0)
        ),
        LogicalLink(
          LogicalPort(probeCsv.operatorIdentifier, 0),
          LogicalPort(hashJoin1.operatorIdentifier, 1)
        ),
        LogicalLink(
          LogicalPort(buildCsv.operatorIdentifier, 0),
          LogicalPort(hashJoin2.operatorIdentifier, 0)
        ),
        LogicalLink(
          LogicalPort(hashJoin1.operatorIdentifier, 0),
          LogicalPort(hashJoin2.operatorIdentifier, 1)
        ),
        LogicalLink(
          LogicalPort(hashJoin2.operatorIdentifier, 0),
          LogicalPort(sink.operatorIdentifier, 0)
        )
      )
    )
    val regions = workflow.regionPlan.regions
    assert(regions.size == 2)
  }

  "RegionPlanGenerator" should "correctly find regions in csv->split->training-infer workflow" in {
    val csv = TestOperators.headerlessSmallCsvScanOpDesc()
    val split = new SplitOpDesc()
    val training = new PythonUDFOpDescV2()
    val inference = new DualInputPortsPythonUDFOpDescV2()
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(
        csv,
        split,
        training,
        inference,
        sink
      ),
      List(
        LogicalLink(
          LogicalPort(csv.operatorIdentifier, 0),
          LogicalPort(split.operatorIdentifier, 0)
        ),
        LogicalLink(
          LogicalPort(split.operatorIdentifier, 0),
          LogicalPort(training.operatorIdentifier, 0)
        ),
        LogicalLink(
          LogicalPort(training.operatorIdentifier, 0),
          LogicalPort(inference.operatorIdentifier, 0)
        ),
        LogicalLink(
          LogicalPort(split.operatorIdentifier, 1),
          LogicalPort(inference.operatorIdentifier, 1)
        ),
        LogicalLink(
          LogicalPort(inference.operatorIdentifier, 0),
          LogicalPort(sink.operatorIdentifier, 0)
        )
      )
    )
    val regions = workflow.regionPlan.regions
    assert(regions.size == 2)
  }

}
