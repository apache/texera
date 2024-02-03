package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.amber.engine.e2e.TestOperators
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.texera.workflow.common.workflow.LogicalLink
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
        ),
        LogicalLink(
          joinOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      )
    )

    val regions = workflow.regionPlan.regions
    assert(regions.size == 2)

    val buildRegion = regions
      .find(region =>
        region.getOperators
          .map(_.id)
          .exists(physicalOpId =>
            OperatorIdentity(physicalOpId.logicalOpId.id) == headerlessCsvOpDesc1.operatorIdentifier
          )
      )
      .get
    val probeRegion = regions
      .find(region =>
        region.getOperators
          .map(_.id)
          .exists(physicalOpId =>
            OperatorIdentity(physicalOpId.logicalOpId.id) == headerlessCsvOpDesc2.operatorIdentifier
          )
      )
      .get

    assert(workflow.regionPlan.getUpstreamRegions(probeRegion).size == 1)
    assert(workflow.regionPlan.getUpstreamRegions(probeRegion).contains(buildRegion))
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
          csv.operatorIdentifier,
          PortIdentity(),
          split.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          split.operatorIdentifier,
          PortIdentity(),
          training.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          training.operatorIdentifier,
          PortIdentity(),
          inference.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          split.operatorIdentifier,
          PortIdentity(1),
          inference.operatorIdentifier,
          PortIdentity(1)
        ),
        LogicalLink(
          inference.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      )
    )
    val regions = workflow.regionPlan.regions
    assert(regions.size == 2)
  }

}
