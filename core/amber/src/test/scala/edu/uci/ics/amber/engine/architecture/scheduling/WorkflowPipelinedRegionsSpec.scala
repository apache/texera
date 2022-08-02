package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.{OperatorIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.e2e.TestOperators
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.{
  BreakpointInfo,
  OperatorLink,
  OperatorPort,
  WorkflowCompiler,
  WorkflowInfo
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class WorkflowPipelinedRegionsSpec extends AnyFlatSpec with MockFactory {

  def buildWorkflow(
      operators: mutable.MutableList[OperatorDescriptor],
      links: mutable.MutableList[OperatorLink]
  ): Workflow = {
    val context = new WorkflowContext
    context.jobId = "workflow-test"

    val texeraWorkflowCompiler = new WorkflowCompiler(
      WorkflowInfo(operators, links, mutable.MutableList[BreakpointInfo]()),
      context
    )
    texeraWorkflowCompiler.amberWorkflow(WorkflowIdentity("workflow-test"), new OpResultStorage())
  }

  "Pipelined Regions" should "correctly find regions in headerlessCsv->keyword->sink workflow" in {
    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      mutable.MutableList[OperatorDescriptor](headerlessCsvOpDesc, keywordOpDesc, sink),
      mutable.MutableList[OperatorLink](
        OperatorLink(
          OperatorPort(headerlessCsvOpDesc.operatorID, 0),
          OperatorPort(keywordOpDesc.operatorID, 0)
        ),
        OperatorLink(OperatorPort(keywordOpDesc.operatorID, 0), OperatorPort(sink.operatorID, 0))
      )
    )

    val pipelinedRegions = new WorkflowPipelinedRegions(workflow)
    assert(pipelinedRegions.pipelinedRegionsDAG.vertexSet().size == 1)
  }

  "Pipelined Regions" should "correctly find regions in csv->(csv->)->join->sink workflow" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val headerlessCsvOpDesc2 = TestOperators.headerlessSmallCsvScanOpDesc()
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      mutable.MutableList[OperatorDescriptor](
        headerlessCsvOpDesc1,
        headerlessCsvOpDesc2,
        joinOpDesc,
        sink
      ),
      mutable.MutableList[OperatorLink](
        OperatorLink(
          OperatorPort(headerlessCsvOpDesc1.operatorID, 0),
          OperatorPort(joinOpDesc.operatorID, 0)
        ),
        OperatorLink(
          OperatorPort(headerlessCsvOpDesc2.operatorID, 0),
          OperatorPort(joinOpDesc.operatorID, 1)
        ),
        OperatorLink(
          OperatorPort(joinOpDesc.operatorID, 0),
          OperatorPort(sink.operatorID, 0)
        )
      )
    )
    val pipelinedRegions = new WorkflowPipelinedRegions(workflow)
    assert(pipelinedRegions.pipelinedRegionsDAG.vertexSet().size == 2)

    var buildRegion: PipelinedRegion = null
    pipelinedRegions.pipelinedRegionsDAG
      .vertexSet()
      .forEach(p =>
        if (
          p.getOperators()
            .contains(
              OperatorIdentity(workflow.getWorkflowId().id, headerlessCsvOpDesc1.operatorID)
            )
        ) {
          buildRegion = p
        }
      )

    var probeRegion: PipelinedRegion = null
    pipelinedRegions.pipelinedRegionsDAG
      .vertexSet()
      .forEach(p =>
        if (
          p.getOperators()
            .contains(
              OperatorIdentity(workflow.getWorkflowId().id, headerlessCsvOpDesc2.operatorID)
            )
        ) {
          probeRegion = p
        }
      )

    assert(pipelinedRegions.pipelinedRegionsDAG.getAncestors(probeRegion).size() == 1)
    assert(pipelinedRegions.pipelinedRegionsDAG.getAncestors(probeRegion).contains(buildRegion))
    assert(buildRegion.blockingDowstreamOperatorsInOtherRegions.size == 1)
    assert(
      buildRegion.blockingDowstreamOperatorsInOtherRegions.contains(
        OperatorIdentity(workflow.getWorkflowId().id, joinOpDesc.operatorID)
      )
    )
  }

  "Pipelined Regions" should "correctly find regions in csv->->filter->join->sink workflow" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      mutable.MutableList[OperatorDescriptor](
        headerlessCsvOpDesc1,
        keywordOpDesc,
        joinOpDesc,
        sink
      ),
      mutable.MutableList[OperatorLink](
        OperatorLink(
          OperatorPort(headerlessCsvOpDesc1.operatorID, 0),
          OperatorPort(joinOpDesc.operatorID, 0)
        ),
        OperatorLink(
          OperatorPort(headerlessCsvOpDesc1.operatorID, 0),
          OperatorPort(keywordOpDesc.operatorID, 0)
        ),
        OperatorLink(
          OperatorPort(keywordOpDesc.operatorID, 0),
          OperatorPort(joinOpDesc.operatorID, 1)
        ),
        OperatorLink(
          OperatorPort(joinOpDesc.operatorID, 0),
          OperatorPort(sink.operatorID, 0)
        )
      )
    )
    assertThrows[WorkflowRuntimeException](new WorkflowPipelinedRegions(workflow))
  }

  "Pipelined Regions" should "correctly find regions in buildcsv->probecsv->hashjoin->hashjoin->sink workflow" in {
    val buildCsv = TestOperators.headerlessSmallCsvScanOpDesc()
    val probeCsv = TestOperators.smallCsvScanOpDesc()
    val hashJoin1 = TestOperators.joinOpDesc("column-1", "Region")
    val hashJoin2 = TestOperators.joinOpDesc("column-2", "Country")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      mutable.MutableList[OperatorDescriptor](
        buildCsv,
        probeCsv,
        hashJoin1,
        hashJoin2,
        sink
      ),
      mutable.MutableList[OperatorLink](
        OperatorLink(
          OperatorPort(buildCsv.operatorID, 0),
          OperatorPort(hashJoin1.operatorID, 0)
        ),
        OperatorLink(
          OperatorPort(probeCsv.operatorID, 0),
          OperatorPort(hashJoin1.operatorID, 1)
        ),
        OperatorLink(
          OperatorPort(buildCsv.operatorID, 0),
          OperatorPort(hashJoin2.operatorID, 0)
        ),
        OperatorLink(
          OperatorPort(hashJoin1.operatorID, 0),
          OperatorPort(hashJoin2.operatorID, 1)
        ),
        OperatorLink(
          OperatorPort(hashJoin2.operatorID, 0),
          OperatorPort(sink.operatorID, 0)
        )
      )
    )
    val pipelinedRegions = new WorkflowPipelinedRegions(workflow)
    assert(pipelinedRegions.pipelinedRegionsDAG.vertexSet().size == 2)
  }

}
