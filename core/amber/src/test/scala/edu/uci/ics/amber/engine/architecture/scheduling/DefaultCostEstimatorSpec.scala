package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.amber.operator.TestOperators
import edu.uci.ics.amber.operator.keywordSearch.KeywordSearchOpDesc
import edu.uci.ics.amber.operator.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import edu.uci.ics.amber.workflow.PortIdentity
import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowRuntimeStatisticsDao,
  WorkflowVersionDao
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{
  Workflow,
  WorkflowExecutions,
  WorkflowRuntimeStatistics,
  WorkflowVersion
}
import edu.uci.ics.texera.workflow.LogicalLink
import org.jooq.types.{UInteger, ULong}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec

class DefaultCostEstimatorSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val headerlessCsvOpDesc: CSVScanSourceOpDesc =
    TestOperators.headerlessSmallCsvScanOpDesc()
  private val keywordOpDesc: KeywordSearchOpDesc =
    TestOperators.keywordSearchOpDesc("column-1", "Asia")
  private val sink: ProgressiveSinkOpDesc = TestOperators.sinkOpDesc()

  private val testWorkflowEntry: Workflow = {
    val workflow = new Workflow
    workflow.setName("test workflow")
    workflow.setWid(UInteger.valueOf(1))
    workflow.setContent("test workflow content")
    workflow.setDescription("test description")
    workflow
  }

  private val testWorkflowVersionEntry: WorkflowVersion = {
    val workflowVersion = new WorkflowVersion
    workflowVersion.setWid(UInteger.valueOf(1))
    workflowVersion.setVid(UInteger.valueOf(1))
    workflowVersion.setContent("test version content")
    workflowVersion
  }

  private val testWorkflowExecutionEntry: WorkflowExecutions = {
    val workflowExecution = new WorkflowExecutions
    workflowExecution.setEid(UInteger.valueOf(1))
    workflowExecution.setVid(UInteger.valueOf(1))
    workflowExecution.setUid(UInteger.valueOf(1))
    workflowExecution.setStatus(3.toByte)
    workflowExecution.setEnvironmentVersion("test engine")
    workflowExecution
  }

  private val headerlessCsvOpStatisticsEntry: WorkflowRuntimeStatistics = {
    val workflowRuntimeStatistics = new WorkflowRuntimeStatistics
    workflowRuntimeStatistics.setOperatorId(headerlessCsvOpDesc.operatorIdentifier.id)
    workflowRuntimeStatistics.setWorkflowId(UInteger.valueOf(1))
    workflowRuntimeStatistics.setExecutionId(UInteger.valueOf(1))
    workflowRuntimeStatistics.setDataProcessingTime(ULong.valueOf(100))
    workflowRuntimeStatistics.setControlProcessingTime(ULong.valueOf(100))
    workflowRuntimeStatistics
  }

  private val keywordOpDescStatisticsEntry: WorkflowRuntimeStatistics = {
    val workflowRuntimeStatistics = new WorkflowRuntimeStatistics
    workflowRuntimeStatistics.setOperatorId(keywordOpDesc.operatorIdentifier.id)
    workflowRuntimeStatistics.setWorkflowId(UInteger.valueOf(1))
    workflowRuntimeStatistics.setExecutionId(UInteger.valueOf(1))
    workflowRuntimeStatistics.setDataProcessingTime(ULong.valueOf(300))
    workflowRuntimeStatistics.setControlProcessingTime(ULong.valueOf(300))
    workflowRuntimeStatistics
  }

  private val sinkOpDescStatisticsEntry: WorkflowRuntimeStatistics = {
    val workflowRuntimeStatistics = new WorkflowRuntimeStatistics
    workflowRuntimeStatistics.setOperatorId(sink.operatorIdentifier.id)
    workflowRuntimeStatistics.setWorkflowId(UInteger.valueOf(1))
    workflowRuntimeStatistics.setExecutionId(UInteger.valueOf(1))
    workflowRuntimeStatistics.setDataProcessingTime(ULong.valueOf(200))
    workflowRuntimeStatistics.setControlProcessingTime(ULong.valueOf(200))
    workflowRuntimeStatistics
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  "DefaultCostEstimator" should "use fallback method when no past statistics are available" in {
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

  "DefaultCostEstimator" should "use the latest successful execution to estimate cost when available" in {
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

    val workflowDao = new WorkflowDao(getDSLContext.configuration())
    val workflowExecutionsDao = new WorkflowExecutionsDao(getDSLContext.configuration())
    val workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())
    val workflowRuntimeStatisticsDao =
      new WorkflowRuntimeStatisticsDao(getDSLContext.configuration())

    workflowDao.insert(testWorkflowEntry)
    workflowVersionDao.insert(testWorkflowVersionEntry)
    workflowExecutionsDao.insert(testWorkflowExecutionEntry)
    workflowRuntimeStatisticsDao.insert(headerlessCsvOpStatisticsEntry)
    workflowRuntimeStatisticsDao.insert(keywordOpDescStatisticsEntry)
    workflowRuntimeStatisticsDao.insert(sinkOpDescStatisticsEntry)

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

    assert(costOfRegion != 0)
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

}
