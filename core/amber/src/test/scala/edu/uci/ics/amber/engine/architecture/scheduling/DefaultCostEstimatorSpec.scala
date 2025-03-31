package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}
import edu.uci.ics.amber.core.storage.result.ResultSchema
import edu.uci.ics.amber.core.storage.{DocumentFactory, VFSURIFactory}
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{GlobalPortIdentity, PortIdentity, WorkflowContext}
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.amber.operator.TestOperators
import edu.uci.ics.amber.operator.aggregate.{AggregateOpDesc, AggregationFunction}
import edu.uci.ics.amber.operator.keywordSearch.KeywordSearchOpDesc
import edu.uci.ics.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos._
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos._
import edu.uci.ics.texera.workflow.LogicalLink
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.net.URI
import java.sql.Timestamp
import scala.jdk.CollectionConverters.CollectionHasAsScala

class DefaultCostEstimatorSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val headerlessCsvOpDesc: CSVScanSourceOpDesc =
    TestOperators.headerlessSmallCsvScanOpDesc()
  private val keywordOpDesc: KeywordSearchOpDesc =
    TestOperators.keywordSearchOpDesc("column-1", "Asia")
  private val groupByOpDesc: AggregateOpDesc =
    TestOperators.aggregateAndGroupByDesc("column-1", AggregationFunction.COUNT, List[String]())

  private val testUser: User = {
    val user = new User
    user.setUid(Integer.valueOf(1))
    user.setName("test_user")
    user.setRole(UserRoleEnum.ADMIN)
    user.setPassword("123")
    user.setEmail("test_user@test.com")
    user
  }

  private val testWorkflowEntry: Workflow = {
    val workflow = new Workflow
    workflow.setName("test workflow")
    workflow.setWid(Integer.valueOf(1))
    workflow.setContent("test workflow content")
    workflow.setDescription("test description")
    workflow
  }

  private val testWorkflowVersionEntry: WorkflowVersion = {
    val workflowVersion = new WorkflowVersion
    workflowVersion.setWid(Integer.valueOf(1))
    workflowVersion.setVid(Integer.valueOf(1))
    workflowVersion.setContent("test version content")
    workflowVersion
  }

  private val testWorkflowExecutionEntry: WorkflowExecutions = {
    val workflowExecution = new WorkflowExecutions
    workflowExecution.setEid(Integer.valueOf(1))
    workflowExecution.setVid(Integer.valueOf(1))
    workflowExecution.setUid(Integer.valueOf(1))
    workflowExecution.setStatus(3.toByte)
    workflowExecution.setEnvironmentVersion("test engine")
    workflowExecution
  }

  private var uri: URI = _
  private var writer: BufferedItemWriter[Tuple] = _
  private var document: VirtualDocument[_] = _

  override protected def beforeEach(): Unit = {
    initializeDBAndReplaceDSLContext()
    uri = VFSURIFactory.createRuntimeStatisticsURI(
      WorkflowIdentity(testWorkflowEntry.getWid.longValue()),
      ExecutionIdentity(testWorkflowExecutionEntry.getEid.longValue())
    )
    document = DocumentFactory.createDocument(uri, ResultSchema.runtimeStatisticsSchema)
    writer = document
      .writer(s"runtime_statistics_${testWorkflowExecutionEntry.getEid.longValue()}")
      .asInstanceOf[BufferedItemWriter[Tuple]]
    writer.open()
  }

  override protected def afterEach(): Unit = {
    document.clear()
    shutdownDB()
  }

  "DefaultCostEstimator" should "use fallback method when no past statistics are available" in {
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(0),
          keywordOpDesc.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    )

    val costEstimator = new DefaultCostEstimator(
      workflow.context,
      CONTROLLER
    )
    val ports = workflow.physicalPlan.operators.flatMap(op =>
      op.inputPorts.keys
        .map(inputPortId => GlobalPortIdentity(op.id, inputPortId, input = true))
        .toSet ++ op.outputPorts.keys
        .map(outputPortId => GlobalPortIdentity(op.id, outputPortId))
        .toSet
    )

    val region = Region(
      id = RegionIdentity(0),
      physicalOps = workflow.physicalPlan.operators,
      physicalLinks = workflow.physicalPlan.links,
      ports = ports
    )

    val costOfRegion = costEstimator.estimate(region, 1)

    assert(costOfRegion == 0)
  }

  "DefaultCostEstimator" should "use the latest successful execution to estimate cost when available" in {
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(0),
          keywordOpDesc.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    )

    val userDao = new UserDao(getDSLContext.configuration())
    val workflowDao = new WorkflowDao(getDSLContext.configuration())
    val workflowExecutionsDao = new WorkflowExecutionsDao(getDSLContext.configuration())
    val workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())

    userDao.insert(testUser)
    workflowDao.insert(testWorkflowEntry)
    workflowVersionDao.insert(testWorkflowVersionEntry)
    testWorkflowExecutionEntry.setRuntimeStatsUri(uri.toString)
    workflowExecutionsDao.insert(testWorkflowExecutionEntry)

    val headerlessCsvOpRuntimeStatistics = new Tuple(
      ResultSchema.runtimeStatisticsSchema,
      Array(
        headerlessCsvOpDesc.operatorIdentifier.id,
        new Timestamp(System.currentTimeMillis()),
        0L,
        0L,
        0L,
        0L,
        100L,
        100L,
        0L,
        1,
        0
      )
    )
    val keywordOpRuntimeStatistics = new Tuple(
      ResultSchema.runtimeStatisticsSchema,
      Array(
        keywordOpDesc.operatorIdentifier.id,
        new Timestamp(System.currentTimeMillis()),
        0L,
        0L,
        0L,
        0L,
        300L,
        300L,
        0L,
        1,
        0
      )
    )

    writer.putOne(headerlessCsvOpRuntimeStatistics)
    writer.putOne(keywordOpRuntimeStatistics)
    writer.close()

    val costEstimator = new DefaultCostEstimator(
      workflow.context,
      CONTROLLER
    )

    val ports = workflow.physicalPlan.operators.flatMap(op =>
      op.inputPorts.keys
        .map(inputPortId => GlobalPortIdentity(op.id, inputPortId, input = true))
        .toSet ++ op.outputPorts.keys
        .map(outputPortId => GlobalPortIdentity(op.id, outputPortId))
        .toSet
    )

    val region = Region(
      id = RegionIdentity(0),
      physicalOps = workflow.physicalPlan.operators,
      physicalLinks = workflow.physicalPlan.links,
      ports = ports
    )

    val costOfRegion = costEstimator.estimate(region, 1)

    assert(costOfRegion != 0)
  }

  "DefaultCostEstimator" should "use correctly estimate costs in a search" in {
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, groupByOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(0),
          groupByOpDesc.operatorIdentifier,
          PortIdentity(0)
        ),
        LogicalLink(
          groupByOpDesc.operatorIdentifier,
          PortIdentity(0),
          keywordOpDesc.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    )

    val userDao = new UserDao(getDSLContext.configuration())
    val workflowDao = new WorkflowDao(getDSLContext.configuration())
    val workflowExecutionsDao = new WorkflowExecutionsDao(getDSLContext.configuration())
    val workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())

    userDao.insert(testUser)
    workflowDao.insert(testWorkflowEntry)
    workflowVersionDao.insert(testWorkflowVersionEntry)
    testWorkflowExecutionEntry.setRuntimeStatsUri(uri.toString)
    workflowExecutionsDao.insert(testWorkflowExecutionEntry)

    val headerlessCsvOpRuntimeStatistics = new Tuple(
      ResultSchema.runtimeStatisticsSchema,
      Array(
        headerlessCsvOpDesc.operatorIdentifier.id,
        new Timestamp(System.currentTimeMillis()),
        0L,
        0L,
        0L,
        0L,
        100L,
        100L,
        0L,
        1,
        0
      )
    )
    val groupByOpRuntimeStatistics = new Tuple(
      ResultSchema.runtimeStatisticsSchema,
      Array(
        groupByOpDesc.operatorIdentifier.id,
        new Timestamp(System.currentTimeMillis()),
        0L,
        0L,
        0L,
        0L,
        1000L,
        1000L,
        0L,
        1,
        0
      )
    )
    val keywordOpRuntimeStatistics = new Tuple(
      ResultSchema.runtimeStatisticsSchema,
      Array(
        keywordOpDesc.operatorIdentifier.id,
        new Timestamp(System.currentTimeMillis()),
        0L,
        0L,
        0L,
        0L,
        300L,
        300L,
        0L,
        1,
        0
      )
    )

    writer.putOne(headerlessCsvOpRuntimeStatistics)
    writer.putOne(groupByOpRuntimeStatistics)
    writer.putOne(keywordOpRuntimeStatistics)
    writer.close()

    // Should contain two regions, one with CSV->localAgg->globalAgg, another with keyword
    val searchResult = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      CONTROLLER
    ).bottomUpSearch()

    val groupByRegion =
      searchResult.regionDAG.vertexSet().asScala.filter(region => region.physicalOps.size == 3).head
    val keywordRegion =
      searchResult.regionDAG.vertexSet().asScala.filter(region => region.physicalOps.size == 1).head

    val costEstimator = new DefaultCostEstimator(
      workflow.context,
      CONTROLLER
    )

    val groupByRegionCost = costEstimator.estimate(groupByRegion, 1)

    val groupByOperatorCost = (groupByOpRuntimeStatistics.getField(6).asInstanceOf[Long] +
      groupByOpRuntimeStatistics.getField(7).asInstanceOf[Long]) / 1e9

    // The cost of the first region should be the cost of the GroupBy operator (note the two physical operators for
    // the GroupBy logical operator have the same cost because we use logical operator in the statistics.
    // The GroupBy operator has a longer running time.
    assert(groupByRegionCost == groupByOperatorCost)

    val keywordRegionCost = costEstimator.estimate(keywordRegion, 1)

    val keywordOperatorCost = (keywordOpRuntimeStatistics.getField(6).asInstanceOf[Long] +
      keywordOpRuntimeStatistics.getField(7).asInstanceOf[Long]) / 1e9

    // The cost of the second region should be the cost of the keyword operator.
    assert(keywordRegionCost == keywordOperatorCost)

    // The cost of the region plan should be the sum of region costs
    assert(searchResult.cost == groupByRegionCost + keywordRegionCost)
  }
}
