package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.core.workflow.{PortIdentity, WorkflowContext}
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.amber.operator.TestOperators
import edu.uci.ics.amber.operator.aggregate.{AggregateOpDesc, AggregationFunction}
import edu.uci.ics.amber.operator.keywordSearch.KeywordSearchOpDesc
import edu.uci.ics.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRole
import edu.uci.ics.texera.dao.jooq.generated.tables.daos._
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos._
import edu.uci.ics.texera.workflow.LogicalLink
import org.jooq.types.{UInteger, ULong}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.jooq.DSLContext
import edu.uci.ics.texera.dao.jooq.generated.tables.OperatorExecutions.OPERATOR_EXECUTIONS

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
    user.setUid(UInteger.valueOf(1))
    user.setName("test_user")
    user.setRole(UserRole.ADMIN)
    user.setPassword("123")
    user.setEmail("test_user@test.com")
    user
  }

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

  private val headerlessCsvOperatorExecutionEntry: OperatorExecutions = {
    val operatorExecution = new OperatorExecutions
    operatorExecution.setWorkflowExecutionId(UInteger.valueOf(1))
    operatorExecution.setOperatorId(headerlessCsvOpDesc.operatorIdentifier.id)
    operatorExecution
  }

  private val keywordOperatorExecutionEntry: OperatorExecutions = {
    val operatorExecution = new OperatorExecutions
    operatorExecution.setWorkflowExecutionId(UInteger.valueOf(1))
    operatorExecution.setOperatorId(keywordOpDesc.operatorIdentifier.id)
    operatorExecution
  }

  private val groupByOperatorExecutionEntry: OperatorExecutions = {
    val operatorExecution = new OperatorExecutions
    operatorExecution.setWorkflowExecutionId(UInteger.valueOf(1))
    operatorExecution.setOperatorId(groupByOpDesc.operatorIdentifier.id)
    operatorExecution
  }

  private def headerlessCsvOpRuntimeStatisticsEntry(
      operatorExecutionId: ULong
  ): OperatorRuntimeStatistics = {
    val operatorRuntimeStatistics = new OperatorRuntimeStatistics
    operatorRuntimeStatistics.setOperatorExecutionId(operatorExecutionId)
    operatorRuntimeStatistics.setDataProcessingTime(ULong.valueOf(100))
    operatorRuntimeStatistics.setControlProcessingTime(ULong.valueOf(100))
    operatorRuntimeStatistics
  }

  private def keywordOpDescRuntimeStatisticsEntry(
      operatorExecutionId: ULong
  ): OperatorRuntimeStatistics = {
    val operatorRuntimeStatistics = new OperatorRuntimeStatistics
    operatorRuntimeStatistics.setOperatorExecutionId(operatorExecutionId)
    operatorRuntimeStatistics.setDataProcessingTime(ULong.valueOf(300))
    operatorRuntimeStatistics.setControlProcessingTime(ULong.valueOf(300))
    operatorRuntimeStatistics
  }

  private def groupByOpDescRuntimeStatisticsEntry(
      operatorExecutionId: ULong
  ): OperatorRuntimeStatistics = {
    val operatorRuntimeStatistics = new OperatorRuntimeStatistics
    operatorRuntimeStatistics.setOperatorExecutionId(operatorExecutionId)
    operatorRuntimeStatistics.setDataProcessingTime(ULong.valueOf(1000))
    operatorRuntimeStatistics.setControlProcessingTime(ULong.valueOf(1000))
    operatorRuntimeStatistics
  }

  private def insertOperatorExecutionAndGetId(
      dslContext: DSLContext,
      operatorExecution: OperatorExecutions
  ): ULong = {
    val record = dslContext
      .insertInto(OPERATOR_EXECUTIONS)
      .set(OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID, operatorExecution.getWorkflowExecutionId)
      .set(OPERATOR_EXECUTIONS.OPERATOR_ID, operatorExecution.getOperatorId)
      .returning(OPERATOR_EXECUTIONS.OPERATOR_EXECUTION_ID)
      .fetchOne()

    record.getOperatorExecutionId
  }

  override protected def beforeEach(): Unit = {
    initializeDBAndReplaceDSLContext()
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
    val operatorRuntimeStatisticsDao =
      new OperatorRuntimeStatisticsDao(getDSLContext.configuration())

    userDao.insert(testUser)
    workflowDao.insert(testWorkflowEntry)
    workflowVersionDao.insert(testWorkflowVersionEntry)
    workflowExecutionsDao.insert(testWorkflowExecutionEntry)

    val headerlessCsvOperatorExecutionId = insertOperatorExecutionAndGetId(
      getDSLContext,
      headerlessCsvOperatorExecutionEntry
    )
    val keywordOperatorExecutionId = insertOperatorExecutionAndGetId(
      getDSLContext,
      keywordOperatorExecutionEntry
    )

    val headerlessCsvOpRuntimeStatistics =
      headerlessCsvOpRuntimeStatisticsEntry(headerlessCsvOperatorExecutionId)
    val keywordOpRuntimeStatistics =
      keywordOpDescRuntimeStatisticsEntry(keywordOperatorExecutionId)

    operatorRuntimeStatisticsDao.insert(headerlessCsvOpRuntimeStatistics)
    operatorRuntimeStatisticsDao.insert(keywordOpRuntimeStatistics)

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
    val operatorRuntimeStatisticsDao =
      new OperatorRuntimeStatisticsDao(getDSLContext.configuration())

    userDao.insert(testUser)
    workflowDao.insert(testWorkflowEntry)
    workflowVersionDao.insert(testWorkflowVersionEntry)
    workflowExecutionsDao.insert(testWorkflowExecutionEntry)

    val headerlessCsvOperatorExecutionId = insertOperatorExecutionAndGetId(
      getDSLContext,
      headerlessCsvOperatorExecutionEntry
    )
    val groupByOperatorExecutionId = insertOperatorExecutionAndGetId(
      getDSLContext,
      groupByOperatorExecutionEntry
    )
    val keywordOperatorExecutionId = insertOperatorExecutionAndGetId(
      getDSLContext,
      keywordOperatorExecutionEntry
    )

    val headerlessCsvOpRuntimeStatistics =
      headerlessCsvOpRuntimeStatisticsEntry(headerlessCsvOperatorExecutionId)
    val groupByOpRuntimeStatistics =
      groupByOpDescRuntimeStatisticsEntry(groupByOperatorExecutionId)
    val keywordOpRuntimeStatistics =
      keywordOpDescRuntimeStatisticsEntry(keywordOperatorExecutionId)

    operatorRuntimeStatisticsDao.insert(headerlessCsvOpRuntimeStatistics)
    operatorRuntimeStatisticsDao.insert(groupByOpRuntimeStatistics)
    operatorRuntimeStatisticsDao.insert(keywordOpRuntimeStatistics)

    // Should contain two regions, one with CSV->localAgg->globalAgg, another with keyword->sink
    val searchResult = new CostBasedScheduleGenerator(
      workflow.context,
      workflow.physicalPlan,
      CONTROLLER
    ).bottomUpSearch()

    val groupByRegion =
      searchResult.regionDAG.vertexSet().asScala.filter(region => region.physicalOps.size == 3).head
    val keywordRegion =
      searchResult.regionDAG.vertexSet().asScala.filter(region => region.physicalOps.size == 2).head

    val costEstimator = new DefaultCostEstimator(
      workflow.context,
      CONTROLLER
    )

    val groupByRegionCost = costEstimator.estimate(groupByRegion, 1)

    val groupByOperatorCost = (groupByOpRuntimeStatistics.getControlProcessingTime
      .doubleValue() + groupByOpRuntimeStatistics.getControlProcessingTime
      .doubleValue()) / 1e9

    // The cost of the first region should be the cost of the GroupBy operator (note the two physical operators for
    // the GroupBy logical operator have the same cost because we use logical operator in the statistics.
    // The GroupBy operator has a longer running time.
    assert(groupByRegionCost == groupByOperatorCost)

    val keywordRegionCost = costEstimator.estimate(keywordRegion, 1)

    val keywordOperatorCost = (keywordOpRuntimeStatistics.getControlProcessingTime
      .doubleValue() + keywordOpRuntimeStatistics.getControlProcessingTime
      .doubleValue()) / 1e9

    // The cost of the second region should be the cost of the keyword operator, since the sink operator has the same
    // logical operator as the keyword operator.
    assert(keywordRegionCost == keywordOperatorCost)

    // The cost of the region plan should be the sum of region costs
    assert(searchResult.cost == groupByRegionCost + keywordRegionCost)
  }

  override protected def afterEach(): Unit = {
    shutdownDB()
  }

}
