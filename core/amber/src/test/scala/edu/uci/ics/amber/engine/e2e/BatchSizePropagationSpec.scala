package edu.uci.ics.amber.engine.e2e

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.core.workflow.{WorkflowContext, WorkflowSettings}
import edu.uci.ics.amber.engine.architecture.controller._
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings._
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.amber.operator.TestOperators
import edu.uci.ics.amber.operator.aggregate.AggregationFunction
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.texera.workflow.LogicalLink
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration.DurationInt

class BatchSizePropagationSpec
    extends TestKit(ActorSystem("BatchSizePropagationSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  implicit val timeout: Timeout = Timeout(5.seconds)

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def verifyBatchSizeInPartitioning(
      workflowScheduler: WorkflowScheduler,
      expectedBatchSize: Int
  ): Unit = {
    var nextRegions = workflowScheduler.getNextRegions
    while (nextRegions.nonEmpty) {
      nextRegions.foreach { region =>
        region.resourceConfig.foreach { resourceConfig =>
          resourceConfig.linkConfigs.foreach {
            case (_, linkConfig) =>
              val partitioning = linkConfig.partitioning
              partitioning match {
                case oneToOne: OneToOnePartitioning =>
                  println(s"Testing OneToOnePartitioning with batch size: ${oneToOne.batchSize}")
                  assert(
                    oneToOne.batchSize == expectedBatchSize,
                    s"Batch size mismatch: ${oneToOne.batchSize} != $expectedBatchSize"
                  )

                case roundRobin: RoundRobinPartitioning =>
                  println(
                    s"Testing RoundRobinPartitioning with batch size: ${roundRobin.batchSize}"
                  )
                  assert(
                    roundRobin.batchSize == expectedBatchSize,
                    s"Batch size mismatch: ${roundRobin.batchSize} != $expectedBatchSize"
                  )

                case hashBased: HashBasedShufflePartitioning =>
                  println(
                    s"Testing HashBasedShufflePartitioning with batch size: ${hashBased.batchSize}"
                  )
                  assert(
                    hashBased.batchSize == expectedBatchSize,
                    s"Batch size mismatch: ${hashBased.batchSize} != $expectedBatchSize"
                  )

                case rangeBased: RangeBasedShufflePartitioning =>
                  println(
                    s"Testing RangeBasedShufflePartitioning with batch size: ${rangeBased.batchSize}"
                  )
                  assert(
                    rangeBased.batchSize == expectedBatchSize,
                    s"Batch size mismatch: ${rangeBased.batchSize} != $expectedBatchSize"
                  )

                case broadcast: BroadcastPartitioning =>
                  println(s"Testing BroadcastPartitioning with batch size: ${broadcast.batchSize}")
                  assert(
                    broadcast.batchSize == expectedBatchSize,
                    s"Batch size mismatch: ${broadcast.batchSize} != $expectedBatchSize"
                  )

                case _ =>
                  throw new IllegalArgumentException("Unknown partitioning type encountered")
              }
          }
        }
      }
      nextRegions = workflowScheduler.getNextRegions
    }
  }

  "Engine" should "propagate the correct batch size for headerlessCsv workflow" in {
    val expectedBatchSize = 1

    val customWorkflowSettings = WorkflowSettings(dataTransferBatchSize = expectedBatchSize)

    val context = new WorkflowContext(workflowSettings = customWorkflowSettings)

    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()

    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc),
      List(),
      context
    )

    val workflowScheduler = new WorkflowScheduler(context, CONTROLLER)
    workflowScheduler.updateSchedule(workflow.physicalPlan)

    verifyBatchSizeInPartitioning(workflowScheduler, 1)
  }

  "Engine" should "propagate the correct batch size for headerlessCsv->keyword workflow" in {
    val expectedBatchSize = 500

    val customWorkflowSettings = WorkflowSettings(dataTransferBatchSize = expectedBatchSize)

    val context = new WorkflowContext(workflowSettings = customWorkflowSettings)

    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")

    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      context
    )

    val workflowScheduler = new WorkflowScheduler(context, CONTROLLER)
    workflowScheduler.updateSchedule(workflow.physicalPlan)

    verifyBatchSizeInPartitioning(workflowScheduler, 500)
  }

  "Engine" should "propagate the correct batch size for csv->keyword->count workflow" in {
    val expectedBatchSize = 100

    val customWorkflowSettings = WorkflowSettings(dataTransferBatchSize = expectedBatchSize)

    val context = new WorkflowContext(workflowSettings = customWorkflowSettings)

    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val countOpDesc =
      TestOperators.aggregateAndGroupByDesc("Region", AggregationFunction.COUNT, List[String]())

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
      context
    )

    val workflowScheduler = new WorkflowScheduler(context, CONTROLLER)
    workflowScheduler.updateSchedule(workflow.physicalPlan)

    verifyBatchSizeInPartitioning(workflowScheduler, 100)
  }

  "Engine" should "propagate the correct batch size for csv->keyword->averageAndGroupBy workflow" in {
    val expectedBatchSize = 300

    val customWorkflowSettings = WorkflowSettings(dataTransferBatchSize = expectedBatchSize)

    val context = new WorkflowContext(workflowSettings = customWorkflowSettings)

    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val averageAndGroupByOpDesc =
      TestOperators.aggregateAndGroupByDesc(
        "Units Sold",
        AggregationFunction.AVERAGE,
        List[String]("Country")
      )
    val workflow = buildWorkflow(
      List(csvOpDesc, keywordOpDesc, averageAndGroupByOpDesc),
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
          averageAndGroupByOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      context
    )

    val workflowScheduler = new WorkflowScheduler(context, CONTROLLER)
    workflowScheduler.updateSchedule(workflow.physicalPlan)

    verifyBatchSizeInPartitioning(workflowScheduler, 300)
  }

  "Engine" should "propagate the correct batch size for csv->(csv->)->join workflow" in {
    val expectedBatchSize = 1

    val customWorkflowSettings = WorkflowSettings(dataTransferBatchSize = expectedBatchSize)

    val context = new WorkflowContext(workflowSettings = customWorkflowSettings)

    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val headerlessCsvOpDesc2 = TestOperators.headerlessSmallCsvScanOpDesc()
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")

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
      context
    )

    val workflowScheduler = new WorkflowScheduler(context, CONTROLLER)
    workflowScheduler.updateSchedule(workflow.physicalPlan)

    verifyBatchSizeInPartitioning(workflowScheduler, 1)
  }

}
