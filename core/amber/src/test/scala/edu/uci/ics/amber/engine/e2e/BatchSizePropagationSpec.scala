package edu.uci.ics.amber.engine.e2e

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.controller._
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.{
  BroadcastPartitioning,
  HashBasedShufflePartitioning,
  OneToOnePartitioning,
  RangeBasedShufflePartitioning,
  RoundRobinPartitioning
}
import edu.uci.ics.amber.engine.common.model.WorkflowContext
import edu.uci.ics.amber.engine.common.model.WorkflowSettings
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import edu.uci.ics.texera.workflow.common.workflow.LogicalLink

import scala.concurrent.duration.DurationInt
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.texera.workflow.operators.aggregate.AggregationFunction

class BatchSizePropagationSpec
    extends TestKit(ActorSystem("BatchSizePropagationSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  implicit val timeout: Timeout = Timeout(5.seconds)

  val resultStorage = new OpResultStorage()

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    resultStorage.clear()
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

  "Engine" should "propagate the correct batch size for headerlessCsv->sink workflow" in {
    val expectedBatchSize = 1

    val customWorkflowSettings = WorkflowSettings(dataTransferBatchSize = expectedBatchSize)

    val context = new WorkflowContext(workflowSettings = customWorkflowSettings)

    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val sink = TestOperators.sinkOpDesc()

    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, sink),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage,
      context
    )

    val workflowScheduler = new WorkflowScheduler(context, resultStorage)
    workflowScheduler.updateSchedule(workflow.physicalPlan)

    verifyBatchSizeInPartitioning(workflowScheduler, 1)
  }

  "Engine" should "propagate the correct batch size for headerlessCsv->keyword->sink workflow" in {
    val expectedBatchSize = 500

    val customWorkflowSettings = WorkflowSettings(dataTransferBatchSize = expectedBatchSize)

    val context = new WorkflowContext(workflowSettings = customWorkflowSettings)

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
      ),
      resultStorage,
      context
    )

    val workflowScheduler = new WorkflowScheduler(context, resultStorage)
    workflowScheduler.updateSchedule(workflow.physicalPlan)

    verifyBatchSizeInPartitioning(workflowScheduler, 500)
  }

  "Engine" should "propagate the correct batch size for csv->keyword->count->sink workflow" in {
    val expectedBatchSize = 100

    val customWorkflowSettings = WorkflowSettings(dataTransferBatchSize = expectedBatchSize)

    val context = new WorkflowContext(workflowSettings = customWorkflowSettings)

    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val countOpDesc =
      TestOperators.aggregateAndGroupByDesc("Region", AggregationFunction.COUNT, List[String]())
    val sink = TestOperators.sinkOpDesc()

    val workflow = buildWorkflow(
      List(csvOpDesc, keywordOpDesc, countOpDesc, sink),
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
        ),
        LogicalLink(
          countOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage,
      context
    )

    val workflowScheduler = new WorkflowScheduler(context, resultStorage)
    workflowScheduler.updateSchedule(workflow.physicalPlan)

    verifyBatchSizeInPartitioning(workflowScheduler, 100)
  }

  "Engine" should "propagate the correct batch size for csv->keyword->averageAndGroupBy->sink workflow" in {
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
    val sink = TestOperators.sinkOpDesc()

    val workflow = buildWorkflow(
      List(csvOpDesc, keywordOpDesc, averageAndGroupByOpDesc, sink),
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
        ),
        LogicalLink(
          averageAndGroupByOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage,
      context
    )

    val workflowScheduler = new WorkflowScheduler(context, resultStorage)
    workflowScheduler.updateSchedule(workflow.physicalPlan)

    verifyBatchSizeInPartitioning(workflowScheduler, 300)
  }

  "Engine" should "propagate the correct batch size for csv->(csv->)->join->sink workflow" in {
    val expectedBatchSize = 1

    val customWorkflowSettings = WorkflowSettings(dataTransferBatchSize = expectedBatchSize)

    val context = new WorkflowContext(workflowSettings = customWorkflowSettings)

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
      ),
      resultStorage,
      context
    )

    val workflowScheduler = new WorkflowScheduler(context, resultStorage)
    workflowScheduler.updateSchedule(workflow.physicalPlan)

    verifyBatchSizeInPartitioning(workflowScheduler, 1)
  }

}
