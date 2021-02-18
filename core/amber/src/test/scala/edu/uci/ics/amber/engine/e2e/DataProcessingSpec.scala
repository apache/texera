package edu.uci.ics.amber.engine.e2e

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.controller.{
  Controller,
  ControllerEventListener,
  ControllerState
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow._
import edu.uci.ics.texera.workflow.operators.aggregate.AggregationFunction
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class DataProcessingSpec
    extends TestKit(ActorSystem("DataProcessingSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  implicit val timeout: Timeout = Timeout(5.seconds)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override def beforeAll: Unit = {
    system.actorOf(Props[SingleNodeListener], "cluster-info")
  }
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def expectCompletedAfterExecution(
      operators: mutable.MutableList[OperatorDescriptor],
      links: mutable.MutableList[OperatorLink]
  ): Unit = {
    val parent = TestProbe()
    val context = new WorkflowContext
    context.jobID = "workflow-test"

    val texeraWorkflowCompiler = new WorkflowCompiler(
      WorkflowInfo(operators, links, mutable.MutableList[BreakpointInfo]()),
      context
    )
    texeraWorkflowCompiler.init()
    val workflow = texeraWorkflowCompiler.amberWorkflow
    val workflowTag = WorkflowIdentity("workflow-test")

    val controller = parent.childActorOf(
      Controller.props(workflowTag, workflow, ControllerEventListener(), 100)
    )
    parent.expectMsg(ControllerState.Ready)
    controller ! ControlInvocation(AsyncRPCClient.IgnoreReply, StartWorkflow())
    parent.expectMsg(ControllerState.Running)
    parent.expectMsg(1.minute, ControllerState.Completed)
    parent.ref ! PoisonPill
  }

  "Engine" should "execute headerlessCsv->sink workflow normally" in {
    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val sink = TestOperators.sinkOpDesc()

    expectCompletedAfterExecution(
      mutable.MutableList[OperatorDescriptor](headerlessCsvOpDesc, sink),
      mutable.MutableList[OperatorLink](
        OperatorLink(
          OperatorPort(headerlessCsvOpDesc.operatorID, 0),
          OperatorPort(sink.operatorID, 0)
        )
      )
    )
  }

  "Engine" should "execute headerlessCsv->keyword->sink workflow normally" in {
    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val sink = TestOperators.sinkOpDesc()
    expectCompletedAfterExecution(
      mutable.MutableList[OperatorDescriptor](headerlessCsvOpDesc, keywordOpDesc, sink),
      mutable.MutableList[OperatorLink](
        OperatorLink(
          OperatorPort(headerlessCsvOpDesc.operatorID, 0),
          OperatorPort(keywordOpDesc.operatorID, 0)
        ),
        OperatorLink(OperatorPort(keywordOpDesc.operatorID, 0), OperatorPort(sink.operatorID, 0))
      )
    )
  }

  "Engine" should "execute csv->sink workflow normally" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val sink = TestOperators.sinkOpDesc()
    expectCompletedAfterExecution(
      mutable.MutableList[OperatorDescriptor](csvOpDesc, sink),
      mutable.MutableList[OperatorLink](
        OperatorLink(OperatorPort(csvOpDesc.operatorID, 0), OperatorPort(sink.operatorID, 0))
      )
    )
  }

  "Engine" should "execute csv->keyword->sink workflow normally" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val sink = TestOperators.sinkOpDesc()
    expectCompletedAfterExecution(
      mutable.MutableList[OperatorDescriptor](csvOpDesc, keywordOpDesc, sink),
      mutable.MutableList[OperatorLink](
        OperatorLink(
          OperatorPort(csvOpDesc.operatorID, 0),
          OperatorPort(keywordOpDesc.operatorID, 0)
        ),
        OperatorLink(OperatorPort(keywordOpDesc.operatorID, 0), OperatorPort(sink.operatorID, 0))
      )
    )
  }

  "Engine" should "execute csv->keyword->count->sink workflow normally" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val countOpDesc =
      TestOperators.aggregateAndGroupByDesc("Region", AggregationFunction.COUNT, List[String]())
    val sink = TestOperators.sinkOpDesc()
    expectCompletedAfterExecution(
      mutable.MutableList[OperatorDescriptor](csvOpDesc, keywordOpDesc, countOpDesc, sink),
      mutable.MutableList[OperatorLink](
        OperatorLink(
          OperatorPort(csvOpDesc.operatorID, 0),
          OperatorPort(keywordOpDesc.operatorID, 0)
        ),
        OperatorLink(
          OperatorPort(keywordOpDesc.operatorID, 0),
          OperatorPort(countOpDesc.operatorID, 0)
        ),
        OperatorLink(OperatorPort(countOpDesc.operatorID, 0), OperatorPort(sink.operatorID, 0))
      )
    )
  }

  "Engine" should "execute csv->keyword->averageAndGroupby->sink workflow normally" in {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val averageAndGroupbyOpDesc =
      TestOperators.aggregateAndGroupByDesc(
        "Units Sold",
        AggregationFunction.AVERAGE,
        List[String]("Country")
      )
    val sink = TestOperators.sinkOpDesc()
    expectCompletedAfterExecution(
      mutable
        .MutableList[OperatorDescriptor](csvOpDesc, keywordOpDesc, averageAndGroupbyOpDesc, sink),
      mutable.MutableList[OperatorLink](
        OperatorLink(
          OperatorPort(csvOpDesc.operatorID, 0),
          OperatorPort(keywordOpDesc.operatorID, 0)
        ),
        OperatorLink(
          OperatorPort(keywordOpDesc.operatorID, 0),
          OperatorPort(averageAndGroupbyOpDesc.operatorID, 0)
        ),
        OperatorLink(
          OperatorPort(averageAndGroupbyOpDesc.operatorID, 0),
          OperatorPort(sink.operatorID, 0)
        )
      )
    )
  }

  "Engine" should "execute csv->(csv->)->join->sink workflow normally" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val headerlessCsvOpDesc2 = TestOperators.headerlessSmallCsvScanOpDesc()
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val sink = TestOperators.sinkOpDesc()
    expectCompletedAfterExecution(
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
  }

}
