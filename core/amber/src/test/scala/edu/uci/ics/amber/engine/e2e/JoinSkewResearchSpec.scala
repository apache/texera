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
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage.{
  DetectSkew,
  DetectSkewTemp,
  Start
}
import edu.uci.ics.amber.engine.common.ambermessage.ControllerMessage.{
  AckedControllerInitialization,
  ReportState
}
import edu.uci.ics.amber.engine.common.ambertag.{OperatorIdentifier, WorkflowTag}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.{
  BreakpointInfo,
  OperatorLink,
  OperatorPort,
  WorkflowCompiler,
  WorkflowInfo
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt

class JoinSkewResearchSpec
    extends TestKit(ActorSystem("JoinSkewResearchSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override def beforeAll: Unit = {
    system.actorOf(Props[SingleNodeListener], "cluster-info")
  }
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  ignore should "process DetectSkew message properly" in {
    val headerlessCsvOpDesc1 = TestOperators.smallCsvScanOpDesc()
    val headerlessCsvOpDesc2 = TestOperators.smallCsvScanOpDesc()
    val joinOpDesc = TestOperators.joinOpDesc("Region", "Region")
    val sink = TestOperators.sinkOpDesc()

    println(
      s"IDs csv1 ${headerlessCsvOpDesc1.operatorID}, csv2 ${headerlessCsvOpDesc2.operatorID}, join ${joinOpDesc.operatorID}, sink ${sink.operatorID}"
    )

    val parent = TestProbe()
    val context = new WorkflowContext
    context.workflowID = "workflow-test"

    val texeraWorkflowCompiler = new WorkflowCompiler(
      WorkflowInfo(
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
        ),
        mutable.MutableList[BreakpointInfo]()
      ),
      context
    )
    texeraWorkflowCompiler.init()
    val workflow = texeraWorkflowCompiler.amberWorkflow
    val workflowTag = WorkflowTag.apply("workflow-test")

    val controller = parent.childActorOf(
      Controller.props(workflowTag, workflow, false, ControllerEventListener(), 100)
    )
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    // controller ! DetectSkewTemp(OperatorIdentifier("workflow-test", joinOpDesc.operatorID))
    parent.expectMsg(10.minute, ReportState(ControllerState.Completed))
    parent.ref ! PoisonPill
  }

}
