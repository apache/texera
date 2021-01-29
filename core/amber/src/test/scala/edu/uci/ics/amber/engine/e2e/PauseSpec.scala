package edu.uci.ics.amber.engine.e2e

import edu.uci.ics.amber.clustering.SingleNodeListener
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.controller.ControllerState
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.{
  BreakpointInfo,
  OperatorLink,
  OperatorPort,
  WorkflowCompiler,
  WorkflowInfo
}
import org.scalatest.BeforeAndAfterAll

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._
import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.scalatest.flatspec.AnyFlatSpecLike

class PauseSpec
    extends TestKit(ActorSystem("PauseSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val logger = Logger("PauseSpecLogger")

  override def beforeAll: Unit = {
    system.actorOf(Props[SingleNodeListener], "cluster-info")
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def shouldPause(
      operators: mutable.MutableList[OperatorDescriptor],
      links: mutable.MutableList[OperatorLink]
  ): Unit = {
    val parent = TestProbe()
    val controller = parent.childActorOf(
      Utils.getControllerProps(operators, links)
    )
    parent.expectMsg(ControllerState.Ready)
    controller ! ControlInvocation(-1, StartWorkflow())
    parent.expectMsg(ControllerState.Running)
    controller ! ControlInvocation(-1, PauseWorkflow())
    parent.expectMsg(ControllerState.Paused)
    controller ! ControlInvocation(-1, ResumeWorkflow())
    parent.expectMsg(ControllerState.Running)
    Thread.sleep(400)
    controller ! ControlInvocation(-1, PauseWorkflow())
    parent.expectMsg(ControllerState.Paused)
    controller ! ControlInvocation(-1, ResumeWorkflow())
    parent.expectMsg(ControllerState.Running)
    parent.expectMsg(1.minute, ControllerState.Completed)
    parent.ref ! PoisonPill
  }

  "Engine" should "be able to pause csv->sink workflow" in {
    val csvOpDesc = TestOperators.mediumCsvScanOpDesc()
    val sink = TestOperators.sinkOpDesc()
    logger.info(s"csv-id ${csvOpDesc.operatorID}, sink-id ${sink.operatorID}")
    shouldPause(
      mutable.MutableList[OperatorDescriptor](csvOpDesc, sink),
      mutable.MutableList[OperatorLink](
        OperatorLink(OperatorPort(csvOpDesc.operatorID, 0), OperatorPort(sink.operatorID, 0))
      )
    )
  }

  "Engine" should "be able to pause csv->keyword->sink workflow" in {
    val csvOpDesc = TestOperators.mediumCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val sink = TestOperators.sinkOpDesc()
    logger.info(
      s"csv-id ${csvOpDesc.operatorID}, keyword-id ${keywordOpDesc.operatorID}, sink-id ${sink.operatorID}"
    )
    shouldPause(
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
}
