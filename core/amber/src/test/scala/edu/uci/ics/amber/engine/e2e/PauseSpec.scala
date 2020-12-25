package edu.uci.ics.amber.engine.e2e

import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage.{Pause, Resume, Start}
import edu.uci.ics.amber.engine.common.ambermessage.ControllerMessage.{
  AckedControllerInitialization,
  PassBreakpointTo,
  ReportState
}
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.controller.ControllerState
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.{
  BreakpointInfo,
  OperatorLink,
  WorkflowCompiler,
  WorkflowInfo
}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._
import com.typesafe.scalalogging.Logger

class PauseSpec
    extends TestKit(ActorSystem("PauseSpec"))
    with ImplicitSender
    with FlatSpecLike
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
      testName: String,
      operators: mutable.MutableList[OperatorDescriptor],
      links: mutable.MutableList[OperatorLink]
  ): Unit = {
    val parent = TestProbe(testName)
    val controller = parent.childActorOf(
      Utils.getControllerProps(operators, links)
    )
    controller ! AckedControllerInitialization
    parent.expectMsg(ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    controller ! Pause
    parent.expectMsg(ReportState(ControllerState.Pausing))
    parent.expectMsg(ReportState(ControllerState.Paused))
    controller ! Resume
    parent.expectMsg(ReportState(ControllerState.Resuming))
    parent.expectMsg(ReportState(ControllerState.Running))
    Thread.sleep(400)
    controller ! Pause
    parent.expectMsg(ReportState(ControllerState.Pausing))
    parent.expectMsg(ReportState(ControllerState.Paused))
    controller ! Resume
    parent.expectMsg(ReportState(ControllerState.Resuming))
    parent.expectMsg(ReportState(ControllerState.Running))
    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
    parent.ref ! PoisonPill
  }

  "Engine" should "be able to pause csv->sink workflow" in {
    val csvOpDesc = TestOperators.mediumCsvScanOpDesc()
    val sink = TestOperators.sinkOpDesc()
    logger.info(s"csv-id ${csvOpDesc.operatorID}, sink-id ${sink.operatorID}")
    shouldPause(
      "pause-csv-sink",
      mutable.MutableList[OperatorDescriptor](csvOpDesc, sink),
      mutable.MutableList[OperatorLink](OperatorLink(csvOpDesc.operatorID, sink.operatorID))
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
      "pause-csv-keyword-sink",
      mutable.MutableList[OperatorDescriptor](csvOpDesc, keywordOpDesc, sink),
      mutable.MutableList[OperatorLink](
        OperatorLink(csvOpDesc.operatorID, keywordOpDesc.operatorID),
        OperatorLink(keywordOpDesc.operatorID, sink.operatorID)
      )
    )
  }
}
