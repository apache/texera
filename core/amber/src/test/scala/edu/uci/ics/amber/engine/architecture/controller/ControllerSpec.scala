package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.clustering.SingleNodeListener
import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class ControllerSpec
    extends TestKit(ActorSystem("ControllerSpec"))
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

//  private val logicalPlan1 =
//    """{
//      |"operators":[
//      |{"tableName":"D:\\large_input.csv","operatorId":"Scan","operatorType":"LocalScanSource","delimiter":","},
//      |{"attributeName":0,"keyword":"Asia","operatorId":"KeywordSearch","operatorType":"KeywordMatcher"},
//      |{"operatorId":"Count","operatorType":"Aggregation"},
//      |{"operatorId":"Sink","operatorType":"Sink"}],
//      |"links":[
//      |{"origin":"Scan","destination":"KeywordSearch"},
//      |{"origin":"KeywordSearch","destination":"Count"},
//      |{"origin":"Count","destination":"Sink"}]
//      |}""".stripMargin
//
//  private val logicalPlan2 =
//    """{
//      |"operators":[
//      |{"tableName":"D:\\large_input.csv","operatorId":"Scan","operatorType":"LocalScanSource","delimiter":","},
//      |{"operatorId":"Count","operatorType":"Aggregation"},
//      |{"operatorId":"Sink","operatorType":"Sink"}],
//      |"links":[
//      |{"origin":"Scan","destination":"Count"},
//      |{"origin":"Count","destination":"Sink"}]
//      |}""".stripMargin
//
//  private val logicalPlan3 =
//    """{
//      |"operators":[
//      |{"tableName":"D:\\test.txt","operatorId":"Scan","operatorType":"LocalScanSource","delimiter":"|"},
//      |{"attributeName":15,"keyword":"package","operatorId":"KeywordSearch","operatorType":"KeywordMatcher"},
//      |{"operatorId":"Count","operatorType":"Aggregation"},
//      |{"operatorId":"Sink","operatorType":"Sink"}],
//      |"links":[
//      |{"origin":"Scan","destination":"KeywordSearch"},
//      |{"origin":"KeywordSearch","destination":"Count"},
//      |{"origin":"Count","destination":"Sink"}]
//      |}""".stripMargin
//
//  private val logicalPlan4 =
//    """{
//      |"operators":[
//      |{"tableName":"D:\\test.txt","operatorId":"Scan1","operatorType":"LocalScanSource","delimiter":"|","indicesToKeep":null},
//      |{"tableName":"D:\\test.txt","operatorId":"Scan2","operatorType":"LocalScanSource","delimiter":"|","indicesToKeep":null},
//      |{"attributeName":15,"keyword":"package","operatorId":"KeywordSearch","operatorType":"KeywordMatcher"},
//      |{"operatorId":"Join","operatorType":"HashJoin","innerTableIndex":0,"outerTableIndex":0},
//      |{"operatorId":"Count","operatorType":"Aggregation"},
//      |{"operatorId":"Sink","operatorType":"Sink"}],
//      |"links":[
//      |{"origin":"Scan1","destination":"KeywordSearch"},
//      |{"origin":"KeywordSearch","destination":"Join"},
//      |{"origin":"Scan2","destination":"Join"},
//      |{"origin":"Join","destination":"Count"},
//      |{"origin":"Count","destination":"Sink"}]
//      |}""".stripMargin
//
//  "A controller" should "be able to set and trigger count breakpoint in the workflow1" in {
//    val parent = TestProbe()
//    val controller = parent.childActorOf(CONTROLLER.props(logicalPlan1))
//    controller ! AckedControllerInitialization
//    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
//    controller ! PassBreakpointTo("KeywordSearch", new CountGlobalBreakpoint("break1", 100000))
//    controller ! Start
//    parent.expectMsg(ReportState(ControllerState.Running))
//    var isCompleted = false
//    parent.receiveWhile(30.seconds, 10.seconds) {
//      case ReportState(ControllerState.Paused) =>
//        controller ! Resume
//      case ReportState(ControllerState.Completed) =>
//        isCompleted = true
//      case _ =>
//    }
//    assert(isCompleted)
//    parent.ref ! PoisonPill
//  }
//
//  "A controller" should "execute the workflow1 normally" in {
//    val parent = TestProbe()
//    val controller = parent.childActorOf(CONTROLLER.props(logicalPlan1))
//    controller ! AckedControllerInitialization
//    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
//    controller ! Start
//    parent.expectMsg(ReportState(ControllerState.Running))
//    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
//    parent.ref ! PoisonPill
//  }
//
//  "A controller" should "execute the workflow3 normally" in {
//    val parent = TestProbe()
//    val controller = parent.childActorOf(CONTROLLER.props(logicalPlan3))
//    controller ! AckedControllerInitialization
//    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
//    controller ! Start
//    parent.expectMsg(ReportState(ControllerState.Running))
//    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
//    parent.ref ! PoisonPill
//  }
//
//  "A controller" should "execute the workflow2 normally" in {
//    val parent = TestProbe()
//    val controller = parent.childActorOf(CONTROLLER.props(logicalPlan2))
//    controller ! AckedControllerInitialization
//    parent.expectMsg(ReportState(ControllerState.Ready))
//    controller ! Start
//    parent.expectMsg(ReportState(ControllerState.Running))
//    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
//    parent.ref ! PoisonPill
//  }
//
//  "A controller" should "be able to pause/resume the workflow1" in {
//    val parent = TestProbe()
//    val controller = parent.childActorOf(CONTROLLER.props(logicalPlan1))
//    controller ! AckedControllerInitialization
//    parent.expectMsg(ReportState(ControllerState.Ready))
//    controller ! Start
//    parent.expectMsg(ReportState(ControllerState.Running))
//    controller ! Pause
//    parent.expectMsg(ReportState(ControllerState.Pausing))
//    parent.expectMsg(ReportState(ControllerState.Paused))
//    controller ! Resume
//    parent.expectMsg(ReportState(ControllerState.Resuming))
//    parent.expectMsg(ReportState(ControllerState.Running))
//    controller ! Pause
//    parent.expectMsg(ReportState(ControllerState.Pausing))
//    parent.expectMsg(ReportState(ControllerState.Paused))
//    controller ! Resume
//    parent.expectMsg(ReportState(ControllerState.Resuming))
//    parent.expectMsg(ReportState(ControllerState.Running))
//    controller ! Pause
//    parent.expectMsg(ReportState(ControllerState.Pausing))
//    parent.expectMsg(ReportState(ControllerState.Paused))
//    controller ! Resume
//    parent.expectMsg(ReportState(ControllerState.Resuming))
//    parent.expectMsg(ReportState(ControllerState.Running))
//    controller ! Pause
//    parent.expectMsg(ReportState(ControllerState.Pausing))
//    parent.expectMsg(ReportState(ControllerState.Paused))
//    controller ! Resume
//    parent.expectMsg(ReportState(ControllerState.Resuming))
//    parent.expectMsg(ReportState(ControllerState.Running))
//    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
//    parent.ref ! PoisonPill
//  }

//  "A controller" should "be able to modify the logic after pausing the workflow1" in {
//    val parent = TestProbe()
//    val controller = parent.childActorOf(CONTROLLER.props(logicalPlan1))
//    controller ! AckedControllerInitialization
//    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
//    controller ! Start
//    parent.expectMsg(ReportState(ControllerState.Running))
//    Thread.sleep(300)
//    controller ! Pause
//    parent.expectMsg(ReportState(ControllerState.Pausing))
//    parent.expectMsg(ReportState(ControllerState.Paused))
//    controller ! ModifyLogic(
//      new KeywordSearchMetadata(
//        OperatorTag("sample", "KeywordSearch"),
//        Constants.currentWorkerNum,
//        0,
//        "asia"
//      )
//    )
//    parent.expectMsg(Ack)
//    Thread.sleep(10000)
//    controller ! Resume
//    parent.expectMsg(ReportState(ControllerState.Resuming))
//    parent.expectMsg(ReportState(ControllerState.Running))
//    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
//    parent.ref ! PoisonPill
//  }

//  "A controller" should "be able to set and trigger conditional breakpoint in the workflow1" in {
//    val parent = TestProbe()
//    val controller = parent.childActorOf(CONTROLLER.props(logicalPlan1))
//    controller ! AckedControllerInitialization
//    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
//    controller ! PassBreakpointTo(
//      "KeywordSearch",
//      new ConditionalGlobalBreakpoint("break2", x => x.getString(8).toInt == 9884)
//    )
//    controller ! Start
//    parent.expectMsg(ReportState(ControllerState.Running))
//    var isCompleted = false
//    parent.receiveWhile(30.seconds, 10.seconds) {
//      case ReportState(ControllerState.Paused) =>
//        controller ! Resume
//      case ReportState(ControllerState.Completed) =>
//        isCompleted = true
//      case _ =>
//    }
//    assert(isCompleted)
//    parent.ref ! PoisonPill
//  }
//
//  "A controller" should "be able to set and trigger count breakpoint on complete in the workflow1" in {
//    val parent = TestProbe()
//    val controller = parent.childActorOf(CONTROLLER.props(logicalPlan1))
//    controller ! AckedControllerInitialization
//    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
//    controller ! PassBreakpointTo("KeywordSearch", new CountGlobalBreakpoint("break1", 146017))
//    controller ! Start
//    parent.expectMsg(ReportState(ControllerState.Running))
//    var isCompleted = false
//    parent.receiveWhile(30.seconds, 10.seconds) {
//      case ReportState(ControllerState.Paused) =>
//        controller ! Resume
//      case ReportState(ControllerState.Completed) =>
//        isCompleted = true
//      case _ =>
//    }
//    assert(isCompleted)
//    parent.ref ! PoisonPill
//  }
//
//  "A controller" should "be able to pause/resume with conditional breakpoint in the workflow1" in {
//    val parent = TestProbe()
//    val controller = parent.childActorOf(CONTROLLER.props(logicalPlan1))
//    controller ! AckedControllerInitialization
//    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
//    controller ! PassBreakpointTo(
//      "KeywordSearch",
//      new ConditionalGlobalBreakpoint("break2", x => x.getString(8).toInt == 9884)
//    )
//    controller ! Start
//    parent.expectMsg(ReportState(ControllerState.Running))
//    val random = new Random()
//    for (i <- 0 until 100) {
//      if (random.nextBoolean()) {
//        controller ! Pause
//      } else {
//        controller ! Resume
//      }
//    }
//    controller ! Resume
//    var isCompleted = false
//    parent.receiveWhile(30.seconds, 10.seconds) {
//      case ReportState(ControllerState.Paused) =>
//        controller ! Resume
//      case ReportState(ControllerState.Completed) =>
//        isCompleted = true
//      case _ =>
//    }
//    assert(isCompleted)
//    parent.ref ! PoisonPill
//  }
//
//  "A controller" should "be able to pause/resume with count breakpoint in the workflow1" in {
//    val parent = TestProbe()
//    val controller = parent.childActorOf(CONTROLLER.props(logicalPlan1))
//    controller ! AckedControllerInitialization
//    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
//    controller ! PassBreakpointTo("KeywordSearch", new CountGlobalBreakpoint("break1", 100000))
//    controller ! Start
//    parent.expectMsg(ReportState(ControllerState.Running))
//    val random = new Random()
//    for (i <- 0 until 100) {
//      if (random.nextBoolean()) {
//        controller ! Pause
//      } else {
//        controller ! Resume
//      }
//    }
//    controller ! Resume
//    var isCompleted = false
//    parent.receiveWhile(30.seconds, 10.seconds) {
//      case ReportState(ControllerState.Paused) =>
//        controller ! Resume
//      case ReportState(ControllerState.Completed) =>
//        isCompleted = true
//      case _ =>
//    }
//    assert(isCompleted)
//    parent.ref ! PoisonPill
//  }
//
//  "A controller" should "execute the workflow4 normally" in {
//    val parent = TestProbe()
//    val controller = parent.childActorOf(CONTROLLER.props(logicalPlan4))
//    controller ! AckedControllerInitialization
//    parent.expectMsg(ReportState(ControllerState.Ready))
//    controller ! Start
//    parent.expectMsg(ReportState(ControllerState.Running))
//    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
//    parent.ref ! PoisonPill
//  }

}
