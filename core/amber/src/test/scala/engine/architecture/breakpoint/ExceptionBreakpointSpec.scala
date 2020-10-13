package engine.architecture.breakpoint

import clustering.SingleNodeListener
import engine.architecture.breakpoint.globalbreakpoint.{
  ConditionalGlobalBreakpoint,
  CountGlobalBreakpoint
}
import engine.architecture.controller.{Controller, ControllerState}
import engine.common.AdvancedMessageSending
import engine.common.ambermessage.ControlMessage.{
  ModifyTuple,
  Resume,
  ResumeTuple,
  SkipTuple,
  Start
}
import engine.common.ambermessage.ControllerMessage.{
  AckedControllerInitialization,
  PassBreakpointTo,
  ReportGlobalBreakpointTriggered,
  ReportState
}
import engine.common.ambermessage.WorkerMessage.{DataMessage, EndSending}
import engine.common.ambertag.{LayerTag, LinkTag, OperatorIdentifier, WorkerTag, WorkflowTag}
import engine.common.tuple.Tuple
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.event.LoggingAdapter
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._
import scala.util.Random

class ExceptionBreakpointSpec
    extends TestKit(ActorSystem("PrincipalSpec"))
    with ImplicitSender
    with FlatSpecLike
    with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val log: LoggingAdapter = system.log

  private val logicalPlan1 =
    """{
      |"operators":[
      |{"tableName":"D:\\small_input.csv","operatorID":"Scan","operatorType":"LocalScanSource","delimiter":","},
      |{"attributeName":0,"keyword":"asia","operatorID":"KeywordSearch1","operatorType":"KeywordMatcher"},
      |{"operatorID":"Sink","operatorType":"Sink"}],
      |"links":[
      |{"origin":"Scan","destination":"KeywordSearch1"},
      |{"origin":"KeywordSearch1","destination":"Sink"}]
      |}""".stripMargin

  private val logicalPlan2 =
    """{
      |"operators":[
      |{"limit":10000,"delay":0,"operatorID":"Gen","operatorType":"Generate"},
      |{"operatorID":"Count","operatorType":"Aggregation"},
      |{"operatorID":"Sink","operatorType":"Sink"}],
      |"links":[
      |{"origin":"Gen","destination":"Count"},
      |{"origin":"Count","destination":"Sink"}]
      |}""".stripMargin

  val workflowTag = WorkflowTag("sample")
  var index = 0
  val opTag: () => OperatorIdentifier = () => { index += 1; OperatorIdentifier(workflowTag, index.toString) }
  val layerTag: () => LayerTag = () => { index += 1; LayerTag(opTag(), index.toString) }
  val workerTag: () => WorkerTag = () => { index += 1; WorkerTag(layerTag(), index) }
  val linkTag: () => LinkTag = () => { LinkTag(layerTag(), layerTag()) }

  def resultValidation(expectedTupleCount: Int, idleTime: Duration = 2.seconds): Unit = {
    var counter = 0
    var receivedEnd = false
    receiveWhile(5.minutes, idleTime) {
      case DataMessage(seq, payload) => counter += payload.length
      case EndSending(seq)           => receivedEnd = true
      case msg                       =>
    }
    assert(counter == expectedTupleCount)
    assert(receivedEnd)
  }

  override def beforeAll: Unit = {
    system.actorOf(Props[SingleNodeListener], "cluster-info")
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A workflow" should "be able to detect faulted tuples and trigger exception breakpoint in the workflow1, then skip them" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(30.seconds, 10.seconds) {
      case ReportGlobalBreakpointTriggered(bp, opID) =>
        for (i <- bp) {
          log.info(
            (if (i._1._2.isInput) "[IN]" else "[OUT]") + i._1._2.tuple + " ERRORS: [" + i._2
              .mkString(",") + "]"
          )
          AdvancedMessageSending.blockingAskWithRetry(i._1._1, SkipTuple(i._1._2), 5)
        }
        controller ! Resume
      case ReportState(ControllerState.Paused) =>
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }

  "A workflow" should "be able to detect faulted tuples and trigger exception breakpoint in the workflow1, then modify them" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(30.seconds, 10.seconds) {
      case ReportGlobalBreakpointTriggered(bp, opID) =>
        for (i <- bp) {
          log.info(
            (if (i._1._2.isInput) "[IN]" else "[OUT]") + i._1._2.tuple + " ERRORS: [" + i._2
              .mkString(",") + "]"
          )
          val fixed = new FaultedTuple(
            Tuple("Asia", "Rwanda", "1", "0", "0", "0", "0", "0", "0", "12", "12", "120", "12"),
            i._1._2.id,
            i._1._2.isInput
          )
          AdvancedMessageSending.blockingAskWithRetry(i._1._1, ModifyTuple(fixed), 5)
        }
        controller ! Resume
      case ReportState(ControllerState.Paused) =>
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }

  "A workflow" should "be able to trigger conditional breakpoint in the workflow2, then resume them" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan2))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
    controller ! PassBreakpointTo(
      "Gen",
      new ConditionalGlobalBreakpoint("ConditionalBreakpoint", x => x.getInt(0) % 1000 == 0)
    )
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(30.seconds, 10.seconds) {
      case ReportGlobalBreakpointTriggered(bp, opID) =>
        for (i <- bp) {
          log.info(
            (if (i._1._2.isInput) "[IN]" else "[OUT]") + i._1._2.tuple + " ERRORS: [" + i._2
              .mkString(",") + "]"
          )
          AdvancedMessageSending.blockingAskWithRetry(i._1._1, ResumeTuple(i._1._2), 5)
        }
        controller ! Resume
      case ReportState(ControllerState.Paused) =>
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }

  "A workflow" should "be able to trigger conditional breakpoint in the workflow2, then skip them" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan2))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
    controller ! PassBreakpointTo(
      "Gen",
      new ConditionalGlobalBreakpoint("ConditionalBreakpoint", x => x.getInt(0) % 1000 == 0)
    )
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(30.seconds, 10.seconds) {
      case ReportGlobalBreakpointTriggered(bp, opID) =>
        for (i <- bp) {
          log.info(
            (if (i._1._2.isInput) "[IN]" else "[OUT]") + i._1._2.tuple + " ERRORS: [" + i._2
              .mkString(",") + "]"
          )
          AdvancedMessageSending.blockingAskWithRetry(i._1._1, SkipTuple(i._1._2), 5)
        }
        controller ! Resume
      case ReportState(ControllerState.Paused) =>
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }

  "A workflow" should "be able to trigger count breakpoint in the workflow2, then resume it" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan2))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
    controller ! PassBreakpointTo("Gen", new CountGlobalBreakpoint("CountBreakpoint", 500))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(30.seconds, 10.seconds) {
      case ReportGlobalBreakpointTriggered(bp, opID) =>
        for (i <- bp) {
          log.info(
            (if (i._1._2.isInput) "[IN]" else "[OUT]") + i._1._2.tuple + " ERRORS: [" + i._2
              .mkString(",") + "]"
          )
        }
        controller ! Resume
      case ReportState(ControllerState.Paused) =>
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }

  "A workflow" should "be able to trigger conditional breakpoint in the workflow2, then resume it" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan2))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
    controller ! PassBreakpointTo(
      "Gen",
      new ConditionalGlobalBreakpoint("ConditionalBreakpoint", x => x.getInt(0) % 1000 == 0)
    )
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(30.seconds, 10.seconds) {
      case ReportGlobalBreakpointTriggered(bp, opID) =>
        for (i <- bp) {
          log.info(
            (if (i._1._2.isInput) "[IN]" else "[OUT]") + i._1._2.tuple + " ERRORS: [" + i._2
              .mkString(",") + "]"
          )
        }
        controller ! Resume
      case ReportState(ControllerState.Paused) =>
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }

  "A workflow" should "be able to trigger count breakpoint in the workflow1, then resume it" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
    controller ! PassBreakpointTo("KeywordSearch1", new CountGlobalBreakpoint("CountBreakpoint", 3))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(3000.seconds, 1000.seconds) {
      case ReportGlobalBreakpointTriggered(bp, opID) =>
        for (i <- bp) {
          log.info(
            (if (i._1._2.isInput) "[IN]" else "[OUT]") + i._1._2.tuple + " ERRORS: [" + i._2
              .mkString(",") + "]"
          )
        }
        controller ! Resume
      case ReportState(ControllerState.Paused) =>
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }

}
