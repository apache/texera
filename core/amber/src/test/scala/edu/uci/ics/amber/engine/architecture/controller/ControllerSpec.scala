package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.{
  ConditionalGlobalBreakpoint,
  CountGlobalBreakpoint
}
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage.{
  Ack,
  ModifyLogic,
  Pause,
  Resume,
  Start
}
import edu.uci.ics.amber.engine.common.ambermessage.ControllerMessage.{
  AckedControllerInitialization,
  PassBreakpointTo,
  ReportState
}
import edu.uci.ics.amber.engine.common.ambertag.{OperatorIdentifier, WorkflowTag}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.{Constants, IOperatorExecutor}
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkSenderActorRef
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  CongestionControl,
  NetworkCommunicationActor
}
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.architecture.worker.neo.{DataProcessor, WorkerInternalQueue}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.{
  ActorVirtualIdentity,
  WorkerActorVirtualIdentity
}
import edu.uci.ics.amber.engine.e2e.TestOperators
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.{
  BreakpointInfo,
  OperatorLink,
  OperatorPort,
  WorkflowCompiler,
  WorkflowInfo
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._
import scala.util.Random

class ControllerSpec
    extends TestKit(ActorSystem("ControllerSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with MockFactory {

  implicit val timeout: Timeout = Timeout(5.seconds)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override def beforeAll: Unit = {
    system.actorOf(Props[SingleNodeListener], "cluster-info")
  }
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

//  "Controller" should "process DetectSkew message properly" in {
//    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
//    val headerlessCsvOpDesc2 = TestOperators.headerlessSmallCsvScanOpDesc()
//    val joinOpDesc = TestOperators.joinOpDesc("column0", "column0")
//    val sink = TestOperators.sinkOpDesc()
//
//    val parent = TestProbe()
//    val context = new WorkflowContext
//    context.workflowID = "workflow-test"
//
//    val texeraWorkflowCompiler = new WorkflowCompiler(
//      WorkflowInfo(
//        mutable.MutableList[OperatorDescriptor](
//          headerlessCsvOpDesc1,
//          headerlessCsvOpDesc2,
//          joinOpDesc,
//          sink
//        ),
//        mutable.MutableList[OperatorLink](
//          OperatorLink(
//            OperatorPort(headerlessCsvOpDesc1.operatorID, 0),
//            OperatorPort(joinOpDesc.operatorID, 0)
//          ),
//          OperatorLink(
//            OperatorPort(headerlessCsvOpDesc2.operatorID, 0),
//            OperatorPort(joinOpDesc.operatorID, 1)
//          ),
//          OperatorLink(
//            OperatorPort(joinOpDesc.operatorID, 0),
//            OperatorPort(sink.operatorID, 0)
//          )
//        ),
//        mutable.MutableList[BreakpointInfo]()
//      ),
//      context
//    )
//    texeraWorkflowCompiler.init()
//    val workflow = texeraWorkflowCompiler.amberWorkflow
//    val workflowTag = WorkflowTag.apply("workflow-test")
//
//    val controller = parent.childActorOf(
//      Controller.props(workflowTag, workflow, false, ControllerEventListener(), 100)
//    )
//    controller ! AckedControllerInitialization
//    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
//
//    val mockDataProcessor = mock[DataProcessor]
//    (mockDataProcessor.getQueueSize _).expects().returning(10)
//
//    val controllerNetworkSender = TestProbe()
//    val fakeJoinWorker = TestActorRef(
//      new WorkflowWorker(
//        WorkerActorVirtualIdentity("worker-join"),
//        mock[IOperatorExecutor],
//        controllerNetworkSender.ref
//      ) {
//        override lazy val dataProcessor = mockDataProcessor
//      }
//    )
//
//    val fakeScanWorker = TestActorRef(
//      new WorkflowWorker(
//        WorkerActorVirtualIdentity("worker-scan"),
//        mock[IOperatorExecutor],
//        controllerNetworkSender.ref
//      ) {
//        override lazy val dataProcessor = mockDataProcessor
//        override val networkCommunicationActor = NetworkSenderActorRef(fakeNetworkCommActor)
//      }
//    )
//
//    var congestionControl = mock[CongestionControl]
//    (congestionControl.getUnsentAndTransitMsgCount _).expects().returning(30)
//    val fakeNetworkCommActor =
//      TestActorRef(new NetworkCommunicationActor(controllerNetworkSender.ref) {
//        override val idToActorRefs = mutable.HashMap[ActorVirtualIdentity, ActorRef](
//          WorkerActorVirtualIdentity("worker-scan") -> fakeScanWorker
//        )
//        override val idToCongestionControls =
//          mutable.HashMap[ActorVirtualIdentity, CongestionControl](
//            WorkerActorVirtualIdentity("worker-join") -> congestionControl
//          )
//      })
//
//    fakeScanWorker.underlyingActor.networkCommunicationActor =
//      NetworkSenderActorRef(fakeNetworkCommActor)
//  }

//  private val logicalPlan1 =
//    """{
//      |"operators":[
//      |{"tableName":"D:\\large_input.csv","operatorID":"Scan","operatorType":"LocalScanSource","delimiter":","},
//      |{"attributeName":0,"keyword":"Asia","operatorID":"KeywordSearch","operatorType":"KeywordMatcher"},
//      |{"operatorID":"Count","operatorType":"Aggregation"},
//      |{"operatorID":"Sink","operatorType":"Sink"}],
//      |"links":[
//      |{"origin":"Scan","destination":"KeywordSearch"},
//      |{"origin":"KeywordSearch","destination":"Count"},
//      |{"origin":"Count","destination":"Sink"}]
//      |}""".stripMargin
//
//  private val logicalPlan2 =
//    """{
//      |"operators":[
//      |{"tableName":"D:\\large_input.csv","operatorID":"Scan","operatorType":"LocalScanSource","delimiter":","},
//      |{"operatorID":"Count","operatorType":"Aggregation"},
//      |{"operatorID":"Sink","operatorType":"Sink"}],
//      |"links":[
//      |{"origin":"Scan","destination":"Count"},
//      |{"origin":"Count","destination":"Sink"}]
//      |}""".stripMargin
//
//  private val logicalPlan3 =
//    """{
//      |"operators":[
//      |{"tableName":"D:\\test.txt","operatorID":"Scan","operatorType":"LocalScanSource","delimiter":"|"},
//      |{"attributeName":15,"keyword":"package","operatorID":"KeywordSearch","operatorType":"KeywordMatcher"},
//      |{"operatorID":"Count","operatorType":"Aggregation"},
//      |{"operatorID":"Sink","operatorType":"Sink"}],
//      |"links":[
//      |{"origin":"Scan","destination":"KeywordSearch"},
//      |{"origin":"KeywordSearch","destination":"Count"},
//      |{"origin":"Count","destination":"Sink"}]
//      |}""".stripMargin
//
//  private val logicalPlan4 =
//    """{
//      |"operators":[
//      |{"tableName":"D:\\test.txt","operatorID":"Scan1","operatorType":"LocalScanSource","delimiter":"|","indicesToKeep":null},
//      |{"tableName":"D:\\test.txt","operatorID":"Scan2","operatorType":"LocalScanSource","delimiter":"|","indicesToKeep":null},
//      |{"attributeName":15,"keyword":"package","operatorID":"KeywordSearch","operatorType":"KeywordMatcher"},
//      |{"operatorID":"Join","operatorType":"HashJoin","innerTableIndex":0,"outerTableIndex":0},
//      |{"operatorID":"Count","operatorType":"Aggregation"},
//      |{"operatorID":"Sink","operatorType":"Sink"}],
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
//    val controller = parent.childActorOf(Controller.props(logicalPlan1))
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
//    val controller = parent.childActorOf(Controller.props(logicalPlan1))
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
//    val controller = parent.childActorOf(Controller.props(logicalPlan3))
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
//    val controller = parent.childActorOf(Controller.props(logicalPlan2))
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
//    val controller = parent.childActorOf(Controller.props(logicalPlan1))
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
//    val controller = parent.childActorOf(Controller.props(logicalPlan1))
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
//        Constants.defaultNumWorkers,
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
//    val controller = parent.childActorOf(Controller.props(logicalPlan1))
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
//    val controller = parent.childActorOf(Controller.props(logicalPlan1))
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
//    val controller = parent.childActorOf(Controller.props(logicalPlan1))
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
//    val controller = parent.childActorOf(Controller.props(logicalPlan1))
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
//    val controller = parent.childActorOf(Controller.props(logicalPlan4))
//    controller ! AckedControllerInitialization
//    parent.expectMsg(ReportState(ControllerState.Ready))
//    controller ! Start
//    parent.expectMsg(ReportState(ControllerState.Running))
//    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
//    parent.ref ! PoisonPill
//  }

}
