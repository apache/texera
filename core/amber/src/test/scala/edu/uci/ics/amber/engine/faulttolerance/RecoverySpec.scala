package edu.uci.ics.amber.engine.faulttolerance

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import edu.uci.ics.amber.clustering.SingleNodeListener
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class RecoverySpec
    extends TestKit(ActorSystem("RecoverySpec"))
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
//      |{"tableName":"D:\\large_input.csv","operatorID":"Scan","operatorType":"LocalScanSource","delimiter":","},
//      |{"attributeName":0,"keyword":"123123","operatorID":"KeywordSearch","operatorType":"KeywordMatcher"},
//      |{"operatorID":"Sink","operatorType":"Sink"}],
//      |"links":[
//      |{"origin":"Scan","destination":"KeywordSearch"},
//      |{"origin":"KeywordSearch","destination":"Sink"}]
//      |}""".stripMargin
//
//  private val logicalPlan3 =
//    """{
//      |"operators":[
//      |{"tableName":"D:\\test.txt","operatorID":"Scan1","operatorType":"LocalScanSource","delimiter":"|"},
//      |{"tableName":"D:\\test.txt","operatorID":"Scan2","operatorType":"LocalScanSource","delimiter":"|"},
//      |{"attributeName":15,"keyword":"package","operatorID":"KeywordSearch","operatorType":"KeywordMatcher"},
//      |{"operatorID":"Join","operatorType":"HashJoin","innerTableIndex":0,"outerTableIndex":0},
//      |{"operatorID":"GroupBy1","operatorType":"GroupBy","groupByField":1,"aggregateField":0,"aggregationType":"Count"},
//      |{"operatorID":"GroupBy2","operatorType":"GroupBy","groupByField":1,"aggregateField":0,"aggregationType":"Count"},
//      |{"operatorID":"Sink","operatorType":"Sink"}],
//      |"links":[
//      |{"origin":"Scan1","destination":"KeywordSearch"},
//      |{"origin":"Scan2","destination":"Join"},
//      |{"origin":"KeywordSearch","destination":"Join"},
//      |{"origin":"Join","destination":"GroupBy1"},
//      |{"origin":"GroupBy1","destination":"GroupBy2"},
//      |{"origin":"GroupBy2","destination":"Sink"}]
//      |}""".stripMargin
//
//  "A controller" should "pause, stop and restart, then pause the execution of the workflow1" in {
//    val parent = TestProbe()
//    val context = new WorkflowContext
//    context.jobID = "workflow-test"
//    val objectMapper = Utils.objectMapper
//    val request =
//      objectMapper.readValue(
//        WorkflowJSONExamples.csvToKeywordToSink,
//        classOf[WorkflowExecuteRequest]
//      )
//    val texeraWorkflowCompiler = new WorkflowCompiler(
//      WorkflowInfo(request.operators, request.links, request.breakpoints),
//      context
//    )
//    texeraWorkflowCompiler.init()
//    val workflow = texeraWorkflowCompiler.amberWorkflow
//    val workflowTag = WorkflowTag.apply("workflow-test")
//
//    val controller = parent.childActorOf(
//      CONTROLLER.props(workflowTag, workflow, false, ControllerEventListener(), 100)
//    )
//    controller ! AckedControllerInitialization
//    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
//    controller ! Start
//    parent.expectMsg(ReportState(ControllerState.Running))
//    Thread.sleep(100)
//    controller ! Pause
//    parent.expectMsg(ReportState(ControllerState.Pausing))
//    parent.expectMsg(ReportState(ControllerState.Paused))
//    controller ! KillAndRecover
//    parent.expectMsg(5.minutes, ReportState(ControllerState.Paused))
//    controller ! Resume
//    parent.expectMsg(5.minutes, ReportState(ControllerState.Resuming))
//    parent.expectMsg(5.minutes, ReportState(ControllerState.Running))
//    parent.expectMsg(5.minutes, ReportState(ControllerState.Completed))
//    parent.ref ! PoisonPill
//  }

}
