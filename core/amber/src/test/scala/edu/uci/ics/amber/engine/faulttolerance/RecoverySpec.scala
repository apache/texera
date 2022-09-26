package edu.uci.ics.amber.engine.faulttolerance

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.ScalaKryoInstantiator
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.logging.{InMemDeterminant, ProcessControlMessage}
import edu.uci.ics.amber.engine.architecture.worker.workloadmetrics.SelfWorkloadMetrics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ReturnInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import com.twitter.chill.KryoInstantiator
import com.twitter.chill.KryoPool

import java.io.ByteArrayOutputStream
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
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

  "Controller" should "serialize" in {
    val selfworkload = SelfWorkloadMetrics(1,1,1,1)
    val buffer = ArrayBuffer[mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]]()
    val m = mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]()
    m(ActorVirtualIdentity("1")) = ArrayBuffer[Long](1,2,3,4)
    buffer.append(m)
    val instantiator = new ScalaKryoInstantiator
    instantiator.setRegistrationRequired(false)
    val kryo = instantiator.newKryo()
    val b = new ByteArrayOutputStream()
    val output = new Output(b)
    val a = ProcessControlMessage(ReturnInvocation(1,(selfworkload, buffer)), ActorVirtualIdentity("test"))
    kryo.writeClassAndObject(output, a)
    output.flush()
    output.close()
    val input = new Input(output.getBuffer)
    val deser = kryo.readClassAndObject(input).asInstanceOf[InMemDeterminant]
    println(deser)
  }

  "Controller" should "serialize2" in {
    val selfworkload = SelfWorkloadMetrics(1,1,1,1)
    val buffer = ArrayBuffer[mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]]()
    val m = mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]()
    m(ActorVirtualIdentity("1")) = ArrayBuffer[Long](1,2,3,4)
    buffer.append(m)
    val a = ProcessControlMessage(ReturnInvocation(1,(selfworkload, buffer)), ActorVirtualIdentity("test"))
    val POOL_SIZE = 10
    val in = new KryoInstantiator
    val kryo = KryoPool.withByteArrayOutputStream(POOL_SIZE, in)
    val ser = kryo.toBytesWithClass(a)
    val deserObj = kryo.fromBytes(ser)
    println(deserObj)
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
