package Engine.Operators.HashJoin

import Clustering.SingleNodeListener
import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Architecture.Principal.{Principal, PrincipalState}
import Engine.Architecture.SendSemantics.DataTransferPolicy.{HashBasedShufflePolicy, OneToOnePolicy, RoundRobinPolicy}
import Engine.Architecture.SendSemantics.Routees.DirectRoutee
import Engine.Architecture.Worker.{Generator, Processor}
import Engine.Common.AmberField.FieldType
import Engine.Common.AmberMessage.PrincipalMessage.{AckedPrincipalInitialization, GetInputLayer, GetOutputLayer, ReportPrincipalPartialCompleted, ReportState}
import Engine.Common.AmberMessage.WorkerMessage.{AckedWorkerInitialization, DataMessage, EndSending, UpdateInputLinking, UpdateOutputLinking}
import Engine.Common.AmberTag.{LayerTag, LinkTag, OperatorTag, WorkerTag, WorkflowTag}
import Engine.Common.AmberTuple.Tuple
import Engine.Operators.SimpleCollection.{SimpleProcessOperatorMetadata, SimpleSourceOperatorMetadata, SimpleTupleProcessor, SimpleTupleProducer}
import Engine.Common.{TableMetadata, TupleMetadata}
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import akka.pattern.ask

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

class HashJoinPrincipalSpec
  extends TestKit(ActorSystem("HashJoinPrincipalSpec"))
    with ImplicitSender
    with FlatSpecLike
    with BeforeAndAfterAll {

  val dataSet1 = Array(
    Tuple("Asia",1),
    Tuple("Europe",2),
    Tuple("some",3),
    Tuple("important",4),
    Tuple("keywords",5),
    Tuple("Japan",6),
    Tuple("cat",1),
    Tuple("cat boomer",2),
    Tuple("cat lover",3),
    Tuple("asia",4)
  )

  val dataSet2 = Array(
    Tuple("Japan",4),
    Tuple("cat",3),
    Tuple("cat boomer",2),
    Tuple("cat lover",1),
    Tuple("asia",0)
  )

  val workflowTag = WorkflowTag("sample")
  var index=0
  val opTag: () => OperatorTag = ()=>{index+=1; OperatorTag(workflowTag,index.toString)}
  val layerTag: () => LayerTag = ()=>{index+=1; LayerTag(opTag(),index.toString)}
  val workerTag: () => WorkerTag = ()=>{index+=1; WorkerTag(layerTag(),index)}
  val linkTag: () => LinkTag = ()=>{LinkTag(layerTag(),layerTag())}

  override def beforeAll:Unit = {
    system.actorOf(Props[SingleNodeListener],"cluster-info")
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A HashJoinPrincipal with 5 workers" should "join 2 datasets by the first column" in {
    implicit val timeout = Timeout(5.seconds)
    val sendActor1 = system.actorOf(Processor.props(new SimpleTupleProcessor(),workerTag()))
    sendActor1 ! AckedWorkerInitialization
    sendActor1 ! UpdateInputLinking(testActor,null)
    val sendActor2 = system.actorOf(Processor.props(new SimpleTupleProcessor(),workerTag()))
    sendActor2 ! AckedWorkerInitialization
    sendActor2 ! UpdateInputLinking(testActor,null)
    val parent = TestProbe()
    val receiver = TestProbe()
    parent.ignoreMsg{ case ReportPrincipalPartialCompleted(x,y) => true }
    val innertableTag = layerTag()
    val outertableTag = layerTag()
    val joinMetadata = new HashJoinMetadata[String](opTag(),5,0,0)
    joinMetadata.innerTableTag = innertableTag
    val joinTag = joinMetadata.topology.layers.head.tag
    val innerHash = joinMetadata.getShuffleHashFunction(innertableTag)
    val outerHash = joinMetadata.getShuffleHashFunction(outertableTag)
    val principal = parent.childActorOf(Principal.props(joinMetadata))
    principal ! AckedPrincipalInitialization(Array())
    parent.expectMsg(ReportState(PrincipalState.Ready))
    sendActor1 ! UpdateOutputLinking(new HashBasedShufflePolicy(1,innerHash),LinkTag(innertableTag,joinTag),joinMetadata.topology.layers.head.layer.map(new DirectRoutee(_)))
    sendActor2 ! UpdateOutputLinking(new HashBasedShufflePolicy(1,outerHash),LinkTag(outertableTag,joinTag),joinMetadata.topology.layers.head.layer.map(new DirectRoutee(_)))
    val output = Await.result(principal ? GetOutputLayer,timeout.duration).asInstanceOf[ActorLayer]
    output.layer.foreach(x => x ! UpdateOutputLinking(new OneToOnePolicy(10),linkTag(),Array(new DirectRoutee(receiver.ref))))
    sendActor1 ! DataMessage(0,dataSet2)
    sendActor1 ! EndSending(1)
    //wait for first table to finish
    Thread.sleep(1000)
    sendActor2 ! DataMessage(0,dataSet1)
    sendActor2 ! EndSending(1)
    parent.expectMsg(ReportState(PrincipalState.Running))
    var res = Set[Tuple]()
    receiver.receiveWhile(5.seconds,2.seconds){
      case DataMessage(_,payload) => res ++= Set(payload:_*)
      case msg =>
    }
    //parent.expectMsg(ReportState(PrincipalState.Completed))
    assert(res == Set(
      Tuple("Japan",6,"Japan",4),
      Tuple("cat",1,"cat",3),
      Tuple("cat boomer",2,"cat boomer",2),
      Tuple("cat lover",3,"cat lover",1),
      Tuple("asia",4,"asia",0)))
  }

}
