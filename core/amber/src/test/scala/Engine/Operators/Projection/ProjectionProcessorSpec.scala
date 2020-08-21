package Engine.Operators.Projection

import Engine.Architecture.SendSemantics.DataTransferPolicy.{OneToOnePolicy, RoundRobinPolicy}
import Engine.Architecture.SendSemantics.Routees.DirectRoutee
import Engine.Architecture.Worker.Processor
import Engine.Common.AmberField.FieldType
import Engine.Common.AmberMessage.WorkerMessage.{AckedWorkerInitialization, DataMessage, EndSending, ReportWorkerPartialCompleted, UpdateInputLinking, UpdateOutputLinking}
import Engine.Common.AmberTag.{LayerTag, LinkTag, OperatorTag, WorkerTag, WorkflowTag}
import Engine.Common.AmberTuple.Tuple
import Engine.Common.{TableMetadata, TupleMetadata}
import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._

class ProjectionProcessorSpec
  extends TestKit(ActorSystem("ProjectionProcessorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with BeforeAndAfterAll {

  val dataSet = Array(
    Tuple("Asia",1,"xa"),
    Tuple("Europe",2,"xa"),
    Tuple("some",3,"xb"),
    Tuple("important",4,"xc"),
    Tuple("keywords",5,"xc")
  )

  val workflowTag = WorkflowTag("sample")
  var index=0
  val opTag: () => OperatorTag = ()=>{index+=1; OperatorTag(workflowTag,index.toString)}
  val layerTag: () => LayerTag = ()=>{index+=1; LayerTag(opTag(),index.toString)}
  val workerTag: () => WorkerTag = ()=>{index+=1; WorkerTag(layerTag(),index)}
  val linkTag: () => LinkTag = ()=>{LinkTag(layerTag(),layerTag())}

  override def beforeAll:Unit = {

  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "An ProjectionTupleProcessor" should "apply projection on the last field" in {
    implicit val timeout = Timeout(5.seconds)
    ignoreMsg{
      case UpdateInputLinking(_,_) => true
      case ReportWorkerPartialCompleted(_,_) => true
    }
    val metadata=new TableMetadata("table1",new TupleMetadata(Array[FieldType.Value](FieldType.String)))
    val execActor = system.actorOf(Processor.props(new ProjectionTupleProcessor(Array(2)),workerTag()))
    execActor ? AckedWorkerInitialization
    execActor ? UpdateInputLinking(testActor,null)
    val output = new RoundRobinPolicy(5)
    execActor ? UpdateOutputLinking(output,linkTag(),Array(new DirectRoutee(testActor)))
    execActor ! DataMessage(0,dataSet)
    execActor ! EndSending(1)
    expectMsg(DataMessage(0,Array(Tuple("xa"),Tuple("xa"),Tuple("xb"),Tuple("xc"),Tuple("xc"))))
    expectMsg(EndSending(1))
    Thread.sleep(1000)
    execActor ! PoisonPill
  }


  "An ProjectionTupleProcessor" should "apply projection on the second field" in {
    implicit val timeout = Timeout(5.seconds)
    ignoreMsg{
      case UpdateInputLinking(_,_) => true
      case ReportWorkerPartialCompleted(_,_) => true
    }
    val metadata=new TableMetadata("table1",new TupleMetadata(Array[FieldType.Value](FieldType.String)))
    val execActor = system.actorOf(Processor.props(new ProjectionTupleProcessor(Array(1)),workerTag()))
    execActor ? AckedWorkerInitialization
    execActor ? UpdateInputLinking(testActor,null)
    val output = new RoundRobinPolicy(5)
    execActor ? UpdateOutputLinking(output,linkTag(),Array(new DirectRoutee(testActor)))
    execActor ! DataMessage(0,dataSet)
    execActor ! EndSending(1)
    expectMsg(DataMessage(0,Array(Tuple(1),Tuple(2),Tuple(3),Tuple(4),Tuple(5))))
    expectMsg(EndSending(1))
    Thread.sleep(1000)
    execActor ! PoisonPill
  }

}
