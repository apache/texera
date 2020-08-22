package Engine.Operators.Count

import Clustering.SingleNodeListener
import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Architecture.Principal.{Principal, PrincipalState}
import Engine.Architecture.SendSemantics.DataTransferPolicy.{OneToOnePolicy, RoundRobinPolicy}
import Engine.Architecture.SendSemantics.Routees.DirectRoutee
import Engine.Architecture.Worker.{Generator, Processor}
import Engine.Common.AmberField.FieldType
import Engine.Common.AmberMessage.PrincipalMessage.{
  AckedPrincipalInitialization,
  GetInputLayer,
  GetOutputLayer,
  ReportPrincipalPartialCompleted,
  ReportState
}
import Engine.Common.AmberMessage.WorkerMessage.{
  AckedWorkerInitialization,
  DataMessage,
  EndSending,
  UpdateInputLinking,
  UpdateOutputLinking
}
import Engine.Common.AmberTag.{LayerTag, LinkTag, OperatorTag, WorkerTag, WorkflowTag}
import Engine.Common.AmberTuple.Tuple
import Engine.Operators.SimpleCollection.{
  SimpleProcessOperatorMetadata,
  SimpleSourceOperatorMetadata,
  SimpleTupleProcessor,
  SimpleTupleProducer
}
import Engine.Common.{TableMetadata, TupleMetadata}
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import akka.pattern.ask

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

class CountPrincipalSpec
    extends TestKit(ActorSystem("CountPrincipalSpec"))
    with ImplicitSender
    with FlatSpecLike
    with BeforeAndAfterAll {

  val dataSet = Array(
    Tuple("Asia"),
    Tuple("Europe"),
    Tuple("some"),
    Tuple("important"),
    Tuple("keywords"),
    Tuple("Japan"),
    Tuple("cat"),
    Tuple("cat boomer"),
    Tuple("cat lover"),
    Tuple("asia")
  )

  val workflowTag = WorkflowTag("sample")
  var index = 0
  val opTag: () => OperatorTag = () => { index += 1; OperatorTag(workflowTag, index.toString) }
  val layerTag: () => LayerTag = () => { index += 1; LayerTag(opTag(), index.toString) }
  val workerTag: () => WorkerTag = () => { index += 1; WorkerTag(layerTag(), index) }
  val linkTag: () => LinkTag = () => { LinkTag(layerTag(), layerTag()) }
  override def beforeAll: Unit = {
    system.actorOf(Props[SingleNodeListener], "cluster-info")
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "An Principal with 5 PartialProcessor" should "aggregates the right result" in {
    implicit val timeout = Timeout(5.seconds)
    val metadata =
      new TableMetadata("table1", new TupleMetadata(Array[FieldType.Value](FieldType.String)))
    val sendActor = system.actorOf(Processor.props(new SimpleTupleProcessor(), workerTag()))
    ignoreMsg {
      case UpdateInputLinking(_, _) => true
    }

    val originLayer = layerTag()
    val senderLayer = layerTag()
    val countPartialLayer = layerTag()
    val countFinalLayer = layerTag()

    sendActor ? AckedWorkerInitialization
    sendActor ? UpdateInputLinking(testActor, originLayer)
    val parent = TestProbe()
    parent.ignoreMsg { case ReportPrincipalPartialCompleted(x, y) => true }
    val principal = parent.childActorOf(Principal.props(new CountMetadata(opTag(), 5)))
    principal ? AckedPrincipalInitialization(Array())
    parent.expectMsg(ReportState(PrincipalState.Ready))
    val input = Await.result(principal ? GetInputLayer, timeout.duration).asInstanceOf[ActorLayer]
    sendActor ? UpdateOutputLinking(
      new RoundRobinPolicy(1),
      LinkTag(senderLayer, countPartialLayer),
      input.layer.map(new DirectRoutee(_))
    )
    //input.layer.foreach(x => x ? UpdateInputLinking(sendActor,senderLayer))
    val output = Await.result(principal ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
    output.layer.foreach(x =>
      x ? UpdateOutputLinking(
        new OneToOnePolicy(10),
        LinkTag(countPartialLayer, countFinalLayer),
        Array(new DirectRoutee(testActor))
      )
    )
    sendActor ! DataMessage(0, dataSet)
    sendActor ! EndSending(1)
    parent.expectMsg(ReportState(PrincipalState.Running))
    expectMsg(DataMessage(0, Array(Tuple(10))))
    expectMsg(EndSending(1))
    parent.expectMsg(ReportState(PrincipalState.Completed))
  }

}
