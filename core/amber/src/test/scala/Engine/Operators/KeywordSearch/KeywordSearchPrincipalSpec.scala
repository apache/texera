package Engine.Operators.KeywordSearch

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
import Engine.Common.tuple.Tuple
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

class KeywordSearchPrincipalSpec
    extends TestKit(ActorSystem("KeywordSearchPrincipalSpec"))
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

  "A KeywordSearchPrincipal with 5 workers" should "filter string with 'cat' " in {
    implicit val timeout = Timeout(5.seconds)
    val sendActor = system.actorOf(Processor.props(new SimpleTupleProcessor(), workerTag()))

    val originLayer = layerTag()
    val senderLayer = layerTag()
    val keywordSearchLayer = layerTag()

    sendActor ! AckedWorkerInitialization
    sendActor ! UpdateInputLinking(testActor, originLayer)
    val parent = TestProbe()
    parent.ignoreMsg { case ReportPrincipalPartialCompleted(x, y) => true }
    val receiver = TestProbe()
    val principal =
      parent.childActorOf(Principal.props(new KeywordSearchMetadata(opTag(), 5, 0, "cat")))
    principal ! AckedPrincipalInitialization(Array())
    parent.expectMsg(ReportState(PrincipalState.Ready))
    val input = Await.result(principal ? GetInputLayer, timeout.duration).asInstanceOf[ActorLayer]
    sendActor ! UpdateOutputLinking(
      new RoundRobinPolicy(1),
      LinkTag(senderLayer, keywordSearchLayer),
      input.layer.map(new DirectRoutee(_))
    )
    val output = Await.result(principal ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
    output.layer.foreach(x =>
      x ! UpdateOutputLinking(
        new OneToOnePolicy(10),
        linkTag(),
        Array(new DirectRoutee(receiver.ref))
      )
    )
    sendActor ! DataMessage(0, dataSet)
    sendActor ! EndSending(1)
    parent.expectMsg(ReportState(PrincipalState.Running))
    var res = Set[Tuple]()
    receiver.receiveWhile(5.seconds, 2.seconds) {
      case DataMessage(_, payload) => res ++= Set(payload: _*)
      case msg                     =>
    }
    parent.expectMsg(ReportState(PrincipalState.Completed))
    assert(res == Set(Tuple("cat"), Tuple("cat boomer"), Tuple("cat lover")))
  }

}
