package Engine.Operators.Filter

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
import com.github.nscala_time.time.Imports.DateTime

class FilterPrincipalSpec
    extends TestKit(ActorSystem("FilterPrincipalSpec"))
    with ImplicitSender
    with FlatSpecLike
    with BeforeAndAfterAll {

  val dataSet = Array(
    Tuple(1234),
    Tuple(1),
    Tuple(3),
    Tuple(5),
    Tuple(7832),
    Tuple(0),
    Tuple(-1),
    Tuple(222),
    Tuple(4567),
    Tuple(0)
  )

  val dataSet2 = Array(
    Tuple("1990-10-10"),
    Tuple("2001-10-10"),
    Tuple("1992-10-10"),
    Tuple("1993-10-10"),
    Tuple("1994-10-10"),
    Tuple("1990-09-02"),
    Tuple("2020-01-01"),
    Tuple("1997-10-10"),
    Tuple("1992-10-10"),
    Tuple("1990-02-10")
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

  "An Principal with 5 workers" should "filter out num > 5" in {
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
    val receiver = TestProbe()
    parent.ignoreMsg { case ReportPrincipalPartialCompleted(x, y) => true }
    val principal = parent.childActorOf(
      Principal.props(new FilterMetadata[Int](opTag(), 5, 0, FilterType.Greater, 5))
    )
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
    assert(res == Set(Tuple(1234), Tuple(7832), Tuple(222), Tuple(4567)))
  }

  "An Principal with 5 workers" should "filter out date > 2000-01-01" in {
    implicit val timeout = Timeout(5.seconds)
    implicit def ord: Ordering[DateTime] = Ordering.by(_.getMillis)
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
    val receiver = TestProbe()
    parent.ignoreMsg { case ReportPrincipalPartialCompleted(x, y) => true }
    val principal = parent.childActorOf(
      Principal.props(
        new FilterMetadata[DateTime](
          opTag(),
          5,
          0,
          FilterType.Greater[DateTime],
          DateTime.parse("2000-01-01")
        )
      )
    )
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
        Array(new DirectRoutee(receiver.ref))
      )
    )
    sendActor ! DataMessage(0, dataSet2)
    sendActor ! EndSending(1)
    parent.expectMsg(ReportState(PrincipalState.Running))
    var res = Set[Tuple]()
    receiver.receiveWhile(5.seconds, 2.seconds) {
      case DataMessage(_, payload) => res ++= Set(payload: _*)
      case msg                     =>
    }
    parent.expectMsg(ReportState(PrincipalState.Completed))
    assert(res == Set(Tuple("2001-10-10"), Tuple("2020-01-01")))
  }

}
