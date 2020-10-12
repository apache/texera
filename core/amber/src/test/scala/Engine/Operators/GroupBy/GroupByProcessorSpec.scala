package Engine.Operators.GroupBy

import Engine.Architecture.Principal.PrincipalState
import Engine.Architecture.SendSemantics.DataTransferPolicy.{OneToOnePolicy, RoundRobinPolicy}
import Engine.Architecture.SendSemantics.Routees.DirectRoutee
import Engine.Architecture.Worker.Processor
import Engine.Common.AmberField.FieldType
import Engine.Common.AmberMessage.PrincipalMessage.ReportState
import Engine.Common.AmberMessage.WorkerMessage.{
  AckedWorkerInitialization,
  DataMessage,
  EndSending,
  ReportWorkerPartialCompleted,
  UpdateInputLinking,
  UpdateOutputLinking
}
import Engine.Common.AmberTag.{LayerTag, LinkTag, OperatorTag, WorkerTag, WorkflowTag}
import Engine.Common.tuple.Tuple
import Engine.Common.{TableMetadata, TupleMetadata}
import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._

class GroupByProcessorSpec
    extends TestKit(ActorSystem("GroupByProcessorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with BeforeAndAfterAll {

  val dataSet = Array(
    Tuple("Asia", "1"),
    Tuple("Europe", "2"),
    Tuple("Cat", "3"),
    Tuple("Europe", "4"),
    Tuple("Asia", "5"),
    Tuple("Japan", "6"),
    Tuple("Cat", "1"),
    Tuple("Cat", "2"),
    Tuple("Cat", "4"),
    Tuple("Asia", "3")
  )

  val workflowTag = WorkflowTag("sample")
  var index = 0
  val opTag: () => OperatorTag = () => { index += 1; OperatorTag(workflowTag, index.toString) }
  val layerTag: () => LayerTag = () => { index += 1; LayerTag(opTag(), index.toString) }
  val workerTag: () => WorkerTag = () => { index += 1; WorkerTag(layerTag(), index) }
  val linkTag: () => LinkTag = () => { LinkTag(layerTag(), layerTag()) }

  override def beforeAll: Unit = {}

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "An GroupByLocalTupleProcessor" should "do local aggregation" in {
    implicit val timeout = Timeout(5.seconds)
    ignoreMsg {
      case UpdateInputLinking(_, _)           => true
      case ReportWorkerPartialCompleted(_, _) => true
    }
    val receiver = TestProbe()
    val metadata =
      new TableMetadata("table1", new TupleMetadata(Array[FieldType.Value](FieldType.String)))
    val execActor = system.actorOf(
      Processor.props(
        new GroupByLocalTupleProcessor[String](0, 1, AggregationType.Average),
        workerTag()
      )
    )
    execActor ? AckedWorkerInitialization
    execActor ? UpdateInputLinking(testActor, null)
    val output = new RoundRobinPolicy(20)
    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(receiver.ref)))
    execActor ! DataMessage(0, dataSet)
    execActor ! EndSending(1)
    var res = Set[Tuple]()
    receiver.receiveWhile(5.seconds, 2.seconds) {
      case DataMessage(_, payload) => res ++= Set(payload: _*)
      case msg                     =>
    }
    assert(
      res == Set(Tuple("Asia", 3d), Tuple("Europe", 3d), Tuple("Japan", 6d), Tuple("Cat", 2.5d))
    )
    Thread.sleep(1000)
    execActor ! PoisonPill
  }

}
