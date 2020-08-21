package Engine.Operators.HashJoin

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
import Engine.Common.AmberTuple.Tuple
import Engine.Common.{TableMetadata, TupleMetadata}
import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._

class HashJoinProcessorSpec
    extends TestKit(ActorSystem("HashJoinProcessorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with BeforeAndAfterAll {

  val dataSet1 = Array(
    Tuple("Asia", 1),
    Tuple("Europe", 2),
    Tuple("some", 3),
    Tuple("important", 4),
    Tuple("keywords", 5),
    Tuple("Japan", 6),
    Tuple("cat", 1),
    Tuple("cat boomer", 2),
    Tuple("cat lover", 3),
    Tuple("asia", 4)
  )

  val dataSet2 = Array(
    Tuple("Japan", 4),
    Tuple("cat", 3),
    Tuple("cat boomer", 2),
    Tuple("cat lover", 1),
    Tuple("asia", 0)
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

  "A HashJoinProcessor" should "join 2 datasets by the first column" in {
    implicit val timeout = Timeout(5.seconds)
    ignoreMsg {
      case UpdateInputLinking(_, _)           => true
      case ReportWorkerPartialCompleted(_, _) => true
    }

    val probe1 = TestProbe()
    val probe2 = TestProbe()
    val metadata = new TableMetadata(
      "table1",
      new TupleMetadata(Array[FieldType.Value](FieldType.String, FieldType.Int))
    )
    val link1 = linkTag()
    val link2 = LinkTag(layerTag(), link1.to)
    val execActor = system.actorOf(
      Processor.props(new HashJoinTupleProcessor[String](link1.from, 0, 0), workerTag())
    )
    execActor ? AckedWorkerInitialization
    execActor ? UpdateInputLinking(probe1.ref, link1.from)
    execActor ? UpdateInputLinking(probe2.ref, link2.from)
    val output = new RoundRobinPolicy(50)
    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
    probe1.send(execActor, DataMessage(0, dataSet2))
    probe1.send(execActor, EndSending(1))
    probe2.send(execActor, DataMessage(0, dataSet1))
    probe2.send(execActor, EndSending(1))
    expectMsg(
      DataMessage(
        0,
        Array(
          Tuple("Japan", 6, "Japan", 4),
          Tuple("cat", 1, "cat", 3),
          Tuple("cat boomer", 2, "cat boomer", 2),
          Tuple("cat lover", 3, "cat lover", 1),
          Tuple("asia", 4, "asia", 0)
        )
      )
    )
    expectMsg(EndSending(1))
    Thread.sleep(1000)
    execActor ! PoisonPill
  }

  "A HashJoinProcessor" should "join 2 datasets by the second column" in {
    implicit val timeout = Timeout(5.seconds)
    ignoreMsg {
      case UpdateInputLinking(_, _)           => true
      case ReportWorkerPartialCompleted(_, _) => true
    }

    val probe1 = TestProbe()
    val probe2 = TestProbe()
    val metadata = new TableMetadata(
      "table1",
      new TupleMetadata(Array[FieldType.Value](FieldType.String, FieldType.Int))
    )
    val link1 = linkTag()
    val link2 = LinkTag(layerTag(), link1.to)
    val execActor = system.actorOf(
      Processor.props(new HashJoinTupleProcessor[Int](link1.from, 1, 1), workerTag())
    )
    execActor ? AckedWorkerInitialization
    execActor ? UpdateInputLinking(probe1.ref, link1.from)
    execActor ? UpdateInputLinking(probe2.ref, link2.from)
    val output = new RoundRobinPolicy(50)
    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
    probe1.send(execActor, DataMessage(0, dataSet2))
    probe1.send(execActor, EndSending(1))
    probe2.send(execActor, DataMessage(0, dataSet1))
    probe2.send(execActor, EndSending(1))
    var res = Set[Tuple]()
    receiveWhile(5.seconds, 2.seconds) {
      case DataMessage(_, payload) => res ++= Set(payload: _*)
      case msg                     =>
    }
    assert(
      res == Set(
        Tuple("important", 4, "Japan", 4),
        Tuple("asia", 4, "Japan", 4),
        Tuple("some", 3, "cat", 3),
        Tuple("cat lover", 3, "cat", 3),
        Tuple("Europe", 2, "cat boomer", 2),
        Tuple("cat boomer", 2, "cat boomer", 2),
        Tuple("Asia", 1, "cat lover", 1),
        Tuple("cat", 1, "cat lover", 1)
      )
    )

    Thread.sleep(1000)
    execActor ! PoisonPill
  }

}
