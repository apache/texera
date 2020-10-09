package Engine.Operators.KeywordSearch

import Engine.Architecture.SendSemantics.DataTransferPolicy.{OneToOnePolicy, RoundRobinPolicy}
import Engine.Architecture.SendSemantics.Routees.DirectRoutee
import Engine.Architecture.Worker.Processor
import Engine.Common.AmberField.FieldType
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
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._

class KeywordSearchProcessorSpec
    extends TestKit(ActorSystem("KeywordSearchProcessorSpec"))
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

  override def beforeAll: Unit = {}

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "An KeywordSearchProcessor" should "filter string with 'cat' " in {
    implicit val timeout = Timeout(5.seconds)
    ignoreMsg {
      case UpdateInputLinking(_, _)           => true
      case ReportWorkerPartialCompleted(_, _) => true
    }
    val metadata =
      new TableMetadata("table1", new TupleMetadata(Array[FieldType.Value](FieldType.String)))
    val execActor =
      system.actorOf(Processor.props(new KeywordSearchTupleProcessor(0, "cat"), workerTag()))
    execActor ? AckedWorkerInitialization
    execActor ? UpdateInputLinking(testActor, null)
    val output = new RoundRobinPolicy(5)
    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
    execActor ! DataMessage(0, dataSet)
    execActor ! EndSending(1)
    expectMsg(DataMessage(0, Array(Tuple("cat"), Tuple("cat boomer"), Tuple("cat lover"))))
    expectMsg(EndSending(1))
    Thread.sleep(1000)
    execActor ! PoisonPill
  }

  "An KeywordSearchProcessor" should "filter string with 'asia' " in {
    implicit val timeout = Timeout(5.seconds)
    ignoreMsg {
      case UpdateInputLinking(_, _)           => true
      case ReportWorkerPartialCompleted(_, _) => true
    }
    val metadata =
      new TableMetadata("table1", new TupleMetadata(Array[FieldType.Value](FieldType.String)))
    val execActor =
      system.actorOf(Processor.props(new KeywordSearchTupleProcessor(0, "asia"), workerTag()))
    execActor ? AckedWorkerInitialization
    execActor ? UpdateInputLinking(testActor, null)
    val output = new RoundRobinPolicy(5)
    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
    execActor ! DataMessage(0, dataSet)
    execActor ! EndSending(1)
    expectMsg(DataMessage(0, Array(Tuple("asia"))))
    expectMsg(EndSending(1))
    Thread.sleep(1000)
    execActor ! PoisonPill
  }
}
