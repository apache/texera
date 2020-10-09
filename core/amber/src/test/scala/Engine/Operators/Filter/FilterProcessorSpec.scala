package Engine.Operators.Filter

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
import Engine.Operators.Filter.FilterType.AmberDateTime
import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit}

import reflect.{ClassTag, classTag}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import akka.pattern.ask
import akka.util.Timeout

import scala.language.implicitConversions
import scala.concurrent.duration._
import com.github.nscala_time.time.Imports.DateTime

class FilterProcessorSpec
    extends TestKit(ActorSystem("FilterProcessorSpec"))
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
    Tuple("1991-10-10"),
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

  override def beforeAll: Unit = {}

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "An FilterTupleProcessor" should "filter number == 1 " in {
    implicit val timeout = Timeout(5.seconds)
    ignoreMsg {
      case UpdateInputLinking(_, _)           => true
      case ReportWorkerPartialCompleted(_, _) => true
    }
    val metadata =
      new TableMetadata("table1", new TupleMetadata(Array[FieldType.Value](FieldType.String)))
    val execActor = system.actorOf(
      Processor.props(new FilterTupleProcessor[Int](0, FilterType.Equal[Int], 1), workerTag())
    )
    execActor ? AckedWorkerInitialization
    execActor ? UpdateInputLinking(testActor, null)
    val output = new RoundRobinPolicy(5)
    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
    execActor ! DataMessage(0, dataSet)
    execActor ! EndSending(1)
    expectMsg(DataMessage(0, Array(Tuple(1))))
    expectMsg(EndSending(1))
    Thread.sleep(1000)
    execActor ! PoisonPill
  }

  "An FilterTupleProcessor" should "filter number < 0 " in {
    implicit val timeout = Timeout(5.seconds)
    ignoreMsg {
      case UpdateInputLinking(_, _)           => true
      case ReportWorkerPartialCompleted(_, _) => true
    }
    val metadata =
      new TableMetadata("table1", new TupleMetadata(Array[FieldType.Value](FieldType.String)))
    val execActor = system.actorOf(
      Processor.props(new FilterTupleProcessor[Int](0, FilterType.Less[Int], 0), workerTag())
    )
    execActor ? AckedWorkerInitialization
    execActor ? UpdateInputLinking(testActor, null)
    val output = new RoundRobinPolicy(5)
    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
    execActor ! DataMessage(0, dataSet)
    execActor ! EndSending(1)
    expectMsg(DataMessage(0, Array(Tuple(-1))))
    expectMsg(EndSending(1))
    Thread.sleep(1000)
    execActor ! PoisonPill
  }

  "An FilterTupleProcessor" should "filter date = 2020-01-01 " in {
    implicit val timeout = Timeout(5.seconds)
    implicit def ord: Ordering[DateTime] = Ordering.by(_.getMillis)
    ignoreMsg {
      case UpdateInputLinking(_, _)           => true
      case ReportWorkerPartialCompleted(_, _) => true
    }

    val metadata =
      new TableMetadata("table1", new TupleMetadata(Array[FieldType.Value](FieldType.String)))
    val execActor = system.actorOf(
      Processor.props(
        new FilterSpecializedTupleProcessor(
          0,
          FilterType.Equal[DateTime],
          DateTime.parse("2020-01-01")
        ),
        workerTag()
      )
    )
    execActor ? AckedWorkerInitialization
    execActor ? UpdateInputLinking(testActor, null)
    val output = new RoundRobinPolicy(5)
    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
    execActor ! DataMessage(0, dataSet2)
    execActor ! EndSending(1)
    expectMsg(DataMessage(0, Array(Tuple("2020-01-01"))))
    expectMsg(EndSending(1))
    Thread.sleep(1000)
    execActor ! PoisonPill
  }

}
