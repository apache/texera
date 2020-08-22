package Engine.Operators.Count

import Engine.Architecture.SendSemantics.DataTransferPolicy.{OneToOnePolicy, RoundRobinPolicy}
import Engine.Architecture.SendSemantics.Routees.DirectRoutee
import Engine.Architecture.Worker.Processor
import Engine.Common.AmberField.FieldType
import Engine.Common.AmberMessage.ControlMessage.Ack
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
import Engine.Operators.SimpleCollection.SimpleTupleProcessor
import Engine.Common.{TableMetadata, TupleMetadata}
import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._

class CountProcessorSpec
    extends TestKit(ActorSystem("CountProcessorSpec"))
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

  "An CountProcessor with 2 CountPartialProcessor" should "aggregates the right result" in {
    implicit val timeout = Timeout(5.seconds)
    val metadata =
      new TableMetadata("table1", new TupleMetadata(Array[FieldType.Value](FieldType.String)))
    val sendActor = system.actorOf(Processor.props(new SimpleTupleProcessor(), workerTag()))
    val countPartialActor1 =
      system.actorOf(Processor.props(new CountLocalTupleProcessor(), workerTag()))
    val countPartialActor2 =
      system.actorOf(Processor.props(new CountLocalTupleProcessor(), workerTag()))
    val countFinalActor =
      system.actorOf(Processor.props(new CountGlobalTupleProcessor(), workerTag()))

    ignoreMsg {
      case UpdateInputLinking(_, _)           => true
      case ReportWorkerPartialCompleted(_, _) => true
    }

    val originLayer = layerTag()
    val senderLayer = layerTag()
    val countPartialLayer = layerTag()
    val countFinalLayer = layerTag()
    sendActor ? AckedWorkerInitialization
    sendActor ? UpdateInputLinking(testActor, originLayer)
    val output = new RoundRobinPolicy(1)
    sendActor ? UpdateOutputLinking(
      output,
      LinkTag(senderLayer, countPartialLayer),
      Array(new DirectRoutee(countPartialActor1), new DirectRoutee(countPartialActor2))
    )

    countPartialActor1 ? AckedWorkerInitialization
    //countPartialActor1 ? UpdateInputLinking(sendActor,senderLayer)
    val output1 = new OneToOnePolicy(1)
    countPartialActor1 ? UpdateOutputLinking(
      output1,
      LinkTag(countPartialLayer, countFinalLayer),
      Array(new DirectRoutee(countFinalActor))
    )

    countPartialActor2 ? AckedWorkerInitialization
    //countPartialActor2 ? UpdateInputLinking(sendActor,senderLayer)
    val output2 = new OneToOnePolicy(1)
    countPartialActor2 ? UpdateOutputLinking(
      output2,
      LinkTag(countPartialLayer, countFinalLayer),
      Array(new DirectRoutee(countFinalActor))
    )

    countFinalActor ? AckedWorkerInitialization
    //countFinalActor ? UpdateInputLinking(countPartialActor1,countPartialLayer)
    //countFinalActor ? UpdateInputLinking(countPartialActor2,countPartialLayer)
    val output3 = new OneToOnePolicy(1)
    countFinalActor ? UpdateOutputLinking(output3, linkTag(), Array(new DirectRoutee(testActor)))
    sendActor ! DataMessage(0, dataSet)
    sendActor ! EndSending(1)
    expectMsg(DataMessage(0, Array(Tuple(10))))
    expectMsg(EndSending(1))
    Thread.sleep(1000)
    sendActor ! PoisonPill
    countPartialActor1 ! PoisonPill
    countPartialActor2 ! PoisonPill
    countFinalActor ! PoisonPill
  }

}
