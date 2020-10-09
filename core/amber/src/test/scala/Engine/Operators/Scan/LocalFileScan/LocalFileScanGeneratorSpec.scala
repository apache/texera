package Engine.Operators.Scan.LocalFileScan

import java.io.{BufferedWriter, File, FileWriter}

import Clustering.SingleNodeListener
import Engine.Architecture.SendSemantics.DataTransferPolicy.OneToOnePolicy
import Engine.Architecture.SendSemantics.Routees.DirectRoutee
import Engine.Architecture.Worker.{Generator, WorkerState}
import Engine.Common.AmberField.FieldType
import Engine.Common.AmberMessage.ControlMessage.Start
import Engine.Common.AmberMessage.WorkerMessage.{
  DataMessage,
  EndSending,
  UpdateInputLinking,
  UpdateOutputLinking,
  AckedWorkerInitialization
}
import Engine.Common.AmberTag.{LayerTag, LinkTag, OperatorTag, WorkerTag, WorkflowTag}
import Engine.Common.tuple.Tuple
import Engine.Common.{TableMetadata, TupleMetadata}
import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

class LocalFileScanGeneratorSpec
    extends TestKit(ActorSystem("LocalFileScanGeneratorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with BeforeAndAfterAll {

  val smallFileContent = "1\n2\n3\n4\n5\n6\n7\n"
  val largeFileContent = "1;2;c\n1;4;f;\n5;2;b;\n6;999999;iiii\n8898;6647;kk;"

  val smallFilePath = "smallfile.txt"
  val largeFilePath = "largefile.txt"

  val workflowTag = WorkflowTag("sample")
  var index = 0
  val opTag: () => OperatorTag = () => { index += 1; OperatorTag(workflowTag, index.toString) }
  val layerTag: () => LayerTag = () => { index += 1; LayerTag(opTag(), index.toString) }
  val workerTag: () => WorkerTag = () => { index += 1; WorkerTag(layerTag(), index) }
  val linkTag: () => LinkTag = () => { LinkTag(layerTag(), layerTag()) }

  override def beforeAll: Unit = {
    val smallFile = new File(smallFilePath)
    val bw1 = new BufferedWriter(new FileWriter(smallFile))
    bw1.write(smallFileContent)
    bw1.close()
    val largeFile = new File(largeFilePath)
    val bw2 = new BufferedWriter(new FileWriter(largeFile))
    bw2.write(largeFileContent)
    bw2.close()
  }

  override def afterAll: Unit = {
    new File(smallFilePath).delete()
    new File(largeFilePath).delete()
    TestKit.shutdownActorSystem(system)
  }

  "A LocalFileScanGenerator" should "read small file and generate tuples in order" in {
    implicit val timeout = Timeout(5.seconds)
    ignoreMsg {
      case UpdateInputLinking(_, _) => true
    }
    val metadata =
      new TableMetadata("table1", new TupleMetadata(Array[FieldType.Value](FieldType.Int)))
    val totalBytes = new File(smallFilePath).length()
    val execActor = system.actorOf(
      Generator.props(
        new LocalFileScanTupleProducer(smallFilePath, 0, totalBytes, ';', null, metadata),
        workerTag()
      )
    )
    execActor ? AckedWorkerInitialization
    val output = new OneToOnePolicy(1)
    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
    execActor ? Start
    for (i <- 0 until 7) {
      expectMsg(DataMessage(i, Array(Tuple(i + 1))))
    }
    expectMsg(EndSending(7))
    Thread.sleep(1000)
    execActor ! PoisonPill
  }

  "A LocalFileScanGenerator" should "read large file and generate tuples in order" in {
    implicit val timeout = Timeout(5.seconds)
    ignoreMsg {
      case UpdateInputLinking(_, _) => true
    }
    val metadata = new TableMetadata(
      "table1",
      new TupleMetadata(Array[FieldType.Value](FieldType.Int, FieldType.Int, FieldType.String))
    )
    val totalBytes = new File(largeFilePath).length()
    val execActor = system.actorOf(
      Generator.props(
        new LocalFileScanTupleProducer(largeFilePath, 0, totalBytes, ';', null, metadata),
        workerTag()
      )
    )
    execActor ? AckedWorkerInitialization
    val output = new OneToOnePolicy(1)
    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
    execActor ? Start
    expectMsg(DataMessage(0, Array(Tuple(1, 2, "c"))))
    expectMsg(DataMessage(1, Array(Tuple(1, 4, "f"))))
    expectMsg(DataMessage(2, Array(Tuple(5, 2, "b"))))
    expectMsg(DataMessage(3, Array(Tuple(6, 999999, "iiii"))))
    expectMsg(DataMessage(4, Array(Tuple(8898, 6647, "kk"))))
    expectMsg(EndSending(5))
    Thread.sleep(1000)
    execActor ! PoisonPill
  }

  "A LocalFileScanGenerator" should "read large file and generate tuples with skipped field in order" in {
    implicit val timeout = Timeout(5.seconds)
    ignoreMsg {
      case UpdateInputLinking(_, _) => true
    }
    val metadata =
      new TableMetadata("table1", new TupleMetadata(Array[FieldType.Value](FieldType.String)))
    val totalBytes = new File(largeFilePath).length()
    val execActor = system.actorOf(
      Generator.props(
        new LocalFileScanTupleProducer(largeFilePath, 0, totalBytes, ';', Array(2), metadata),
        workerTag()
      )
    )
    execActor ? AckedWorkerInitialization
    val output = new OneToOnePolicy(1)
    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
    execActor ? Start
    expectMsg(DataMessage(0, Array(Tuple("c"))))
    expectMsg(DataMessage(1, Array(Tuple("f"))))
    expectMsg(DataMessage(2, Array(Tuple("b"))))
    expectMsg(DataMessage(3, Array(Tuple("iiii"))))
    expectMsg(DataMessage(4, Array(Tuple("kk"))))
    expectMsg(EndSending(5))
    Thread.sleep(1000)
    execActor ! PoisonPill
  }

}
