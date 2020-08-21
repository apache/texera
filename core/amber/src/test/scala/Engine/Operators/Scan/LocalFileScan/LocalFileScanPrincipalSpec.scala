package Engine.Operators.Scan.LocalFileScan

import java.io.{BufferedWriter, File, FileWriter}

import Clustering.SingleNodeListener
import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Architecture.Principal.{Principal, PrincipalState}
import Engine.Architecture.SendSemantics.DataTransferPolicy.OneToOnePolicy
import Engine.Architecture.SendSemantics.Routees.DirectRoutee
import Engine.Common.AmberField.FieldType
import Engine.Common.AmberMessage.ControlMessage.Start
import Engine.Common.AmberMessage.PrincipalMessage.{GetOutputLayer, AckedPrincipalInitialization, ReportState}
import Engine.Common.AmberMessage.WorkerMessage.{DataMessage, UpdateOutputLinking}
import Engine.Common.AmberTag.{LayerTag, LinkTag, OperatorTag, WorkerTag, WorkflowTag}
import Engine.Common.AmberTuple.Tuple
import Engine.Common.{TableMetadata, TupleMetadata}
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import akka.pattern.ask
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._



class LocalFileScanPrincipalSpec
  extends TestKit(ActorSystem("LocalFileScanPrincipalSpec"))
  with ImplicitSender
  with FlatSpecLike
  with BeforeAndAfterAll {

  val smallFileContent = "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14"
  val largeFileContent = "1;2;c\n1;4;f;\n5;2;b;\n6;999999;iiii\n8898;6647;kk;\n987;222;112;\n892;123123;ooppookkkkeeedddddd;"

  val smallFilePath = "principal_smallfile.txt"
  val largeFilePath = "principal_largefile.txt"

  val workflowTag = WorkflowTag("sample")
  var index=0
  val opTag: () => OperatorTag = ()=>{index+=1; OperatorTag(workflowTag,index.toString)}
  val layerTag: () => LayerTag = ()=>{index+=1; LayerTag(opTag(),index.toString)}
  val workerTag: () => WorkerTag = ()=>{index+=1; WorkerTag(layerTag(),index)}
  val linkTag: () => LinkTag = ()=>{LinkTag(layerTag(),layerTag())}

  override def beforeAll:Unit = {
    val smallFile = new File(smallFilePath)
    val bw1 = new BufferedWriter(new FileWriter(smallFile))
    bw1.write(smallFileContent)
    bw1.close()
    val largeFile = new File(largeFilePath)
    val bw2 = new BufferedWriter(new FileWriter(largeFile))
    bw2.write(largeFileContent)
    bw2.close()
    system.actorOf(Props[SingleNodeListener],"cluster-info")
  }

  override def afterAll: Unit = {
    new File(smallFilePath).delete()
    new File(largeFilePath).delete()
    TestKit.shutdownActorSystem(system)
  }


  "A LocalFileScanPrincipal" should "read small file and generate tuples" in {
    implicit val timeout = Timeout(5.seconds)
    val metadata = new TableMetadata("table1",new TupleMetadata(Array[FieldType.Value](FieldType.Int)))
    val parent = TestProbe()
    val principal = parent.childActorOf(Principal.props(new LocalFileScanMetadata(opTag(),5,smallFilePath,';',null,metadata)))
    principal ! AckedPrincipalInitialization(Array())
    parent.expectMsg(ReportState(PrincipalState.Ready))
    val output = Await.result(principal ? GetOutputLayer,timeout.duration).asInstanceOf[ActorLayer]
    output.layer.foreach(x => x ! UpdateOutputLinking(new OneToOnePolicy(1),linkTag(),Array(new DirectRoutee(testActor))))
    principal ! Start
    parent.expectMsg(ReportState(PrincipalState.Running))
    var res = Set[Tuple]()
    receiveWhile(5.seconds, 2.seconds){
      case DataMessage(seq,payload) => res ++= Set(payload:_*)
      case msg =>
    }
    parent.expectMsg(ReportState(PrincipalState.Completed))
    assert(res == Set((1 to 14).map(x => Tuple(x)):_*))
  }


  "A LocalFileScanPrincipal" should "read large file with some indexes skipped and generate tuples" in {
    implicit val timeout = Timeout(5.seconds)
    val metadata = new TableMetadata("table1",new TupleMetadata(Array[FieldType.Value](FieldType.String)))
    val parent = TestProbe()
    val principal = parent.childActorOf(Principal.props(new LocalFileScanMetadata(opTag(),5,largeFilePath,';',Array(2),metadata)))
    principal ! AckedPrincipalInitialization(Array())
    parent.expectMsg(ReportState(PrincipalState.Ready))
    val output = Await.result(principal ? GetOutputLayer,timeout.duration).asInstanceOf[ActorLayer]
    output.layer.foreach(x => x ! UpdateOutputLinking(new OneToOnePolicy(1),linkTag(),Array(new DirectRoutee(testActor))))
    principal ! Start
    parent.expectMsg(ReportState(PrincipalState.Running))
    var res = Set[Tuple]()
    receiveWhile(5.seconds, 2.seconds){
      case DataMessage(seq,payload) => res ++= Set(payload:_*)
      case msg =>
    }
    parent.expectMsg(ReportState(PrincipalState.Completed))
    assert(res == Set(Tuple("c"),Tuple("f"),Tuple("b"),Tuple("iiii"),Tuple("kk"),Tuple("112"),Tuple("ooppookkkkeeedddddd")))
  }

}
