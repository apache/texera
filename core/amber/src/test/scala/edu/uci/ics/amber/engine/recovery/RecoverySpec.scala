package edu.uci.ics.amber.engine.recovery

import java.io.File
import java.nio.file.{Files, Paths}

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.QueryWorkerStatistics
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.DataInputPort.WorkflowDataMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkAck,
  NetworkMessage,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.OneToOnePolicy
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddOutputPolicyHandler.AddOutputPolicy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, ISourceOperatorExecutor, InputExhausted}
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlPayload,
  DataFrame,
  EndOfUpstream,
  InputLinking,
  WorkflowMessage
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity.WorkerActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity
}
import edu.uci.ics.amber.engine.recovery.DataLogManager.DataLogElement
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalamock.scalatest.MockFactory

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration._

class RecoverySpec
    extends TestKit(ActorSystem("RecoverySpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  def deleteFolderSafely(path: String): Unit = {
    val folder = new File(path)
    if (folder.exists()) {
      FileUtils.cleanDirectory(folder)
      FileUtils.deleteDirectory(folder)
    }
  }

  override def beforeAll: Unit = {
    deleteFolderSafely("./logs")
    system.actorOf(Props[SingleNodeListener], "cluster-info")
  }
  override def afterAll: Unit = {
    deleteFolderSafely("./logs")
    TestKit.shutdownActorSystem(system)
  }
  val receiverID = WorkerActorVirtualIdentity("receiver")
  val fakeLink: LinkIdentity =
    LinkIdentity(
      LayerIdentity("testRecovery", "mockOp", "src"),
      LayerIdentity("testRecovery", "mockOp", "dst")
    )

  class SourceOperatorForRecoveryTest(outputLimit: Int = 15, generateInterval: Int = 100)
      extends ISourceOperatorExecutor {
    override def open(): Unit = {}
    override def close(): Unit = {}
    override def produce(): Iterator[ITuple] = {
      (for (i <- (1 to outputLimit).view) yield {
        Thread.sleep(generateInterval); println(s"generating tuple $i"); ITuple(i)
      }).toIterator
    }
  }

  class DummyOperatorForRecoveryTest() extends IOperatorExecutor {
    override def open(): Unit = {}
    override def close(): Unit = {}
    override def processTuple(
        tuple: Either[ITuple, InputExhausted],
        input: LinkIdentity
    ): Iterator[ITuple] = {
      tuple match {
        case Left(value) =>
          println(s"received tuple $value")
          Iterator(value)
        case Right(value) =>
          println(s"received tuple $value")
          Iterator.empty
      }
    }
  }

  def forAllNetworkMessages(probe: TestProbe, action: (WorkflowMessage) => Unit): Unit = {
    if (probe != null) {
      probe.receiveWhile(idle = 3.seconds) {
        case NetworkMessage(id, content) =>
          probe.sender() ! NetworkAck(id)
          action(content)
        case other => //skip
      }
    }
  }

  def initWorker(
      id: ActorVirtualIdentity,
      op: IOperatorExecutor,
      controller: TestProbe,
      actorMappingToRegister: Seq[(ActorVirtualIdentity, ActorRef)],
      controlLogStorage: LogStorage[WorkflowControlMessage],
      dataLogStorage: LogStorage[DataLogElement],
      dpLogStorage: LogStorage[Long]
  ): ActorRef = {
    val worker = TestActorRef(
      new WorkflowWorker(id, op, controller.ref, controlLogStorage, dataLogStorage, dpLogStorage) {
        networkCommunicationActor ! RegisterActorRef(
          ActorVirtualIdentity.Controller,
          controller.ref
        )
        actorMappingToRegister.foreach {
          case (id, ref) =>
            networkCommunicationActor ! RegisterActorRef(id, ref)
        }
      }
    )
    worker
  }

  def sendMessagesAsync(worker: ActorRef, controls: Seq[ControlCommand[_]]): Future[Boolean] = {
    Future {
      sendMessages(worker, controls)
      true
    }(ExecutionContext.global)
  }

  def sendMessages(worker: ActorRef, controls: Seq[ControlCommand[_]]): Unit = {
    val messages = controls.indices.map(i =>
      WorkflowControlMessage(
        ActorVirtualIdentity.Controller,
        i,
        ControlInvocation(i, controls(i))
      )
    )
    messages.foreach { x =>
      worker ! NetworkMessage(0, x)
      Thread.sleep(400)
    }
  }

  def waitResponsesAndKillWorker(
      worker: ActorRef,
      controller: TestProbe,
      receiver: TestProbe
  ): mutable.Queue[Any] = {
    val receivedMessages = mutable.Queue[Any]()
    forAllNetworkMessages(controller, x => receivedMessages.enqueue(x))
    forAllNetworkMessages(receiver, x => receivedMessages.enqueue(x))
    worker ! PoisonPill
    println("received messages: \n" + receivedMessages.mkString("\n"))
    receivedMessages
  }

  def testRecovery(
      worker: ActorRef,
      controller: TestProbe,
      receiver: TestProbe,
      receivedMessages: mutable.Queue[Any]
  ): Unit = {
    Thread.sleep(15000)
    forAllNetworkMessages(controller, x => assert(receivedMessages.dequeue() == x))
    forAllNetworkMessages(receiver, x => assert(receivedMessages.dequeue() == x))
  }

  def smallWorkerChain(
      sender1: ActorVirtualIdentity,
      sender2: ActorVirtualIdentity,
      sender1ControlLog: LogStorage[WorkflowControlMessage],
      sender2ControlLog: LogStorage[WorkflowControlMessage],
      sender1DataLog: LogStorage[DataLogElement],
      sender2DataLog: LogStorage[DataLogElement],
      sender1DPLog: LogStorage[Long],
      sender2DPLog: LogStorage[Long]
  ): (
      ISourceOperatorExecutor,
      IOperatorExecutor,
      ActorRef,
      ActorRef,
      TestProbe,
      TestProbe,
      TestProbe
  ) = {
    val source = new SourceOperatorForRecoveryTest()
    val dummy = new DummyOperatorForRecoveryTest()
    val controller1 = TestProbe()
    val controller2 = TestProbe()
    val receiver = TestProbe()
    val controlsForSource = Seq(
      QueryStatistics(),
      AddOutputPolicy(new OneToOnePolicy(fakeLink, 1, Array(sender2))),
      StartWorker(),
      QueryStatistics(),
      QueryStatistics(),
      QueryStatistics()
    )
    val controlsForDummy = Seq(
      AddOutputPolicy(new OneToOnePolicy(fakeLink, 1, Array(receiverID))),
      QueryStatistics(),
      QueryStatistics(),
      QueryStatistics(),
      QueryStatistics()
    )
    val dummyWorker = initWorker(
      sender2,
      dummy,
      controller2,
      Seq((receiverID, receiver.ref)),
      sender2ControlLog,
      sender2DataLog,
      sender2DPLog
    )
    val sourceWorker = initWorker(
      sender1,
      source,
      controller1,
      Seq((sender2, dummyWorker)),
      sender1ControlLog,
      sender1DataLog,
      sender1DPLog
    )
    val f1 = sendMessagesAsync(sourceWorker, controlsForSource)
    val f2 = sendMessagesAsync(dummyWorker, controlsForDummy)
    Await.result(f1, 20.seconds)
    Await.result(f2, 20.seconds)
    (source, dummy, sourceWorker, dummyWorker, controller1, controller2, receiver)
  }
//  The following test will randomly fail in github action, the reason is still unclear.

  "worker" should "write logs during normal processing" in {
    val id = WorkerActorVirtualIdentity("testRecovery1")
    val sender1 = WorkerActorVirtualIdentity("sender1")
    val sender2 = WorkerActorVirtualIdentity("sender2")
    val sender3 = WorkerActorVirtualIdentity("sender3")
    val messages = Seq(
      WorkflowDataMessage(
        sender1,
        0,
        InputLinking(fakeLink)
      ),
      WorkflowDataMessage(
        sender2,
        0,
        InputLinking(fakeLink)
      ),
      WorkflowDataMessage(
        sender3,
        0,
        InputLinking(fakeLink)
      ),
      WorkflowDataMessage(sender1, 1, DataFrame(Array.empty)),
      WorkflowDataMessage(sender2, 1, DataFrame(Array.empty)),
      WorkflowDataMessage(sender2, 2, DataFrame(Array.empty)),
      WorkflowControlMessage(sender2, 0, ControlInvocation(-1, QueryStatistics())),
      WorkflowDataMessage(sender3, 1, DataFrame(Array.empty)),
      WorkflowControlMessage(sender3, 0, ControlInvocation(-1, QueryStatistics())),
      WorkflowDataMessage(sender1, 2, DataFrame(Array.empty))
    )
    val op = new SourceOperatorForRecoveryTest()
    val controlLogStorage: LogStorage[WorkflowControlMessage] =
      new InMemoryLogStorage(id.toString + "-control")
    val dataLogStorage: LogStorage[DataLogElement] = new InMemoryLogStorage(id.toString + "-data")
    val dpLogStorage: LogStorage[Long] = new InMemoryLogStorage(id.toString + "-dp")
    val worker = system.actorOf(
      WorkflowWorker.props(id, op, TestProbe().ref, controlLogStorage, dataLogStorage, dpLogStorage)
    )
    messages.foreach { x =>
      worker ! NetworkMessage(0, x)
    }
    Thread.sleep(10000)
    assert(InMemoryLogStorage.getLogOf(id.toString + "-control").size == 2)
    assert(InMemoryLogStorage.getLogOf(id.toString + "-data").size == 11)
    assert(InMemoryLogStorage.getLogOf(id.toString + "-dp").size == 2)
    dataLogStorage.clear()
    dpLogStorage.clear()
    controlLogStorage.clear()
  }

  "source worker" should "recover with the log after restarting" in {
    val id = WorkerActorVirtualIdentity("testRecovery2")
    val sender1 = WorkerActorVirtualIdentity("sender4")
    val op = new SourceOperatorForRecoveryTest()
    val controller = TestProbe()
    val receiver = TestProbe()
    val controls = Seq(
      AddOutputPolicy(new OneToOnePolicy(fakeLink, 1, Array(receiverID))),
      StartWorker(),
      QueryStatistics(),
      QueryStatistics(),
      QueryStatistics()
    )
    val workerControlLog = new LocalDiskLogStorage[WorkflowControlMessage](id + "-control")
    val workerDataLog = new LocalDiskLogStorage[DataLogElement](id + "-data")
    val workerDPLog = new LocalDiskLogStorage[Long](id + "-dp")
    val worker = initWorker(
      id,
      op,
      controller,
      Seq((receiverID, receiver.ref)),
      workerControlLog,
      workerDataLog,
      workerDPLog
    )
    sendMessages(worker, controls)
    val received = waitResponsesAndKillWorker(worker, controller, receiver)
    val recovered = initWorker(
      id,
      op,
      controller,
      Seq((receiverID, receiver.ref)),
      workerControlLog,
      workerDataLog,
      workerDPLog
    )
    testRecovery(recovered, controller, receiver, received)
    workerControlLog.clear()
    workerDataLog.clear()
    workerDPLog.clear()
  }

  "multiple workers" should "recover with their logs after restarting" in {
    val sourceID = WorkerActorVirtualIdentity("source1")
    val dummyID = WorkerActorVirtualIdentity("dummy1")
    val sourceControlLogStorage: LogStorage[WorkflowControlMessage] =
      new LocalDiskLogStorage(sourceID.toString + "-control")
    val sourceDataLogStorage: LogStorage[DataLogElement] =
      new LocalDiskLogStorage(sourceID.toString + "-data")
    val sourceDPLogStorage: LogStorage[Long] = new LocalDiskLogStorage(sourceID.toString + "-dp")
    val dummyControlLogStorage: LogStorage[WorkflowControlMessage] =
      new LocalDiskLogStorage(dummyID.toString + "-control")
    val dummyDataLogStorage: LogStorage[DataLogElement] =
      new LocalDiskLogStorage(dummyID.toString + "-data")
    val dummyDPLogStorage: LogStorage[Long] = new LocalDiskLogStorage(dummyID.toString + "-dp")
    val (source, dummy, sourceWorker, dummyWorker, controller1, controller2, receiver) =
      smallWorkerChain(
        sourceID,
        dummyID,
        sourceControlLogStorage,
        dummyControlLogStorage,
        sourceDataLogStorage,
        dummyDataLogStorage,
        sourceDPLogStorage,
        dummyDPLogStorage
      )
    val receivedMessageForSource =
      waitResponsesAndKillWorker(sourceWorker, controller1, null)
    val receivedMessageForDummy =
      waitResponsesAndKillWorker(dummyWorker, controller2, receiver)
    val recoveredDummy = initWorker(
      dummyID,
      dummy,
      controller2,
      Seq((receiverID, receiver.ref)),
      dummyControlLogStorage,
      dummyDataLogStorage,
      dummyDPLogStorage
    )
    val recoveredSource = initWorker(
      sourceID,
      source,
      controller1,
      Seq((dummyID, recoveredDummy)),
      sourceControlLogStorage,
      sourceDataLogStorage,
      sourceDPLogStorage
    )
    testRecovery(recoveredSource, controller1, null, receivedMessageForSource)
    testRecovery(recoveredDummy, controller2, receiver, receivedMessageForDummy)
    sourceControlLogStorage.clear()
    dummyControlLogStorage.clear()
    sourceDataLogStorage.clear()
    dummyDataLogStorage.clear()
    sourceDPLogStorage.clear()
    dummyDPLogStorage.clear()
  }

  "one worker" should "recover correctly while the other worker are still alive" in {
    val sourceID = WorkerActorVirtualIdentity("source2")
    val dummyID = WorkerActorVirtualIdentity("dummy2")
    val sourceControlLogStorage: LogStorage[WorkflowControlMessage] =
      new LocalDiskLogStorage(sourceID.toString + "-control")
    val sourceDataLogStorage: LogStorage[DataLogElement] =
      new LocalDiskLogStorage(sourceID.toString + "-data")
    val sourceDPLogStorage: LogStorage[Long] = new LocalDiskLogStorage(sourceID.toString + "-dp")
    val dummyControlLogStorage: LogStorage[WorkflowControlMessage] =
      new LocalDiskLogStorage(dummyID.toString + "-control")
    val dummyDataLogStorage: LogStorage[DataLogElement] =
      new LocalDiskLogStorage(dummyID.toString + "-data")
    val dummyDPLogStorage: LogStorage[Long] = new LocalDiskLogStorage(dummyID.toString + "-dp")
    val (source, dummy, sourceWorker, dummyWorker, controller1, controller2, receiver) =
      smallWorkerChain(
        sourceID,
        dummyID,
        sourceControlLogStorage,
        dummyControlLogStorage,
        sourceDataLogStorage,
        dummyDataLogStorage,
        sourceDPLogStorage,
        dummyDPLogStorage
      )
    val receivedMessageForSource =
      waitResponsesAndKillWorker(sourceWorker, controller1, null)
    val recoveredSource = initWorker(
      sourceID,
      source,
      controller1,
      Seq((dummyID, dummyWorker)),
      sourceControlLogStorage,
      sourceDataLogStorage,
      sourceDPLogStorage
    )
    testRecovery(recoveredSource, controller1, null, receivedMessageForSource)
    val expectedData =
      (Seq(WorkflowDataMessage(dummyID, 0, InputLinking(fakeLink))) ++ (1 until 16).map(x =>
        WorkflowDataMessage(dummyID, x, DataFrame(Array(ITuple(x))))
      ) ++ Seq(WorkflowDataMessage(dummyID, 16, EndOfUpstream()))).to[mutable.Queue]
    forAllNetworkMessages(receiver, w => assert(w == expectedData.dequeue()))
    val receivedControl = mutable.Queue[WorkflowMessage]()
    forAllNetworkMessages(controller2, w => receivedControl.enqueue(w))
    assert(receivedControl.size == 8)
    sourceControlLogStorage.clear()
    dummyControlLogStorage.clear()
    sourceDataLogStorage.clear()
    dummyDataLogStorage.clear()
    sourceDPLogStorage.clear()
    dummyDPLogStorage.clear()
  }

}
