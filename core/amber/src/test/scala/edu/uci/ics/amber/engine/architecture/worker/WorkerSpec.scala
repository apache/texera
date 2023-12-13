package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkMessage
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{OpExecConfig, OpExecInitInfo}
import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.WorkflowWorkerConfig
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddPartitioningHandler.AddPartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelID, DataFrame, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, InputExhausted}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.util.Random

class WorkerSpec
    extends TestKit(ActorSystem("WorkerSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with MockFactory {

  override def beforeAll: Unit = {
    system.actorOf(Props[SingleNodeListener], "cluster-info")
  }
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }
  private val identifier1 = ActorVirtualIdentity("worker-1")
  private val identifier2 = ActorVirtualIdentity("worker-2")

  private val mockOpExecutor = new IOperatorExecutor {
    override def open(): Unit = println("opened!")

    override def close(): Unit = println("closed!")

    override def processTuple(
        tuple: Either[ITuple, InputExhausted],
        input: Int,
        pauseManager: PauseManager,
        asyncRPCClient: AsyncRPCClient
    ): Iterator[(ITuple, Option[Int])] = {
      if (tuple.isLeft) {
        Iterator((tuple.left.get, None))
      } else {
        Iterator.empty
      }
    }
  }
  private val operatorIdentity = OperatorIdentity("testOperator")
  private val layerId1 =
    LayerIdentity(operatorIdentity.id, "1st-layer")
  private val layerId2 =
    LayerIdentity(operatorIdentity.id, "2nd-layer")
  private val mockLink = LinkIdentity(layerId1, 0, layerId2, 0)
  private val opExecConfig = OpExecConfig
    .oneToOneLayer(0, operatorIdentity, OpExecInitInfo(_ => mockOpExecutor))
    .copy(inputToOrdinalMapping = Map(mockLink -> 0), outputToOrdinalMapping = Map(mockLink -> 0))
  private val workerIndex = 0
  private val mockPolicy = OneToOnePartitioning(10, Array(identifier2))
  private val mockHandler = mock[WorkflowFIFOMessage => Unit]
  private val mockOutputManager = mock[OutputManager]

  def sendControlToWorker(
      worker: ActorRef,
      controls: Array[ControlInvocation],
      beginSeqNum: Long = 0
  ): Unit = {
    var seq = beginSeqNum
    controls.foreach { ctrl =>
      worker ! NetworkMessage(
        seq,
        WorkflowFIFOMessage(ChannelID(CONTROLLER, identifier1, isControl = true), seq, ctrl)
      )
      seq += 1
    }
  }

  def mkWorker: ActorRef = {
    TestActorRef(
      new WorkflowWorker(
        identifier1,
        workerIndex,
        opExecConfig,
        WorkflowWorkerConfig(logStorageType = "none", replayTo = None)
      ) {
        this.dp = new DataProcessor(identifier1, mockHandler) {
          override val outputManager: OutputManager = mockOutputManager
        }
        this.dp.initOperator(0, opExecConfig, Iterator.empty)
        this.dp.initTimerService(timerService)
        override val dpThread: DPThread =
          new DPThread(
            actorId,
            dp,
            logManager,
            inputQueue
          )
      }
    )
  }

  "Worker" should "process AddPartitioning message correctly" in {
    val worker = mkWorker
    (mockOutputManager.addPartitionerWithPartitioning _).expects(mockLink, mockPolicy).once()
    (mockHandler.apply _).expects(*).once()
    val invocation = ControlInvocation(0, AddPartitioning(mockLink, mockPolicy))
    sendControlToWorker(worker, Array(invocation))

    //wait test to finish
    Thread.sleep(3000)
  }

  "Worker" should "process data messages correctly" in {
    val worker = mkWorker
    (mockOutputManager.addPartitionerWithPartitioning _).expects(mockLink, mockPolicy).once()
    (mockOutputManager.passTupleToDownstream _).expects(ITuple(1), mockLink).once()
    (mockHandler.apply _).expects(*).anyNumberOfTimes()
    (mockOutputManager.flushAll _).expects().anyNumberOfTimes()
    val invocation = ControlInvocation(0, AddPartitioning(mockLink, mockPolicy))
    val updateInputLinking = ControlInvocation(1, UpdateInputLinking(identifier2, mockLink))
    sendControlToWorker(worker, Array(invocation, updateInputLinking))
    worker ! NetworkMessage(
      3,
      WorkflowFIFOMessage(
        ChannelID(identifier2, identifier1, false),
        0,
        DataFrame(Array(ITuple(1)))
      )
    )
    //wait test to finish
    Thread.sleep(3000)
  }

  "Worker" should "process batches correctly" in {
    ignoreMsg {
      case a => println(a); true
    }
    val worker = mkWorker
    (mockOutputManager.addPartitionerWithPartitioning _).expects(mockLink, mockPolicy).once()
    def mkBatch(start: Int, end: Int): Array[ITuple] = {
      (start until end).map { x =>
        (mockOutputManager.passTupleToDownstream _).expects(ITuple(x, x, x, x), mockLink).once()
        ITuple(x, x, x, x)
      }.toArray
    }
    val batch1 = mkBatch(0, 400)
    val batch2 = mkBatch(400, 500)
    val batch3 = mkBatch(500, 800)
    (mockHandler.apply _).expects(*).anyNumberOfTimes()
    (mockOutputManager.flushAll _).expects().anyNumberOfTimes()
    val invocation = ControlInvocation(0, AddPartitioning(mockLink, mockPolicy))
    val updateInputLinking = ControlInvocation(1, UpdateInputLinking(identifier2, mockLink))
    sendControlToWorker(worker, Array(invocation, updateInputLinking))
    worker ! NetworkMessage(
      3,
      WorkflowFIFOMessage(ChannelID(identifier2, identifier1, false), 0, DataFrame(batch1))
    )
    worker ! NetworkMessage(
      2,
      WorkflowFIFOMessage(ChannelID(identifier2, identifier1, false), 1, DataFrame(batch2))
    )
    Thread.sleep(1000)
    worker ! NetworkMessage(
      4,
      WorkflowFIFOMessage(ChannelID(identifier2, identifier1, false), 2, DataFrame(batch3))
    )
    //wait test to finish
    Thread.sleep(3000)
  }

  "Worker" should "accept messages in fifo order" in {
    ignoreMsg {
      case a => println(a); true
    }
    val worker = mkWorker
    (mockOutputManager.addPartitionerWithPartitioning _).expects(mockLink, mockPolicy).once()
    (mockHandler.apply _).expects(*).anyNumberOfTimes()
    (mockOutputManager.flushAll _).expects().anyNumberOfTimes()
    val invocation = ControlInvocation(0, AddPartitioning(mockLink, mockPolicy))
    val updateInputLinking = ControlInvocation(1, UpdateInputLinking(identifier2, mockLink))
    worker ! NetworkMessage(
      1,
      WorkflowFIFOMessage(ChannelID(CONTROLLER, identifier1, true), 1, updateInputLinking)
    )
    worker ! NetworkMessage(
      0,
      WorkflowFIFOMessage(ChannelID(CONTROLLER, identifier1, true), 0, invocation)
    )
    Random
      .shuffle((0 until 50).map { i =>
        (mockOutputManager.passTupleToDownstream _).expects(ITuple(i), mockLink).once()
        NetworkMessage(
          i + 2,
          WorkflowFIFOMessage(
            ChannelID(identifier2, identifier1, false),
            i,
            DataFrame(Array(ITuple(i)))
          )
        )
      })
      .foreach { x =>
        worker ! x
      }
    Thread.sleep(1000)
    Random
      .shuffle((50 until 100).map { i =>
        (mockOutputManager.passTupleToDownstream _).expects(ITuple(i), mockLink).once()
        NetworkMessage(
          i + 2,
          WorkflowFIFOMessage(
            ChannelID(identifier2, identifier1, false),
            i,
            DataFrame(Array(ITuple(i)))
          )
        )
      })
      .foreach { x =>
        worker ! x
      }
    //wait test to finish
    Thread.sleep(3000)
  }

}
