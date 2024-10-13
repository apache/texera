package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkMessage
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager.FlushNetworkBuffer
import edu.uci.ics.amber.engine.architecture.scheduling.config.WorkerConfig
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  MainThreadDelegateMessage,
  WorkerReplayInitialization
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddInputChannelHandler.AddInputChannel
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddPartitioningHandler.AddPartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AssignPortHandler.AssignPort
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.InitializeExecutorHandler.InitializeExecutor
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.executor.OperatorExecutor
import edu.uci.ics.amber.engine.common.model.tuple.{
  Attribute,
  AttributeType,
  Schema,
  Tuple,
  TupleLike
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.amber.engine.common.workflow.{PhysicalLink, PortIdentity}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.concurrent.CompletableFuture
import scala.collection.mutable
import scala.concurrent.duration.MILLISECONDS
import scala.util.Random

class WorkerSpec
    extends TestKit(ActorSystem("WorkerSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with MockFactory {

  def mkSchema(fields: Any*): Schema = {
    val schemaBuilder = Schema.builder()
    fields.indices.foreach { i =>
      schemaBuilder.add(new Attribute("field" + i, AttributeType.ANY))
    }
    schemaBuilder.build()
  }
  def mkTuple(fields: Any*): Tuple = {
    Tuple.builder(mkSchema(fields: _*)).addSequentially(fields.toArray).build()
  }

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
  private val identifier1 = ActorVirtualIdentity("Worker:WF1-E1-op-layer-1")
  private val identifier2 = ActorVirtualIdentity("Worker:WF1-E1-op-layer-2")

  private val mockOpExecutor = new OperatorExecutor {
    override def open(): Unit = println("opened!")

    override def close(): Unit = println("closed!")

    override def processTupleMultiPort(
        tuple: Tuple,
        port: Int
    ): Iterator[(TupleLike, Option[PortIdentity])] = {
      Iterator((tuple, None))
    }
    override def onFinishMultiPort(
        port: Int
    ): Iterator[(TupleLike, Option[PortIdentity])] = {
      Iterator()
    }

    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = ???
  }
  private val operatorIdentity = OperatorIdentity("testOperator")

  private val mockPortId = PortIdentity()
  private val mockLink =
    PhysicalLink(
      PhysicalOpIdentity(operatorIdentity, "1st-physical-op"),
      mockPortId,
      PhysicalOpIdentity(operatorIdentity, "2nd-physical-op"),
      mockPortId
    )

  private val mockPolicy =
    OneToOnePartitioning(10, Seq(ChannelIdentity(identifier1, identifier2, isControl = false)))

  def sendControlToWorker(
      worker: ActorRef,
      controls: Array[ControlInvocation],
      beginSeqNum: Long = 0
  ): Unit = {
    var seq = beginSeqNum
    controls.foreach { ctrl =>
      worker ! NetworkMessage(
        seq,
        WorkflowFIFOMessage(ChannelIdentity(CONTROLLER, identifier1, isControl = true), seq, ctrl)
      )
      seq += 1
    }
  }

  def mkWorker(expectedOutput: Iterable[TupleLike]): (ActorRef, CompletableFuture[Boolean]) = {
    val expected = mutable.Queue.from(expectedOutput)
    val completeStatus = new CompletableFuture[Boolean]()
    val mockHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit = {
      case Left(value) => ???
      case Right(value) =>
        value match {
          case WorkflowFIFOMessage(_, _, payload) =>
            payload match {
              case payload: DataPayload =>
                payload.asInstanceOf[DataFrame].frame.foreach { item =>
                  val expectedOutput = expected.dequeue()
                  if (expectedOutput != item) {
                    completeStatus.complete(false)
                  } else {
                    if (expected.isEmpty) {
                      completeStatus.complete(true)
                    }
                  }
                }
              case _ => //skip
            }
        }
    }
    val worker = TestActorRef(
      new WorkflowWorker(
        WorkerConfig(identifier1),
        WorkerReplayInitialization(restoreConfOpt = None, faultToleranceConfOpt = None)
      ) {
        this.dp = new DataProcessor(identifier1, mockHandler)
        this.dp.initTimerService(timerService)
        dpThread = new DPThread(
          actorId,
          dp,
          logManager,
          inputQueue
        )
      }
    )
    val invocation = ControlInvocation(0, AddPartitioning(mockLink, mockPolicy))
    val addPort1 = ControlInvocation(1, AssignPort(mockPortId, input = true, mkSchema(1)))
    val addPort2 = ControlInvocation(2, AssignPort(mockPortId, input = false, mkSchema(1)))
    val addInputChannel = ControlInvocation(
      3,
      AddInputChannel(
        ChannelIdentity(identifier2, identifier1, isControl = false),
        mockLink.toPortId
      )
    )
    val initializeOperatorLogic = ControlInvocation(
      4,
      InitializeExecutor(1, OpExecInitInfo((_, _) => mockOpExecutor), isSource = false)
    )
    sendControlToWorker(
      worker,
      Array(invocation, addPort1, addPort2, addInputChannel, initializeOperatorLogic)
    )
    (worker, completeStatus)
  }

  "Worker" should "process data messages correctly" in {
    val (worker, future) = mkWorker(Array(mkTuple(1)))
    worker ! NetworkMessage(
      0,
      WorkflowFIFOMessage(
        ChannelIdentity(identifier2, identifier1, isControl = false),
        0,
        DataFrame(Array(mkTuple(1)))
      )
    )
    worker ! ControlInvocation(
      AsyncRPCClient.IgnoreReplyAndDoNotLog,
      FlushNetworkBuffer()
    )
    //wait test to finish
    assert(future.get(3000, MILLISECONDS))
  }

  "Worker" should "process batches correctly" in {
    ignoreMsg {
      case a => println(a); true
    }

    def mkBatch(start: Int, end: Int): Array[Tuple] = {
      (start until end).map { x =>
        mkTuple(x)
      }.toArray
    }
    val batch1 = mkBatch(0, 400)
    val batch2 = mkBatch(400, 500)
    val batch3 = mkBatch(500, 800)
    val (worker, future) = mkWorker(mkBatch(0, 800))
    worker ! NetworkMessage(
      3,
      WorkflowFIFOMessage(
        ChannelIdentity(identifier2, identifier1, isControl = false),
        0,
        DataFrame(batch1)
      )
    )
    worker ! NetworkMessage(
      2,
      WorkflowFIFOMessage(
        ChannelIdentity(identifier2, identifier1, isControl = false),
        1,
        DataFrame(batch2)
      )
    )
    Thread.sleep(1000)
    worker ! NetworkMessage(
      4,
      WorkflowFIFOMessage(
        ChannelIdentity(identifier2, identifier1, isControl = false),
        2,
        DataFrame(batch3)
      )
    )
    //wait test to finish
    assert(future.get(3000, MILLISECONDS))
  }

  "Worker" should "accept messages in fifo order" in {
    ignoreMsg {
      case a => println(a); true
    }
    val (worker, future) = mkWorker((0 until 100).map(mkTuple(_)))
    Random
      .shuffle((0 until 50).map { i =>
        NetworkMessage(
          i + 2,
          WorkflowFIFOMessage(
            ChannelIdentity(identifier2, identifier1, isControl = false),
            i,
            DataFrame(Array(mkTuple(i)))
          )
        )
      })
      .foreach { x =>
        worker ! x
      }
    Thread.sleep(1000)
    Random
      .shuffle((50 until 100).map { i =>
        NetworkMessage(
          i + 2,
          WorkflowFIFOMessage(
            ChannelIdentity(identifier2, identifier1, isControl = false),
            i,
            DataFrame(Array(mkTuple(i)))
          )
        )
      })
      .foreach { x =>
        worker ! x
      }
    //wait test to finish
    assert(future.get(3000, MILLISECONDS))
  }

}
