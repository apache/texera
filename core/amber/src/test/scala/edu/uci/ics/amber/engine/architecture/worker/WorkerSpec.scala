package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.serialization.SerializationExtension
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.core.executor.{OpExecWithClassName, OperatorExecutor}
import edu.uci.ics.amber.core.tuple._
import edu.uci.ics.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.amber.core.workflow.{PhysicalLink, PortIdentity}
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkMessage
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands._
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc._
import edu.uci.ics.amber.engine.architecture.scheduling.config.WorkerConfig
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  MainThreadDelegateMessage,
  WorkerReplayInitialization
}
import edu.uci.ics.amber.engine.common.AmberRuntime
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.concurrent.CompletableFuture
import scala.collection.mutable
import scala.concurrent.duration.MILLISECONDS
import scala.util.Random
class DummyOperatorExecutor extends OperatorExecutor {
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    Iterator(tuple)
  }
}

class WorkerSpec
    extends TestKit(ActorSystem("WorkerSpec", AmberRuntime.akkaConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with MockFactory {

  def mkSchema(fields: Any*): Schema = {
    var schema = Schema()
    fields.indices.foreach { i =>
      schema = schema.add(new Attribute("field" + i, AttributeType.ANY))
    }
    schema
  }

  def mkTuple(fields: Any*): Tuple = {
    Tuple.builder(mkSchema(fields: _*)).addSequentially(fields.toArray).build()
  }

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    AmberRuntime.serde = SerializationExtension(system)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private val identifier1 = ActorVirtualIdentity("Worker:WF1-E1-op-layer-1")
  private val identifier2 = ActorVirtualIdentity("Worker:WF1-E1-op-layer-2")

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
    val invocation = AsyncRPCClient.ControlInvocation(
      METHOD_ADD_PARTITIONING,
      AddPartitioningRequest(mockLink, mockPolicy),
      AsyncRPCContext(CONTROLLER, identifier1),
      0
    )
    val addPort1 = AsyncRPCClient.ControlInvocation(
      METHOD_ASSIGN_PORT,
      AssignPortRequest(mockPortId, input = true, mkSchema(1).toRawSchema),
      AsyncRPCContext(CONTROLLER, identifier1),
      1
    )
    val addPort2 = AsyncRPCClient.ControlInvocation(
      METHOD_ASSIGN_PORT,
      AssignPortRequest(mockPortId, input = false, mkSchema(1).toRawSchema),
      AsyncRPCContext(CONTROLLER, identifier1),
      2
    )
    val addInputChannel = AsyncRPCClient.ControlInvocation(
      METHOD_ADD_INPUT_CHANNEL,
      AddInputChannelRequest(
        ChannelIdentity(identifier2, identifier1, isControl = false),
        mockLink.toPortId
      ),
      AsyncRPCContext(CONTROLLER, identifier1),
      3
    )

    val initializeOperatorLogic = AsyncRPCClient.ControlInvocation(
      METHOD_INITIALIZE_EXECUTOR,
      InitializeExecutorRequest(
        1,
        OpExecWithClassName("edu.uci.ics.amber.engine.architecture.worker.DummyOperatorExecutor"),
        isSource = false
      ),
      AsyncRPCContext(CONTROLLER, identifier1),
      4
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
    worker ! AsyncRPCClient.ControlInvocation(
      METHOD_FLUSH_NETWORK_BUFFER,
      EmptyRequest(),
      AsyncRPCContext(CONTROLLER, identifier1),
      1
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
